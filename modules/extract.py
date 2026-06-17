from pathlib import Path
import warnings

import pandas as pd

from modules.data_quality.rules_catalog import validate_sku_catalog_consistency
from modules.data_quality.rules_input import (
    extract_date_from_data,
    validate_expected_columns,
    validate_location_structure,
    validate_no_negatives,
)
from modules.data_quality.shared import (
    canonicalize_extract_columns,
    find_extract_column,
    is_controlled_internal_warehouse,
    normalize_dataframe_columns,
)


def read_excel_file(file_path: Path) -> pd.DataFrame:
    """
    Lee el archivo Excel original.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Cannot parse header or footer so it will be ignored",
            category=UserWarning,
        )
        df = pd.read_excel(file_path)
    df = normalize_dataframe_columns(df)
    df["_source_row_num"] = df.index + 2
    return df


def _normalize_operational_location(value: object) -> str:
    return ",".join(part.strip() for part in str(value).strip().upper().split(","))


def _build_operational_resolution_frame(df: pd.DataFrame) -> pd.DataFrame:
    df_check = canonicalize_extract_columns(df).copy()

    empresa_col = find_extract_column(df_check.columns, "EMPRESA")
    almacen_col = find_extract_column(df_check.columns, "ALMAC")
    ubicacion_col = find_extract_column(df_check.columns, "UBIC")
    codigo_col = find_extract_column(df_check.columns, "DIGO")
    producto_col = find_extract_column(df_check.columns, "PRODUCTO")
    presentacion_col = find_extract_column(df_check.columns, "PRESENT")
    fecha_fabricacion_col = find_extract_column(df_check.columns, "FECHA", "FABRIC")
    cantidad_col = find_extract_column(df_check.columns, "CANT")

    required_columns = [
        empresa_col,
        almacen_col,
        ubicacion_col,
        codigo_col,
        producto_col,
        presentacion_col,
        fecha_fabricacion_col,
        cantidad_col,
    ]
    if any(column is None for column in required_columns):
        return pd.DataFrame()

    for col in [empresa_col, almacen_col, ubicacion_col, codigo_col, producto_col]:
        df_check[col] = df_check[col].astype(str).str.strip()

    df_check[presentacion_col] = df_check[presentacion_col].astype(str).str.strip()

    if "_source_row_num" not in df_check.columns:
        df_check["_source_row_num"] = df_check.index + 2

    df_check = df_check[df_check[almacen_col].apply(is_controlled_internal_warehouse)].copy()
    if df_check.empty:
        return pd.DataFrame()

    df_check["ubicacion_operativa"] = df_check[ubicacion_col].apply(_normalize_operational_location)
    df_check["codigo_norm"] = df_check[codigo_col].astype(str).str.strip().str.upper()
    df_check["producto_norm"] = df_check[producto_col].astype(str).str.strip().str.upper()
    df_check["presentacion_norm"] = df_check[presentacion_col].astype(str).str.strip().str.upper()
    df_check["fecha_fabricacion_dt"] = pd.to_datetime(
        df_check[fecha_fabricacion_col],
        dayfirst=True,
        errors="coerce",
    )
    df_check["identity_key_extract"] = (
        df_check[empresa_col].astype(str).str.strip().str.upper()
        + "|"
        + df_check["codigo_norm"]
        + "|"
        + df_check["producto_norm"]
        + "|"
        + df_check["presentacion_norm"]
        + "|"
        + df_check[almacen_col].astype(str).str.strip().str.upper()
    )
    df_check["codigo_display"] = df_check[codigo_col]
    df_check["producto_display"] = df_check[producto_col]
    df_check["cantidad_display"] = pd.to_numeric(df_check[cantidad_col], errors="coerce")
    return df_check


def summarize_operational_resolution_candidates(df: pd.DataFrame, filename: str) -> str:
    """
    Resume patrones detectados en el Excel que luego seran resueltos por la capa
    de ocupacion por ubicacion.
    """
    if df.empty:
        return ""

    df_check = _build_operational_resolution_frame(df)
    if df_check.empty:
        return ""

    consolidation_count = 0
    conflict_count = 0
    multipallet_count = 0

    grouped = df_check.groupby("ubicacion_operativa", dropna=False)

    for ubicacion, group in grouped:
        if not ubicacion:
            continue

        same_identity_groups = [
            candidate_group
            for _, candidate_group in group.groupby("identity_key_extract", dropna=False)
            if len(candidate_group) > 1
        ]

        if same_identity_groups:
            consolidation_count += len(same_identity_groups)

        distinct_codes = group["codigo_norm"].nunique(dropna=True)
        if distinct_codes > 1:
            cajas_por_codigo = (
                group.groupby("codigo_norm")["cantidad_display"]
                .sum(min_count=1)
                .fillna(0)
                .astype(float)
                .to_dict()
            )
            small_pallets = all(value <= 60 for value in cajas_por_codigo.values()) and len(cajas_por_codigo) <= 2

            if small_pallets:
                multipallet_count += 1
            else:
                conflict_count += 1

    if consolidation_count == 0 and conflict_count == 0 and multipallet_count == 0:
        return ""

    return (
        f"[REVISION_PREVIA] {filename} | "
        f"consolidaciones={consolidation_count} | "
        f"multipallet={multipallet_count} | "
        f"conflictos={conflict_count}. "
        "Estos casos no bloquean el ETL; se resuelven automaticamente."
    )
