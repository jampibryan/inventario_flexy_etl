from pathlib import Path
import re
import unicodedata
import warnings

import pandas as pd

from config import (
    BLOCK_ON_INVALID_LOCATION_STRUCTURE,
    CONTROLLED_INTERNAL_WAREHOUSES,
    EXPECTED_COLUMNS,
)
from modules.dimensiones import (
    CAPACITY_CONFIG,
    get_camera_capacity_limits,
    resolve_camera_section,
)
from modules.fechas import parse_datetime_series
from modules.ubicaciones import (
    CONTROLLED_INTERNAL_WAREHOUSE_SET,
    RECEPCION_LABEL,
    normalize_camara,
)


def _normalize_text(value: str) -> str:
    return unicodedata.normalize("NFC", str(value))


def _normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_text(col) for col in df.columns]
    return df


def _canonicalize_extract_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_dataframe_columns(df)
    canonical_map = {
        "FECHA ACTUALIZACION": "Fecha Actualización",
        "ALMACEN": "Almacén",
        "UBICACION": "Ubicación",
        "CODIGO": "Código",
        "PRESENTACION": "Presentación",
        "FECHA FABRICACION": "Fecha Fabricación",
    }
    rename_map: dict[str, str] = {}

    for column in df.columns:
        normalized = unicodedata.normalize("NFKD", str(column))
        normalized = "".join(char for char in normalized if not unicodedata.combining(char))
        normalized = normalized.upper()
        normalized = re.sub(r"[^A-Z0-9 ]+", "", normalized)
        normalized = " ".join(normalized.split())
        rename_target = canonical_map.get(normalized)
        if rename_target:
            rename_map[column] = rename_target

    return df.rename(columns=rename_map)


def _find_extract_column(columns: pd.Index, *tokens: str) -> str | None:
    normalized_columns: dict[str, str] = {}

    for column in columns:
        normalized = unicodedata.normalize("NFKD", str(column))
        normalized = "".join(char for char in normalized if not unicodedata.combining(char))
        normalized = normalized.upper()
        normalized = re.sub(r"[^A-Z0-9 ]+", "", normalized)
        normalized = " ".join(normalized.split())
        normalized_columns[normalized] = str(column)

    for normalized, original in normalized_columns.items():
        if all(token.upper() in normalized for token in tokens):
            return original

    return None


def _build_capacity_lookup() -> dict[str, dict[str, int | str]]:
    return {cfg["camara"]: cfg for cfg in CAPACITY_CONFIG}


CAPACITY_LOOKUP = _build_capacity_lookup()
STRUCTURAL_CAMERAS = set(CAPACITY_LOOKUP)


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
    df = _normalize_dataframe_columns(df)
    df["_source_row_num"] = df.index + 2
    return df


def validate_expected_columns(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Valida que el Excel tenga todas las columnas esperadas.
    """
    df = _normalize_dataframe_columns(df)
    expected_columns = [_normalize_text(col) for col in EXPECTED_COLUMNS]
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        return False, f"Faltan columnas: {', '.join(missing)}"
    return True, ""


def extract_date_from_data(df: pd.DataFrame) -> tuple[bool, str, str]:
    """
    Extrae la fecha desde la columna 'Fecha Actualizacion' del DataFrame.
    Toma la primera fecha valida encontrada.
    Retorna: (exito, fecha_str_yyyy_mm_dd, mensaje_error)
    """
    df = _normalize_dataframe_columns(df)

    if "Fecha Actualizaci\u00f3n" not in df.columns:
        return False, "", "No se encontro la columna 'Fecha Actualizacion'"

    fechas = parse_datetime_series(df["Fecha Actualizaci\u00f3n"]).dropna()

    if fechas.empty:
        return False, "", "No se encontraron fechas validas en 'Fecha Actualizacion'"

    primera_fecha = fechas.iloc[0]
    fecha_str = primera_fecha.strftime("%Y-%m-%d")
    return True, fecha_str, ""


def _prepare_numeric_validation_frame(df: pd.DataFrame) -> pd.DataFrame:
    df_check = _normalize_dataframe_columns(df)

    df_check["Cantidad"] = (
        df_check["Cantidad"]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)
        .str.replace("\xa0", "", regex=False)
    )
    df_check["Cantidad"] = pd.to_numeric(df_check["Cantidad"], errors="coerce")
    df_check["Presentaci\u00f3n"] = pd.to_numeric(df_check["Presentaci\u00f3n"], errors="coerce")

    ubic_split = df_check["Ubicaci\u00f3n"].astype(str).str.split(",", expand=True)
    if 1 in ubic_split.columns:
        df_check["Rack"] = pd.to_numeric(ubic_split[1].str.strip(), errors="coerce")
    if 2 in ubic_split.columns:
        df_check["Nivel"] = pd.to_numeric(ubic_split[2].str.strip(), errors="coerce")
    if 3 in ubic_split.columns:
        df_check["Posici\u00f3n"] = pd.to_numeric(ubic_split[3].str.strip(), errors="coerce")

    return df_check


def _format_pair_row_detail(row: pd.Series, idx: int) -> str:
    fila_excel = idx + 2
    codigo = row.get("C\u00f3digo", "?")
    producto = row.get("Producto", "?")
    cantidad = row.get("Cantidad")
    presentacion = row.get("Presentaci\u00f3n")
    ubicacion = str(row.get("Ubicaci\u00f3n", "")).strip()
    return (
        f"     - Fila {fila_excel} | Cantidad = {cantidad} | Presentacion = {presentacion} "
        f"| Ubicacion: {ubicacion} | Codigo: {codigo} | Producto: {producto}"
    )


def _normalize_compensation_location(value: object) -> str:
    return " ".join(str(value).strip().upper().split())


def _find_compensated_negative_pairs(df_check: pd.DataFrame) -> tuple[set[int], list[str]]:
    drop_indices: set[int] = set()
    detail_lines: list[str] = []
    tolerance = 1e-9

    for pos in range(1, len(df_check)):
        prev_idx = df_check.index[pos - 1]
        curr_idx = df_check.index[pos]

        if prev_idx in drop_indices or curr_idx in drop_indices:
            continue

        prev = df_check.loc[prev_idx]
        curr = df_check.loc[curr_idx]

        prev_cantidad = prev.get("Cantidad")
        curr_cantidad = curr.get("Cantidad")
        prev_presentacion = prev.get("Presentaci\u00f3n")
        curr_presentacion = curr.get("Presentaci\u00f3n")
        prev_ubicacion = _normalize_compensation_location(prev.get("Ubicaci\u00f3n", ""))
        curr_ubicacion = _normalize_compensation_location(curr.get("Ubicaci\u00f3n", ""))

        if not (
            pd.notna(prev_cantidad)
            and pd.notna(curr_cantidad)
            and pd.notna(prev_presentacion)
            and pd.notna(curr_presentacion)
            and prev_ubicacion
            and curr_ubicacion
            and prev_ubicacion == curr_ubicacion
            and curr_cantidad < 0
            and curr_presentacion < 0
            and abs(prev_cantidad + curr_cantidad) <= tolerance
            and abs(prev_presentacion + curr_presentacion) <= tolerance
        ):
            continue

        drop_indices.update({prev_idx, curr_idx})
        detail_lines.append(
            "     -> Se eliminan 2 filas consecutivas porque se compensan entre si:"
        )
        detail_lines.append(_format_pair_row_detail(prev, prev_idx))
        detail_lines.append(_format_pair_row_detail(curr, curr_idx))
        detail_lines.append(
            "        Suma del par: "
            f"Cantidad = {prev_cantidad + curr_cantidad}, "
            f"Presentacion = {prev_presentacion + curr_presentacion}"
        )

    return drop_indices, detail_lines


def validate_no_negatives(df: pd.DataFrame, filename: str) -> tuple[bool, str, pd.DataFrame]:
    """
    Valida que no haya valores negativos en columnas numericas.
    Revisa: Cantidad, Presentacion, y Rack/Nivel/Posicion (desde Ubicacion).
    Si detecta pares consecutivos cuya suma en Cantidad y Presentacion da 0,
    elimina ambas filas y permite continuar. Si quedan negativos no resueltos,
    el archivo se bloquea.
    """
    warning_counts: list[str] = []
    df_check = _prepare_numeric_validation_frame(df)
    drop_indices, compensated_detail_lines = _find_compensated_negative_pairs(df_check)

    df_filtered = df.drop(index=list(drop_indices)).copy() if drop_indices else df.copy()
    df_filtered_check = _prepare_numeric_validation_frame(df_filtered)

    columns_to_check = ["Cantidad", "Presentaci\u00f3n", "Rack", "Nivel", "Posici\u00f3n"]
    total_negativos = 0

    for col in columns_to_check:
        if col not in df_filtered_check.columns:
            continue
        neg_mask = df_filtered_check[col] < 0
        if neg_mask.any():
            count = int(neg_mask.sum())
            total_negativos += count
            warning_counts.append(f"{col}: {count}")

    if warning_counts:
        resumen = ", ".join(warning_counts)
        msg = (
            f"[REVISION_EXCEL] {filename} | bloqueado por valores negativos | "
            f"total={total_negativos} | detalle={resumen}. "
            "Corrige el Excel original y vuelve a ejecutar."
        )
        return False, msg, df

    if compensated_detail_lines:
        msg = (
            f"[REVISION_EXCEL] {filename} | ajuste automatico aplicado | "
            f"filas_eliminadas={len(drop_indices)} por compensacion exacta "
            "en Cantidad y Presentacion."
        )
        return True, msg, df_filtered

    return True, "", df_filtered


def _prepare_location_structure_frame(df: pd.DataFrame) -> pd.DataFrame:
    df_check = _normalize_dataframe_columns(df).copy()

    ubicacion = df_check["Ubicaci\u00f3n"].fillna("").astype(str)
    ubic_split = ubicacion.str.split(",", expand=True)

    df_check["camara_raw"] = ubic_split[0].fillna("").astype(str).str.strip() if 0 in ubic_split.columns else ""
    df_check["rack_raw"] = ubic_split[1].fillna("").astype(str).str.strip() if 1 in ubic_split.columns else ""
    df_check["nivel_raw"] = ubic_split[2].fillna("").astype(str).str.strip() if 2 in ubic_split.columns else ""
    df_check["posicion_raw"] = ubic_split[3].fillna("").astype(str).str.strip() if 3 in ubic_split.columns else ""

    df_check["camara_normalizada"] = df_check["camara_raw"].apply(normalize_camara)
    df_check["rack_num"] = pd.to_numeric(df_check["rack_raw"], errors="coerce")
    df_check["nivel_num"] = pd.to_numeric(df_check["nivel_raw"], errors="coerce")
    df_check["posicion_num"] = pd.to_numeric(df_check["posicion_raw"], errors="coerce")
    df_check["ubicacion_componentes"] = ubic_split.shape[1]
    df_check["almacen_upper"] = df_check["Almac\u00e9n"].fillna("").astype(str).str.upper()
    df_check["es_chavin"] = df_check["Almacén"].apply(_is_controlled_internal_warehouse)
    df_check["componentes_no_vacios"] = df_check[["camara_raw", "rack_raw", "nivel_raw", "posicion_raw"]].apply(
        lambda row: sum(bool(str(value).strip()) for value in row),
        axis=1,
    )

    extra_cols = [col for col in ubic_split.columns if col > 3]
    if extra_cols:
        df_check["componentes_extra"] = ubic_split[extra_cols].fillna("").astype(str).apply(
            lambda row: [value.strip() for value in row if value.strip()],
            axis=1,
        )
    else:
        df_check["componentes_extra"] = [[] for _ in range(len(df_check))]

    return df_check


def _normalize_operational_location(value: object) -> str:
    return ",".join(part.strip() for part in str(value).strip().upper().split(","))


def _is_controlled_internal_warehouse(value: object) -> bool:
    return " ".join(str(value).strip().upper().split()) in CONTROLLED_INTERNAL_WAREHOUSE_SET


def _build_operational_resolution_frame(df: pd.DataFrame) -> pd.DataFrame:
    df_check = _canonicalize_extract_columns(df).copy()

    empresa_col = _find_extract_column(df_check.columns, "EMPRESA")
    almacen_col = _find_extract_column(df_check.columns, "ALMAC")
    ubicacion_col = _find_extract_column(df_check.columns, "UBIC")
    codigo_col = _find_extract_column(df_check.columns, "DIGO")
    producto_col = _find_extract_column(df_check.columns, "PRODUCTO")
    presentacion_col = _find_extract_column(df_check.columns, "PRESENT")
    fecha_fabricacion_col = _find_extract_column(df_check.columns, "FECHA", "FABRIC")
    cantidad_col = _find_extract_column(df_check.columns, "CANT")

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

    df_check = df_check[df_check[almacen_col].apply(_is_controlled_internal_warehouse)].copy()
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
            sorted_group = group.sort_values(
                by=["fecha_fabricacion_dt", "_source_row_num"],
                ascending=[False, False],
                na_position="last",
            )
            latest = sorted_group.iloc[0]
            other_codes = ", ".join(sorted(group["codigo_norm"].dropna().unique().tolist()))
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


def _format_location_issue(row: pd.Series, motivo: str) -> str:
    fila_excel = int(row.get("_source_row_num", row.name + 2))
    codigo = row.get("C\u00f3digo", "?")
    producto = row.get("Producto", "?")
    ubicacion = row.get("Ubicaci\u00f3n", "")
    camara = row.get("camara_normalizada", "")
    rack = row.get("rack_num")
    nivel = row.get("nivel_num")
    posicion = row.get("posicion_num")

    return (
        f"     -> Fila {fila_excel} | Ubicacion = {ubicacion} | Camara = {camara or '[VACIA]'} "
        f"| Rack = {rack} | Nivel = {nivel} | Posicion = {posicion} "
        f"| Codigo: {codigo} | Producto: {producto} | Error: {motivo}"
    )


def _find_non_numeric_components(row: pd.Series) -> list[str]:
    invalid_components: list[str] = []

    for label, raw_col, num_col in [
        ("rack", "rack_raw", "rack_num"),
        ("nivel", "nivel_raw", "nivel_num"),
        ("posicion", "posicion_raw", "posicion_num"),
    ]:
        raw_value = str(row.get(raw_col, "")).strip()
        num_value = row.get(num_col)
        if raw_value and pd.isna(num_value):
            invalid_components.append(label)

    return invalid_components


def validate_location_structure(df: pd.DataFrame, filename: str) -> tuple[bool, str]:
    """
    Valida que las ubicaciones del Excel respeten la estructura fisica definida
    en CAPACITY_CONFIG. Si encuentra una ubicacion interna fuera de rango o con
    formato inconsistente, bloquea el archivo.
    """
    df_check = _prepare_location_structure_frame(df)
    issues: list[str] = []

    for _, row in df_check.iterrows():
        if not bool(row.get("es_chavin")):
            continue

        camara = row.get("camara_normalizada", "")
        camara_raw = row.get("camara_raw", "")
        rack_raw = str(row.get("rack_raw", "")).strip()
        nivel_raw = str(row.get("nivel_raw", "")).strip()
        posicion_raw = str(row.get("posicion_raw", "")).strip()
        rack = row.get("rack_num")
        nivel = row.get("nivel_num")
        posicion = row.get("posicion_num")
        extra_components = row.get("componentes_extra", [])
        non_numeric_components = _find_non_numeric_components(row)

        if extra_components:
            issues.append(
                _format_location_issue(
                    row,
                    f"ubicacion tiene componentes extra no permitidos: {extra_components}",
                )
            )
            continue

        if not camara_raw:
            if any([rack_raw, nivel_raw, posicion_raw]):
                issues.append(
                    _format_location_issue(
                        row,
                        "ubicacion parcial: hay coordenadas pero falta la camara",
                    )
                )
            continue

        if camara == RECEPCION_LABEL:
            if any([rack_raw, nivel_raw, posicion_raw]):
                issues.append(
                    _format_location_issue(
                        row,
                        "RECEPCION no debe tener rack, nivel ni posicion",
                    )
                )
            continue

        if camara not in STRUCTURAL_CAMERAS:
            issues.append(
                _format_location_issue(
                    row,
                    "camara interna no existe en CAPACITY_CONFIG",
                )
            )
            continue

        if row.get("componentes_no_vacios", 0) != 4:
            issues.append(
                _format_location_issue(
                    row,
                    "ubicacion estructural debe tener exactamente 4 componentes: camara,rack,nivel,posicion",
                )
            )
            continue

        if non_numeric_components:
            issues.append(
                _format_location_issue(
                    row,
                    f"componentes no numericos en: {', '.join(non_numeric_components)}",
                )
            )
            continue

        if any(pd.isna(value) for value in [rack, nivel, posicion]):
            issues.append(
                _format_location_issue(
                    row,
                    "ubicacion estructural incompleta: faltan rack, nivel o posicion",
                )
            )
            continue

        cfg = CAPACITY_LOOKUP[camara]
        section = resolve_camera_section(cfg, int(rack))

        if section is None:
            limits = get_camera_capacity_limits(cfg)
            issues.append(
                _format_location_issue(
                    row,
                    f"rack fuera de rango para {camara}: permitido {limits['rack_min']}..{limits['rack_max']}",
                )
            )
            continue

        section_ranges = [
            ("nivel", int(nivel), 1, int(section["niveles"])),
            ("posicion", int(posicion), 1, int(section["posiciones"])),
        ]

        for label, value, min_value, max_value in section_ranges:
            if value < min_value or value > max_value:
                issues.append(
                    _format_location_issue(
                        row,
                        f"{label} fuera de rango para {camara}: permitido {min_value}..{max_value}",
                    )
                )
                break

    if not issues:
        return True, ""

    sep = "=" * 90

    if not BLOCK_ON_INVALID_LOCATION_STRUCTURE:
        msg = (
            f"[VALIDACION_UBICACION] {filename} | observaciones={len(issues)}. "
            "Se continuara, pero esas ubicaciones no contaran como posiciones validas."
        )
        return True, msg

    msg = (
        f"[VALIDACION_UBICACION] {filename} | bloqueado | "
        f"ubicaciones_invalidas={len(issues)}. "
        "Corrige el Excel original para respetar camara, rack, nivel y posicion."
    )
    return False, msg
