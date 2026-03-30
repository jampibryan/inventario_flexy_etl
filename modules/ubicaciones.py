import re
import unicodedata
from typing import Any

import pandas as pd

from config import ALLOW_INVALID_LOCATION_AS_POSITION, CONTROLLED_INTERNAL_WAREHOUSES


CAMARA_PREFIX = "C\u00c1MARA"
RECEPCION_LABEL = "RECEPCI\u00d3N"
POSICION_LABEL = "POSICI\u00d3N"
SIN_UBICACION_LABEL = "SIN_UBICACI\u00d3N"
EXTERNO_LABEL = "EXTERNO"

COLUMN_ALIASES = {
    "almacen": ("ALMACEN", "ALMACÉN", "ALMACÃ‰N"),
    "almacen_original": ("ALMACEN ORIGINAL", "ALMACÉN ORIGINAL", "ALMACÃ‰N ORIGINAL"),
    "camara": ("CAMARA", "CÁMARA", "CÃMARA"),
    "rack": ("RACK",),
    "nivel": ("NIVEL",),
    "posicion": ("POSICION", "POSICIÓN", "POSICIÃ“N"),
}

AUDIT_COLUMN_LABELS = {
    "almacen": "ALMACEN",
    "camara": "CAMARA",
    "rack": "RACK",
    "nivel": "NIVEL",
    "posicion": "POSICION",
}


def normalize_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().upper()


CONTROLLED_INTERNAL_WAREHOUSE_SET = {
    normalize_text(value) for value in (*CONTROLLED_INTERNAL_WAREHOUSES, "CHAVIN")
}


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _row_get(row: pd.Series, alias_group: str) -> Any:
    for alias in COLUMN_ALIASES[alias_group]:
        if alias in row.index:
            return row.get(alias)
    return None


def _select_existing_column(df: pd.DataFrame, alias_group: str) -> str | None:
    for alias in COLUMN_ALIASES[alias_group]:
        if alias in df.columns:
            return alias
    return None


def _build_invalid_rows_audit_view(invalid_rows: pd.DataFrame) -> pd.DataFrame:
    selected_columns: list[str] = ["source_file", "source_row_num"]
    rename_map: dict[str, str] = {}

    for alias_group, label in AUDIT_COLUMN_LABELS.items():
        column = _select_existing_column(invalid_rows, alias_group)
        if column:
            selected_columns.append(column)
            rename_map[column] = label

    selected_columns.extend(
        [
            "camara_normalizada",
            "ubicacion_key_candidata",
            "contabilizada_temporalmente_flag",
        ]
    )
    return invalid_rows[selected_columns].rename(columns=rename_map)


def normalize_camara(value: Any) -> str:
    camara = normalize_text(value)
    camara_ascii = strip_accents(camara)

    if camara_ascii == "RECEPCION" or re.fullmatch(r"RECEPCI.N", camara_ascii):
        return RECEPCION_LABEL

    if re.fullmatch(r"\d{1,2}", camara_ascii):
        return f"{CAMARA_PREFIX} {int(camara_ascii):02d}"

    match = re.fullmatch(r"CAMARA\s*(\d{1,2})", camara_ascii)
    if match:
        return f"{CAMARA_PREFIX} {int(match.group(1)):02d}"

    match_flexible = re.fullmatch(r"C.MARA\s*(\d{1,2})", camara_ascii)
    if match_flexible:
        return f"{CAMARA_PREFIX} {int(match_flexible.group(1)):02d}"

    return camara


def to_nullable_int(value: Any) -> int | None:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def build_ubicacion_key(
    camara: Any,
    rack: Any,
    nivel: Any,
    posicion: Any,
) -> str | None:
    camara_norm = normalize_camara(camara)
    match = re.fullmatch(rf"{CAMARA_PREFIX}\s(\d{{2}})", camara_norm)
    rack_num = to_nullable_int(rack)
    nivel_num = to_nullable_int(nivel)
    posicion_num = to_nullable_int(posicion)

    if not match or rack_num is None or nivel_num is None or posicion_num is None:
        return None

    return (
        f"CAM{match.group(1)}-"
        f"R{rack_num:03d}-"
        f"N{nivel_num:02d}-"
        f"P{posicion_num:02d}"
    )


def resolve_almacen_control_reference(row: pd.Series) -> Any:
    return _row_get(row, "almacen_original") or _row_get(row, "almacen")


def resolve_ubicacion_inventario(
    almacen: Any,
    camara: Any,
    ubicacion_key: str | None,
    valid_ubicacion_keys: set[str],
) -> str | None:
    almacen_norm = normalize_text(almacen)
    camara_norm = normalize_camara(camara)

    if almacen_norm not in CONTROLLED_INTERNAL_WAREHOUSE_SET:
        return EXTERNO_LABEL
    if camara_norm == RECEPCION_LABEL:
        return RECEPCION_LABEL
    if pd.notna(ubicacion_key) and str(ubicacion_key) in valid_ubicacion_keys:
        return camara_norm
    if ALLOW_INVALID_LOCATION_AS_POSITION and pd.notna(ubicacion_key):
        return camara_norm
    return None


def resolve_tipo_ubicacion(
    almacen: Any,
    camara: Any,
    ubicacion_key: str | None,
    valid_ubicacion_keys: set[str],
) -> str:
    almacen_norm = normalize_text(almacen)
    camara_norm = normalize_camara(camara)

    if almacen_norm not in CONTROLLED_INTERNAL_WAREHOUSE_SET:
        return EXTERNO_LABEL
    if camara_norm == RECEPCION_LABEL:
        return RECEPCION_LABEL
    if pd.notna(ubicacion_key) and str(ubicacion_key) in valid_ubicacion_keys:
        return POSICION_LABEL
    if ALLOW_INVALID_LOCATION_AS_POSITION and pd.notna(ubicacion_key):
        return POSICION_LABEL
    return SIN_UBICACION_LABEL


def sanitize_fact_ubicaciones(
    fact_df: pd.DataFrame,
    valid_ubicacion_keys: set[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    fact = fact_df.copy()

    ubicacion_key_candidata = fact.apply(
        lambda row: build_ubicacion_key(
            _row_get(row, "camara"),
            _row_get(row, "rack"),
            _row_get(row, "nivel"),
            _row_get(row, "posicion"),
        ),
        axis=1,
    )

    es_match_dim = ubicacion_key_candidata.isin(valid_ubicacion_keys)
    if ALLOW_INVALID_LOCATION_AS_POSITION:
        fact["ubicacion_key"] = ubicacion_key_candidata.astype("string")
    else:
        fact["ubicacion_key"] = ubicacion_key_candidata.where(es_match_dim, pd.NA).astype("string")

    fact["ubicacion_inventario"] = fact.apply(
        lambda row: resolve_ubicacion_inventario(
            resolve_almacen_control_reference(row),
            _row_get(row, "camara"),
            row.get("ubicacion_key"),
            valid_ubicacion_keys,
        ),
        axis=1,
    )
    fact["tipo_ubicacion"] = fact.apply(
        lambda row: resolve_tipo_ubicacion(
            resolve_almacen_control_reference(row),
            _row_get(row, "camara"),
            row.get("ubicacion_key"),
            valid_ubicacion_keys,
        ),
        axis=1,
    )

    internal_mask = fact.apply(
        lambda row: normalize_text(resolve_almacen_control_reference(row)) in CONTROLLED_INTERNAL_WAREHOUSE_SET,
        axis=1,
    )
    invalid_mask = internal_mask & ubicacion_key_candidata.notna() & ~es_match_dim

    invalid_rows = fact_df.loc[invalid_mask].copy()
    invalid_rows["ubicacion_key_candidata"] = ubicacion_key_candidata[invalid_mask]
    invalid_rows["camara_normalizada"] = invalid_rows.apply(
        lambda row: normalize_camara(_row_get(row, "camara")),
        axis=1,
    )
    invalid_rows["contabilizada_temporalmente_flag"] = 1 if ALLOW_INVALID_LOCATION_AS_POSITION else 0
    audit_rows = _build_invalid_rows_audit_view(invalid_rows)

    fact["ubicacion_invalida_estructura_flag"] = 0
    fact.loc[invalid_mask, "ubicacion_invalida_estructura_flag"] = 1

    return fact, {
        "invalid_position_count": int(invalid_mask.sum()),
        "invalid_position_keys": sorted(audit_rows["ubicacion_key_candidata"].dropna().unique().tolist()),
        "invalid_rows": audit_rows.reset_index(drop=True),
        "invalid_positions_preserved": bool(ALLOW_INVALID_LOCATION_AS_POSITION),
    }
