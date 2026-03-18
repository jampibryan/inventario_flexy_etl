import hashlib
import re

import pandas as pd

from modules.ubicaciones import sanitize_fact_ubicaciones, strip_accents


def _canonicalize_columns(fact: pd.DataFrame) -> pd.DataFrame:
    canonical_map = {
        "ALMACEN": "ALMACÉN",
        "ALMACN": "ALMACÉN",
        "CAMARA": "CÁMARA",
        "CMARA": "CÁMARA",
        "POSICION": "POSICIÓN",
        "POSICIN": "POSICIÓN",
        "CODIGO": "CÓDIGO",
        "CDIGO": "CÓDIGO",
        "PRESENTACION": "PRESENTACIÓN",
        "PRESENTACIN": "PRESENTACIÓN",
        "FECHA FABRICACION": "FECHA FABRICACIÓN",
        "FECHA FABRICACIN": "FECHA FABRICACIÓN",
        "CLASIFICACION": "CLASIFICACIÓN",
        "CLASIFICACIN": "CLASIFICACIÓN",
    }
    rename_map: dict[str, str] = {}

    for column in fact.columns:
        normalized = strip_accents(str(column)).upper()
        normalized = re.sub(r"[^A-Z0-9 ]+", "", normalized)
        normalized = " ".join(normalized.split())
        rename_target = canonical_map.get(normalized)
        if rename_target:
            rename_map[column] = rename_target

    return fact.rename(columns=rename_map)


def build_fact_snapshot(
    df: pd.DataFrame,
    source_file: str,
    valid_ubicacion_keys: set[str],
) -> tuple[pd.DataFrame, dict[str, object]]:
    fact = _canonicalize_columns(df.copy())

    fact["FECHA CORTE"] = pd.to_datetime(fact["FECHA CORTE"], errors="coerce")
    fact["fecha_key"] = fact["FECHA CORTE"].dt.strftime("%Y%m%d").astype(int)
    fact["cliente_key"] = fact["CLIENTE"].astype(str).str.strip().str.upper()
    fact["producto_key"] = fact["CÓDIGO"].astype(str).str.strip().str.upper()

    fact["almacen_grupo"] = fact["ALMACÉN"].astype(str).str.upper()
    fact["tipo_almacen"] = fact["almacen_grupo"].apply(
        lambda value: "INTERNO" if value == "CHAVIN" else "EXTERNO"
    )

    fact["pallets"] = 1
    fact["source_file"] = source_file

    if "source_row_num" in fact.columns:
        fact["source_row_num"] = pd.to_numeric(fact["source_row_num"], errors="coerce").astype("Int64")
    elif "_SOURCE_ROW_NUM" in fact.columns:
        fact["source_row_num"] = pd.to_numeric(fact["_SOURCE_ROW_NUM"], errors="coerce").astype("Int64")
        fact.drop(columns=["_SOURCE_ROW_NUM"], inplace=True)
    else:
        fact["source_row_num"] = pd.Series(range(2, len(fact) + 2), dtype="Int64")

    def _snapshot_token(row: pd.Series) -> str:
        if pd.notna(row.get("pallet_logico_id")):
            return str(row["pallet_logico_id"])
        return str(row.get("source_row_num"))

    fact["snapshot_row_id"] = fact.apply(
        lambda row: hashlib.sha1(
            f"{row['FECHA CORTE'].date()}|{source_file}|{_snapshot_token(row)}".encode("utf-8")
        ).hexdigest(),
        axis=1,
    )

    fact, ubicacion_audit = sanitize_fact_ubicaciones(fact, valid_ubicacion_keys)
    fact["FECHA CORTE"] = fact["FECHA CORTE"].dt.date

    return fact, ubicacion_audit
