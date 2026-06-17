import re
import unicodedata

import pandas as pd

from modules.ubicaciones import CONTROLLED_INTERNAL_WAREHOUSE_SET


def normalize_text(value: str) -> str:
    return unicodedata.normalize("NFC", str(value))


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_text(col) for col in df.columns]
    return df


def canonicalize_extract_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_dataframe_columns(df)
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


def find_extract_column(columns: pd.Index, *tokens: str) -> str | None:
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


def is_controlled_internal_warehouse(value: object) -> bool:
    return " ".join(str(value).strip().upper().split()) in CONTROLLED_INTERNAL_WAREHOUSE_SET
