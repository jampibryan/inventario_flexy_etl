from pathlib import Path
import pandas as pd
from config import EXPECTED_COLUMNS


def read_excel_file(file_path: Path) -> pd.DataFrame:
    """
    Lee el archivo Excel original.
    """
    df = pd.read_excel(file_path)
    return df


def validate_expected_columns(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Valida que el Excel tenga todas las columnas esperadas.
    """
    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        return False, f"Faltan columnas: {', '.join(missing)}"
    return True, ""