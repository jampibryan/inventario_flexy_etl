from pathlib import Path
from datetime import datetime
import re


def ensure_directories(paths: list[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def is_valid_original_filename(filename: str) -> bool:
    """
    Valida que el nombre del archivo siga el formato yyyy-mm-dd.xlsx
    Ejemplo válido: 2026-03-07.xlsx
    """
    pattern = r"^\d{4}-\d{2}-\d{2}\.xlsx$"
    return bool(re.match(pattern, filename))


def extract_date_from_filename(filename: str) -> str:
    """
    Extrae la fecha yyyy-mm-dd desde el nombre del archivo.
    Ejemplo: 2026-03-07.xlsx -> 2026-03-07
    """
    return Path(filename).stem


def format_date_ddmmyyyy(date_str: str) -> str:
    """
    Convierte una fecha de yyyy-mm-dd a dd-mm-yyyy
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%d-%m-%Y")


def build_output_names(date_str: str) -> tuple[str, str]:
    """
    Construye los nombres de salida:
    inventario_dd-mm-yyyy.xlsx
    inventario_dd-mm-yyyy.csv
    """
    visible_date = format_date_ddmmyyyy(date_str)
    excel_name = f"inventario_{visible_date}.xlsx"
    csv_name = f"inventario_{visible_date}.csv"
    return excel_name, csv_name