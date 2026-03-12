from pathlib import Path
from datetime import datetime


def ensure_directories(paths: list[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def format_date_ddmmyyyy(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%d-%m-%Y")


def build_output_names(date_str: str) -> str:
    visible_date = format_date_ddmmyyyy(date_str)
    return f"inventario_{visible_date}.xlsx"