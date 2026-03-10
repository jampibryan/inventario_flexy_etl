from pathlib import Path
from datetime import datetime
import pandas as pd


CONTROL_COLUMNS = [
    "archivo_original",
    "fecha_archivo",
    "fecha_procesamiento",
    "estado",
    "archivo_excel_salida",
    "archivo_csv_salida",
    "observacion",
]


def load_control_file(control_path: Path) -> pd.DataFrame:
    """
    Carga el archivo de control si existe.
    Si no existe, crea un DataFrame vacío con la estructura esperada.
    """
    if control_path.exists():
        df = pd.read_csv(control_path, dtype=str)
        for col in CONTROL_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        return df[CONTROL_COLUMNS].fillna("")
    return pd.DataFrame(columns=CONTROL_COLUMNS)


def save_control_file(control_df: pd.DataFrame, control_path: Path) -> None:
    """
    Guarda el archivo de control.
    """
    control_df.to_csv(control_path, index=False, encoding="utf-8-sig")


def is_already_processed(control_df: pd.DataFrame, filename: str) -> bool:
    """
    Revisa si un archivo ya fue procesado correctamente.
    """
    if control_df.empty:
        return False

    match = control_df[
        (control_df["archivo_original"] == filename) &
        (control_df["estado"] == "PROCESADO")
    ]
    return not match.empty


def add_control_record(
    control_df: pd.DataFrame,
    archivo_original: str,
    fecha_archivo: str,
    estado: str,
    archivo_excel_salida: str = "",
    archivo_csv_salida: str = "",
    observacion: str = "",
) -> pd.DataFrame:
    """
    Agrega un registro al archivo de control.
    """
    new_row = {
        "archivo_original": archivo_original,
        "fecha_archivo": fecha_archivo,
        "fecha_procesamiento": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "estado": estado,
        "archivo_excel_salida": archivo_excel_salida,
        "archivo_csv_salida": archivo_csv_salida,
        "observacion": observacion,
    }

    return pd.concat([control_df, pd.DataFrame([new_row])], ignore_index=True)