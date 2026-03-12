from pathlib import Path
import shutil
import pandas as pd
from modules.parquet_io import save_parquet


def save_daily_outputs(df: pd.DataFrame, excel_path: Path) -> None:
    """
    Guarda Excel transformado para revisión humana.
    """
    excel_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(excel_path, index=False, engine="xlsxwriter")


def get_partition_path(base_dir: Path, fecha_corte: str) -> Path:
    """
    Devuelve la ruta de la partición:
    DW/fact_inventario/fecha_corte=YYYY-MM-DD/data.parquet
    """
    return base_dir / f"fecha_corte={fecha_corte}" / "data.parquet"


def write_fact_partition(fact_daily: pd.DataFrame, partitioned_dir: Path, replace_if_exists: bool = True) -> Path:
    """
    Guarda el snapshot diario en su partición.
    Si ya existe la partición y replace_if_exists=True, la reemplaza completa.
    """
    if fact_daily.empty:
        raise ValueError("fact_daily está vacío. No se puede crear una partición.")

    fechas = pd.to_datetime(fact_daily["FECHA CORTE"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").unique()

    if len(fechas) != 1:
        raise ValueError("fact_daily debe contener una sola fecha de corte por partición.")

    fecha_corte = fechas[0]
    partition_file = get_partition_path(partitioned_dir, fecha_corte)
    partition_dir = partition_file.parent

    if partition_dir.exists() and replace_if_exists:
        shutil.rmtree(partition_dir)

    partition_dir.mkdir(parents=True, exist_ok=True)
    save_parquet(fact_daily, partition_file)

    return partition_file