import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from modules.parquet_io import save_parquet
from modules.ubicaciones import sanitize_fact_ubicaciones


def save_daily_outputs(df: pd.DataFrame, excel_path: Path) -> None:
    """
    Guarda Excel transformado para revision humana.
    """
    excel_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(excel_path, index=False, engine="xlsxwriter")


def get_partition_path(base_dir: Path, fecha_corte: str) -> Path:
    """
    Devuelve la ruta de la particion:
    DW/fact_inventario/fecha_corte=YYYY-MM-DD/data.parquet
    """
    return base_dir / f"fecha_corte={fecha_corte}" / "data.parquet"


def write_fact_partition(fact_daily: pd.DataFrame, partitioned_dir: Path, replace_if_exists: bool = True) -> Path:
    """
    Guarda el snapshot diario en su particion.
    Si ya existe la particion y replace_if_exists=True, la reemplaza completa.
    """
    if fact_daily.empty:
        raise ValueError("fact_daily esta vacio. No se puede crear una particion.")

    fechas = pd.to_datetime(fact_daily["FECHA CORTE"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").unique()

    if len(fechas) != 1:
        raise ValueError("fact_daily debe contener una sola fecha de corte por particion.")

    fecha_corte = fechas[0]
    partition_file = get_partition_path(partitioned_dir, fecha_corte)
    partition_dir = partition_file.parent

    if partition_dir.exists() and replace_if_exists:
        shutil.rmtree(partition_dir)

    partition_dir.mkdir(parents=True, exist_ok=True)
    save_parquet(fact_daily, partition_file)

    return partition_file


def sanitize_fact_partitions(
    partitioned_dir: Path,
    valid_ubicacion_keys: set[str],
) -> dict[str, Any]:
    parquet_files = list(partitioned_dir.glob("fecha_corte=*/data.parquet"))
    summary = {
        "files_scanned": len(parquet_files),
        "files_rewritten": 0,
        "invalid_position_count": 0,
        "invalid_position_keys": [],
        "invalid_rows": pd.DataFrame(),
    }

    audit_frames: list[pd.DataFrame] = []
    invalid_keys: set[str] = set()

    for parquet_file in parquet_files:
        fact_partition = pd.read_parquet(parquet_file)
        sanitized_partition, audit = sanitize_fact_ubicaciones(fact_partition, valid_ubicacion_keys)

        summary["invalid_position_count"] += audit["invalid_position_count"]
        invalid_keys.update(audit["invalid_position_keys"])
        if not audit["invalid_rows"].empty:
            audit_rows = audit["invalid_rows"].copy()
            audit_rows["partition_file"] = str(parquet_file)
            audit_frames.append(audit_rows)

        comparable_original = fact_partition.copy().convert_dtypes()
        comparable_sanitized = sanitized_partition.copy().convert_dtypes()
        if not comparable_original.equals(comparable_sanitized):
            save_parquet(sanitized_partition, parquet_file)
            summary["files_rewritten"] += 1

    summary["invalid_position_keys"] = sorted(invalid_keys)
    if audit_frames:
        summary["invalid_rows"] = pd.concat(audit_frames, ignore_index=True)

    return summary
