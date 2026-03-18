import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from modules.parquet_io import save_parquet
from modules.ubicaciones import sanitize_fact_ubicaciones


def _build_audit_business_view(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "tipo_registro_resuelto" not in df.columns:
        return df.copy()

    business_df = df.copy()

    explanation_map = {
        "PALLET_LOGICO_DIRECTO": (
            "1 pallet logico directo en la ubicacion.",
            "Se conserva como 1 pallet logico vigente.",
            "NO",
            "SE CONSERVA EN FACT LIMPIA",
        ),
        "PALLET_LOGICO_CONSOLIDADO": (
            "Un mismo pallet estaba fragmentado en varias filas del Excel.",
            "Se consolidan las filas en 1 pallet logico vigente.",
            "NO",
            "SE CONSERVA EN FACT LIMPIA",
        ),
        "MULTIPALLET_VALIDO": (
            "Coexistencia valida de varios pallets pequenos en una sola ubicacion.",
            "Se conservan los pallets logicos validos y la ubicacion cuenta una sola vez.",
            "NO",
            "SE CONSERVA EN FACT LIMPIA",
        ),
        "MULTIPALLET_VALIDO_CONSOLIDADO": (
            "Coexistencia valida de multipallet y al menos uno estaba fragmentado en varias filas.",
            "Se consolidan las filas necesarias y se conservan los pallets logicos validos.",
            "NO",
            "SE CONSERVA EN FACT LIMPIA",
        ),
        "CONFLICTO_RESUELTO_MAS_RECIENTE": (
            "Coexistencia operativamente inconsistente en la misma ubicacion.",
            "Se conserva solo el registro mas reciente por FECHA FABRICACION.",
            "NO",
            "SE CONSERVA EN FACT LIMPIA",
        ),
        "DESCARTADO_CONFLICTO": (
            "Registro anterior o inconsistente frente a otro mas reciente en la misma ubicacion.",
            "Se descarta del inventario limpio y queda solo para auditoria.",
            "NO",
            "SOLO AUDITORIA",
        ),
        "ERROR_SOBRECAPACIDAD": (
            "La ubicacion excede la capacidad maxima permitida en cajas.",
            "Se excluye de la fact limpia y queda marcado para revision.",
            "NO",
            "SOLO AUDITORIA",
        ),
        "ERROR_SOBRECAPACIDAD_CONFLICTO": (
            "Habia conflicto por coexistencia y el registro vigente aun excede la capacidad maxima.",
            "Se excluye de la fact limpia y queda marcado para revision.",
            "NO",
            "SOLO AUDITORIA",
        ),
    }

    fallback = (
        "Registro sin regla explicativa especial.",
        "Se conserva segun resultado actual del ETL.",
        "NO",
        "REVISAR DETALLE",
    )

    business_df["detectado_auditoria"] = business_df["tipo_registro_resuelto"].map(
        lambda value: explanation_map.get(str(value), fallback)[0]
    )
    business_df["decision_etl_auditoria"] = business_df["tipo_registro_resuelto"].map(
        lambda value: explanation_map.get(str(value), fallback)[1]
    )
    business_df["bloquea_archivo_flag"] = business_df["tipo_registro_resuelto"].map(
        lambda value: explanation_map.get(str(value), fallback)[2]
    )
    business_df["impacto_salida_limpia"] = business_df["tipo_registro_resuelto"].map(
        lambda value: explanation_map.get(str(value), fallback)[3]
    )
    return business_df


def _build_audit_summary_sheet(audit_df: pd.DataFrame) -> pd.DataFrame:
    if audit_df.empty or "tipo_registro_resuelto" not in audit_df.columns:
        return pd.DataFrame()

    summary = (
        audit_df.groupby(
            [
                "tipo_registro_resuelto",
                "detectado_auditoria",
                "decision_etl_auditoria",
                "bloquea_archivo_flag",
                "impacto_salida_limpia",
            ],
            dropna=False,
        )
        .size()
        .reset_index(name="cantidad_filas_auditoria")
        .sort_values(by=["impacto_salida_limpia", "tipo_registro_resuelto"])
    )
    return summary


def save_daily_outputs(df: pd.DataFrame, excel_path: Path) -> None:
    """
    Guarda Excel transformado para revision humana.
    """
    excel_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(excel_path, index=False, engine="xlsxwriter")


def save_resolution_audit_workbook(
    fact_clean: pd.DataFrame,
    audit_df: pd.DataFrame,
    workbook_path: Path,
) -> None:
    """
    Guarda un Excel de revision con el inventario resuelto y la auditoria.
    """
    workbook_path.parent.mkdir(parents=True, exist_ok=True)
    fact_business = _build_audit_business_view(fact_clean)
    audit_business = _build_audit_business_view(audit_df)
    summary_sheet = _build_audit_summary_sheet(audit_business)

    with pd.ExcelWriter(workbook_path, engine="xlsxwriter") as writer:
        if not summary_sheet.empty:
            summary_sheet.to_excel(writer, sheet_name="resumen_decisiones", index=False)
        fact_business.to_excel(writer, sheet_name="inventario_logico", index=False)
        audit_business.to_excel(writer, sheet_name="auditoria_ocupacion", index=False)


def get_partition_path(base_dir: Path, fecha_corte: str) -> Path:
    """
    Devuelve la ruta de la particion:
    DW/fact_inventario/fecha_corte=YYYY-MM-DD/data.parquet
    """
    return base_dir / f"fecha_corte={fecha_corte}" / "data.parquet"


def write_fact_partition(
    fact_daily: pd.DataFrame,
    partitioned_dir: Path,
    replace_if_exists: bool = True,
    fecha_corte: str | None = None,
) -> Path:
    """
    Guarda el snapshot diario en su particion.
    Si ya existe la particion y replace_if_exists=True, la reemplaza completa.
    """
    if fact_daily.empty:
        if not fecha_corte:
            raise ValueError("fact_daily esta vacio y no se especifico fecha_corte.")
    else:
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


def write_audit_partition(
    audit_daily: pd.DataFrame,
    partitioned_dir: Path,
    replace_if_exists: bool = True,
) -> Path:
    """
    Guarda la auditoria diaria particionada por FECHA CORTE.
    """
    if audit_daily.empty:
        raise ValueError("audit_daily esta vacio. No se puede crear una particion.")

    fechas = pd.to_datetime(audit_daily["FECHA CORTE"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").unique()

    if len(fechas) != 1:
        raise ValueError("audit_daily debe contener una sola fecha de corte por particion.")

    fecha_corte = fechas[0]
    partition_file = get_partition_path(partitioned_dir, fecha_corte)
    partition_dir = partition_file.parent

    if partition_dir.exists() and replace_if_exists:
        shutil.rmtree(partition_dir)

    partition_dir.mkdir(parents=True, exist_ok=True)
    save_parquet(audit_daily, partition_file)

    return partition_file


def purge_reprocess_outputs(
    *,
    fecha_corte: str,
    excel_output_path: Path,
    audit_workbook_path: Path,
    fact_partitioned_dir: Path,
    audit_partitioned_dir: Path,
) -> dict[str, bool]:
    """
    Elimina artefactos diarios previos para que un reproceso no deje datos stale.
    """
    results = {
        "excel_removed": False,
        "audit_workbook_removed": False,
        "fact_partition_removed": False,
        "audit_partition_removed": False,
        "excel_locked": False,
        "audit_workbook_locked": False,
    }

    if excel_output_path.exists():
        try:
            excel_output_path.unlink()
            results["excel_removed"] = True
        except PermissionError:
            results["excel_locked"] = True

    if audit_workbook_path.exists():
        try:
            audit_workbook_path.unlink()
            results["audit_workbook_removed"] = True
        except PermissionError:
            results["audit_workbook_locked"] = True

    fact_partition_dir = get_partition_path(fact_partitioned_dir, fecha_corte).parent
    if fact_partition_dir.exists():
        shutil.rmtree(fact_partition_dir)
        results["fact_partition_removed"] = True

    audit_partition_dir = get_partition_path(audit_partitioned_dir, fecha_corte).parent
    if audit_partition_dir.exists():
        shutil.rmtree(audit_partition_dir)
        results["audit_partition_removed"] = True

    return results


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
