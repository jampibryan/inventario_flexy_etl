import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from modules.parquet_io import save_parquet
from modules.ubicaciones import sanitize_fact_ubicaciones


AUDIT_EXPLANATIONS = {
    "PALLET_LOGICO_DIRECTO": (
        "Se detectó 1 pallet válido en la ubicación.",
        "Se conserva en el inventario final.",
        "NO",
        "SI",
    ),
    "PALLET_LOGICO_CONSOLIDADO": (
        "Un mismo pallet estaba partido en varias filas del Excel.",
        "Se unifican las filas en 1 solo pallet lógico.",
        "NO",
        "SI",
    ),
    "PALLET_REINGRESO_CONSOLIDADO": (
        "La misma presentación volvió a la misma ubicación con fecha de fabricación cercana.",
        "Se trata como 1 solo pallet lógico y se consolida.",
        "NO",
        "SI",
    ),
    "MULTIPALLET_VALIDO": (
        "Hay más de 1 pallet en la ubicación y juntos sí caben.",
        "Se conservan todos en el inventario final.",
        "NO",
        "SI",
    ),
    "MULTIPALLET_VALIDO_CONSOLIDADO": (
        "Hay varios pallets válidos y alguno venía en varias filas.",
        "Se consolidan las filas necesarias y se conservan todos.",
        "NO",
        "SI",
    ),
    "CONFLICTO_RESUELTO_MAS_RECIENTE": (
        "La ubicación repetida ya no soporta todos los pallets detectados.",
        "Se conserva solo el pallet más reciente por FECHA FABRICACIÓN.",
        "NO",
        "SI",
    ),
    "DESCARTADO_CONFLICTO": (
        "Registro anterior o inconsistente frente a otro más reciente en la misma ubicación.",
        "No entra al inventario final; queda solo como auditoría.",
        "NO",
        "NO",
    ),
    "ERROR_SOBRECAPACIDAD": (
        "La ubicación supera la capacidad permitida.",
        "No entra al inventario final; queda para revisión.",
        "NO",
        "NO",
    ),
    "PASSTHROUGH_RECEPCIÓN": (
        "Registro ubicado en RECEPCIÓN.",
        "Se conserva tal como llegó, sin resolver ocupación interna.",
        "NO",
        "SI",
    ),
    "PASSTHROUGH_EXTERNO": (
        "Registro de almacén externo.",
        "Se conserva tal como llegó, fuera del control interno de ubicaciones.",
        "NO",
        "SI",
    ),
    "PASSTHROUGH_SIN_UBICACION": (
        "Registro sin ubicación estructural POSICIÓN.",
        "Se conserva, pero no participa en la resolución de ocupación interna.",
        "NO",
        "SI",
    ),
}

AUDIT_EXPLANATION_FALLBACK = (
    "Caso sin descripción especial.",
    "Revisar el detalle de la fila.",
    "NO",
    "REVISAR",
)


LOCATION_CASE_COLUMNS = {
    "ubicacion": "ubicacion_detectada",
    "tipo_caso": "caso_detectado",
    "pallets_detectados": "pallets_detectados",
    "cajas_totales": "cajas_detectadas",
    "capacidad_permitida": "capacidad_maxima",
    "codigos_detectados": "codigos_detectados",
    "detalle_pallets": "detalle_por_pallet",
    "filas_origen": "filas_excel",
    "decision": "decision_del_etl",
    "detalle": "explicacion",
}

LOCATION_CASE_ORDER = [
    "ubicacion_detectada",
    "caso_detectado",
    "pallets_detectados",
    "cajas_detectadas",
    "capacidad_maxima",
    "codigos_detectados",
    "detalle_por_pallet",
    "filas_excel",
    "decision_del_etl",
    "explicacion",
]


def _resolve_audit_explanation(tipo_registro: object) -> tuple[str, str, str, str]:
    return AUDIT_EXPLANATIONS.get(str(tipo_registro), AUDIT_EXPLANATION_FALLBACK)


def _extract_single_partition_date(df: pd.DataFrame, column: str, empty_message: str) -> str:
    if df.empty:
        raise ValueError(empty_message)

    fechas = pd.to_datetime(df[column], errors="coerce").dropna().dt.strftime("%Y-%m-%d").unique()
    if len(fechas) != 1:
        raise ValueError(f"{column} debe contener una sola fecha de corte por partición.")

    return str(fechas[0])


def _remove_file_if_exists(path: Path, removed_key: str, locked_key: str, results: dict[str, bool]) -> None:
    if not path.exists():
        return

    try:
        path.unlink()
        results[removed_key] = True
    except PermissionError:
        results[locked_key] = True


def _build_audit_business_view(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "tipo_registro_resuelto" not in df.columns:
        return df.copy()

    business_df = df.copy()
    explanations = business_df["tipo_registro_resuelto"].map(_resolve_audit_explanation)
    business_df["detectado_auditoria"] = explanations.map(lambda value: value[0])
    business_df["decision_etl_auditoria"] = explanations.map(lambda value: value[1])
    business_df["bloquea_archivo_flag"] = explanations.map(lambda value: value[2])
    business_df["impacto_salida_limpia"] = explanations.map(lambda value: value[3])
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
    return summary.rename(
        columns={
            "tipo_registro_resuelto": "resultado",
            "detectado_auditoria": "que_se_detecto",
            "decision_etl_auditoria": "que_hizo_el_etl",
            "bloquea_archivo_flag": "bloquea_archivo",
            "impacto_salida_limpia": "sale_en_inventario_final",
            "cantidad_filas_auditoria": "cantidad_filas",
        }
    )


def _build_location_cases_sheet(location_cases_df: pd.DataFrame | None) -> pd.DataFrame:
    if location_cases_df is None or location_cases_df.empty:
        return pd.DataFrame()

    location_cases_sheet = location_cases_df.copy()
    location_cases_sheet = location_cases_sheet.rename(columns=LOCATION_CASE_COLUMNS)
    visible_columns = [column for column in LOCATION_CASE_ORDER if column in location_cases_sheet.columns]
    return location_cases_sheet.loc[:, visible_columns]


def _build_audit_readme_sheet() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"hoja": "resumen", "para_que_sirve": "Vista rápida de lo que detectó y resolvió el ETL."},
            {"hoja": "inventario_final", "para_que_sirve": "Registros que sí quedaron en el inventario limpio."},
            {"hoja": "detalle_auditoria", "para_que_sirve": "Detalle por fila: consolidaciones, descartes, sobrecapacidad y excepciones."},
            {"hoja": "casos_ubicacion", "para_que_sirve": "Explicación por ubicación repetida: qué pasó y qué decisión tomó el ETL."},
        ]
    )


def _format_dates_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    df_copy = df.copy()
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].dt.strftime("%Y-%m-%d")
        else:
            df_copy[col] = df_copy[col].apply(
                lambda x: x.strftime("%Y-%m-%d") if (hasattr(x, "strftime") and not pd.isna(x)) else x
            )
    return df_copy


def _autosize_worksheet(worksheet, df: pd.DataFrame, max_width: int = 40, workbook=None) -> None:
    if df.empty:
        return

    center_format = None
    if workbook is not None:
        center_format = workbook.add_format({"align": "center"})

    for col_idx, column in enumerate(df.columns):
        sample_values = df[column].head(100).tolist()
        sample_lengths = [len("" if pd.isna(value) else str(value)) for value in sample_values]
        width = max(len(str(column)), max(sample_lengths, default=0)) + 2
        worksheet.set_column(col_idx, col_idx, min(width, max_width), center_format)

    worksheet.freeze_panes(1, 0)


def save_daily_outputs(df: pd.DataFrame, excel_path: Path) -> None:
    """
    Guarda Excel transformado para revisión humana.
    """
    excel_path.parent.mkdir(parents=True, exist_ok=True)
    df_excel = df.copy()
    if "_SOURCE_ROW_NUM" in df_excel.columns:
        df_excel.drop(columns=["_SOURCE_ROW_NUM"], inplace=True)

    df_excel = _format_dates_for_excel(df_excel)
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        df_excel.to_excel(writer, sheet_name="Sheet1", index=False)
        _autosize_worksheet(writer.sheets["Sheet1"], df_excel, max_width=28, workbook=writer.book)


def save_resolution_audit_workbook(
    fact_clean: pd.DataFrame,
    audit_df: pd.DataFrame,
    workbook_path: Path,
    location_cases_df: pd.DataFrame | None = None,
) -> None:
    """
    Guarda un Excel de revisión con el inventario resuelto y la auditoría.
    """
    workbook_path.parent.mkdir(parents=True, exist_ok=True)
    readme_sheet = _build_audit_readme_sheet()
    fact_business = _build_audit_business_view(fact_clean)
    audit_business = _build_audit_business_view(audit_df)
    summary_sheet = _build_audit_summary_sheet(audit_business)
    location_cases_sheet = _build_location_cases_sheet(location_cases_df)

    fact_business = _format_dates_for_excel(fact_business)
    audit_business = _format_dates_for_excel(audit_business)
    if not summary_sheet.empty:
        summary_sheet = _format_dates_for_excel(summary_sheet)
    if not location_cases_sheet.empty:
        location_cases_sheet = _format_dates_for_excel(location_cases_sheet)

    with pd.ExcelWriter(workbook_path, engine="xlsxwriter") as writer:
        readme_sheet.to_excel(writer, sheet_name="leeme", index=False)
        if not summary_sheet.empty:
            summary_sheet.to_excel(writer, sheet_name="resumen", index=False)
        fact_business.to_excel(writer, sheet_name="inventario_final", index=False)
        audit_business.to_excel(writer, sheet_name="detalle_auditoria", index=False)
        if not location_cases_sheet.empty:
            location_cases_sheet.to_excel(writer, sheet_name="casos_ubicacion", index=False)

        _autosize_worksheet(writer.sheets["leeme"], readme_sheet, max_width=45)
        if not summary_sheet.empty:
            _autosize_worksheet(writer.sheets["resumen"], summary_sheet, max_width=45, workbook=writer.book)
        _autosize_worksheet(writer.sheets["inventario_final"], fact_business, max_width=28, workbook=writer.book)
        _autosize_worksheet(writer.sheets["detalle_auditoria"], audit_business, max_width=28, workbook=writer.book)
        if not location_cases_sheet.empty:
            _autosize_worksheet(writer.sheets["casos_ubicacion"], location_cases_sheet, max_width=45, workbook=writer.book)


def get_partition_path(base_dir: Path, fecha_corte: str) -> Path:
    """
    Devuelve la ruta de la partición:
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
    Guarda el snapshot diario en su partición.
    Si ya existe la partición y replace_if_exists=True, la reemplaza completa.
    """
    if fact_daily.empty:
        if not fecha_corte:
            raise ValueError("fact_daily está vacío y no se especificó fecha_corte.")
    else:
        fecha_corte = _extract_single_partition_date(
            fact_daily,
            "FECHA CORTE",
            "fact_daily está vacío y no se especificó fecha_corte.",
        )

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
    Guarda la auditoría diaria particionada por FECHA CORTE.
    """
    fecha_corte = _extract_single_partition_date(
        audit_daily,
        "FECHA CORTE",
        "audit_daily está vacío. No se puede crear una partición.",
    )
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

    _remove_file_if_exists(excel_output_path, "excel_removed", "excel_locked", results)
    _remove_file_if_exists(
        audit_workbook_path,
        "audit_workbook_removed",
        "audit_workbook_locked",
        results,
    )

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
