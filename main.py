from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path
import sys

import pandas as pd

from config import (
    AUDIT_PARTITIONED_DIR,
    BOX_CAPACITY_RULES,
    CONTROL_FILE,
    FACT_ACTUAL_FILE,
    DIM_CLIENTE_FILE,
    DIM_FECHA_FILE,
    DIM_PRODUCTO_FILE,
    DIM_UBICACION_FILE,
    DW_DIR,
    FACT_PARTITIONED_DIR,
    LOG_FILE,
    LOGS_DIR,
    MASTER_CATALOG_FILE,
    MULTIPALLET_COMPATIBILITY_RULES,
    ORIGINAL_DIR,
    PALLET_IDENTITY_FIELDS,
    PROCESADOS_AUDITORIA_DIR,
    PROCESADOS_DIR,
    PROCESADOS_EXCEL_DIR,
    SNAPSHOT_CONTROL_FILE,
)
from modules.master_catalog import load_master_catalog, get_master_catalog_lookup
from modules.data_quality.runner import (
    run_file_metadata_checks,
    run_input_quality_checks,
    run_transformed_quality_checks,
)
from modules.control import (
    add_control_record,
    is_already_processed,
    load_control_file,
    remove_control_records,
    save_control_file,
)
from modules.dimensiones import (
    append_observed_invalid_positions,
    build_dim_cliente,
    build_dim_fecha,
    build_dim_producto,
    build_dim_ubicacion,
    build_fact_from_partitions,
    summarize_dim_ubicacion_operativa,
)
from modules.extract import (
    read_excel_file,
    summarize_operational_resolution_candidates,
)
from modules.file_manager import get_original_excel_files
from modules.historico import (
    build_fact_actual,
    build_snapshot_control,
    summarize_snapshot_control,
)
from modules.load import (
    purge_reprocess_outputs,
    save_daily_outputs,
    save_resolution_audit_workbook,
    sanitize_fact_partitions,
    write_audit_partition,
    write_fact_partition,
)
from modules.parquet_io import save_parquet
from modules.resolucion_ocupacion import resolve_location_occupancy
from modules.snapshot import build_fact_snapshot
from modules.transform import transform_inventory
from modules.utils import build_output_names, ensure_directories


@dataclass(frozen=True)
class OutputTargets:
    excel_name: str
    excel_output_path: Path
    audit_workbook_path: Path


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("flexy_etl")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def log_ubicacion_audit(logger: logging.Logger, scope: str, audit: dict[str, object]) -> None:
    invalid_count = int(audit.get("invalid_position_count", 0))

    if invalid_count == 0:
        logger.info("[UBICACIONES][%s] sin observaciones", scope)
        return

    if audit.get("invalid_positions_preserved"):
        logger.warning(
            "[UBICACIONES][%s] %s registro(s) con ubicacion observada fuera de la dimension base. "
            "Se conservaron temporalmente.",
            scope,
            invalid_count,
        )
    else:
        logger.warning(
            "[UBICACIONES][%s] %s registro(s) con ubicacion no valida. "
            "Se enviaron a SIN_UBICACION.",
            scope,
            invalid_count,
        )


def log_resolution_summary(logger: logging.Logger, scope: str, summary: dict[str, int]) -> None:
    logger.info(
        "[RESUMEN][%s] filas=%s | pallets=%s | consolidados=%s | multipallet=%s | conflictos=%s | sobrecapacidad=%s",
        scope,
        summary.get("source_rows", 0),
        summary.get("logical_pallets", 0),
        summary.get("consolidated_pallets", 0),
        summary.get("multipallet_locations", 0),
        summary.get("conflict_locations", 0),
        summary.get("overcapacity_locations", 0),
    )


def log_location_cases(logger: logging.Logger, scope: str, location_cases: list[dict[str, object]]) -> None:
    separator = "-" * 70

    if not location_cases:
        logger.info(separator)
        logger.info("[ANALISIS_UBICACIONES][%s] No se detectaron ubicaciones repetidas para revisar.", scope)
        logger.info(separator)
        return

    logger.info(separator)
    logger.info("[ANALISIS_UBICACIONES][%s] Se revisaron %s ubicacion(es) repetidas.", scope, len(location_cases))
    logger.info("Regla aplicada: se compara la suma grupal de cajas de la ubicacion contra su capacidad.")
    logger.info("Si la suma cabe, se conservan los pallets. Si no cabe, se conserva solo el mas reciente.")

    for index, case in enumerate(location_cases, start=1):
        logger.info(separator)
        logger.info("CASO %s", index)
        logger.info("Ubicacion revisada: %s", case.get("ubicacion", case.get("ubicacion_key", "")))
        logger.info(
            "Pallets detectados: %s | Cajas totales en la ubicacion: %s | Capacidad permitida: %s",
            case.get("pallets_detectados", 0),
            case.get("cajas_totales", 0),
            case.get("capacidad_permitida", 0),
        )
        logger.info("Codigos detectados: %s", case.get("codigos_detectados", ""))
        logger.info("Detalle por pallet: %s", case.get("detalle_pallets", ""))
        logger.info("Filas del Excel involucradas: %s", case.get("filas_origen", ""))
        logger.info("Que detecto el ETL: %s", case.get("tipo_caso", ""))
        logger.info("Decision tomada: %s", case.get("decision", ""))
        logger.info("Explicacion: %s", case.get("detalle", ""))

    logger.info(separator)


def refresh_dimensions_from_partitions(
    logger: logging.Logger,
    dim_ubicacion,
    log_dim_ubicacion: bool = False,
) -> None:
    fact_hist = build_fact_from_partitions(FACT_PARTITIONED_DIR)
    dim_ubicacion_export = append_observed_invalid_positions(dim_ubicacion, fact_hist)
    dim_cliente = build_dim_cliente(fact_hist)
    dim_producto = build_dim_producto(fact_hist)
    dim_fecha = build_dim_fecha(fact_hist)
    fact_actual = build_fact_actual(fact_hist)
    snapshot_control = build_snapshot_control(FACT_PARTITIONED_DIR, AUDIT_PARTITIONED_DIR)

    save_parquet(dim_cliente, DIM_CLIENTE_FILE)
    save_parquet(dim_producto, DIM_PRODUCTO_FILE)
    save_parquet(dim_fecha, DIM_FECHA_FILE)
    save_parquet(fact_actual, FACT_ACTUAL_FILE)
    save_parquet(snapshot_control, SNAPSHOT_CONTROL_FILE)
    if log_dim_ubicacion:
        save_and_validate_dim_ubicacion(logger, dim_ubicacion_export)
    else:
        save_parquet(dim_ubicacion_export, DIM_UBICACION_FILE)

    snapshot_summary = summarize_snapshot_control(snapshot_control)
    log_fn = logger.warning if snapshot_summary["integrity_error"] > 0 else logger.info
    log_fn(
        "[SNAPSHOTS] fechas=%s | integridad_ok=%s | integridad_error=%s | ultimo_snapshot=%s",
        snapshot_summary["snapshots"],
        snapshot_summary["integrity_ok"],
        snapshot_summary["integrity_error"],
        snapshot_summary["latest_rows"],
    )


def log_force_cleanup(logger: logging.Logger, filename: str, file_date: str, cleanup: dict[str, bool]) -> None:
    removed_items = []
    locked_items = []

    if cleanup.get("excel_removed"):
        removed_items.append("excel_salida")
    if cleanup.get("audit_workbook_removed"):
        removed_items.append("excel_auditoria")
    if cleanup.get("fact_partition_removed"):
        removed_items.append("particion_fact")
    if cleanup.get("audit_partition_removed"):
        removed_items.append("particion_auditoria")
    if cleanup.get("excel_locked"):
        locked_items.append("excel_salida_bloqueado")
    if cleanup.get("audit_workbook_locked"):
        locked_items.append("excel_auditoria_bloqueado")

    if removed_items:
        logger.info(
            "[FORCE_CLEANUP] %s | fecha=%s | artefactos_previos_eliminados=%s",
            filename,
            file_date,
            ", ".join(removed_items),
        )
    else:
        logger.info(
            "[FORCE_CLEANUP] %s | fecha=%s | sin artefactos_previos_para_eliminar",
            filename,
            file_date,
        )

    if locked_items:
        logger.warning(
            "[FORCE_CLEANUP] %s | fecha=%s | artefactos_bloqueados=%s",
            filename,
            file_date,
            ", ".join(locked_items),
        )


def log_dim_ubicacion_summary(logger: logging.Logger, scope: str, dim_ubicacion: pd.DataFrame) -> None:
    summary = summarize_dim_ubicacion_operativa(dim_ubicacion)
    logger.info(
        "[DIM_UBICACION][%s] camaras=%s | estructural=%s | operativa=%s | no_operativa=%s",
        scope,
        len(summary),
        int(summary["capacidad_estructural"].sum()) if not summary.empty else 0,
        int(summary["capacidad_operativa_real"].sum()) if not summary.empty else 0,
        int(summary["ubicaciones_es_operativa_0"].sum()) if not summary.empty else 0,
    )


def save_and_validate_dim_ubicacion(logger: logging.Logger, dim_ubicacion: pd.DataFrame) -> None:
    log_dim_ubicacion_summary(logger, "dataframe_pre_export", dim_ubicacion)
    save_parquet(dim_ubicacion, DIM_UBICACION_FILE)
    persisted_dim_ubicacion = pd.read_parquet(DIM_UBICACION_FILE)
    log_dim_ubicacion_summary(logger, "parquet_exportado", persisted_dim_ubicacion)


def build_locked_fallback_output_path(path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return path.with_stem(f"{path.stem}_reproceso_{timestamp}")


def build_output_targets(file_date: str) -> OutputTargets:
    excel_name = build_output_names(file_date)
    return OutputTargets(
        excel_name=excel_name,
        excel_output_path=PROCESADOS_EXCEL_DIR / excel_name,
        audit_workbook_path=PROCESADOS_AUDITORIA_DIR / excel_name.replace(
            "_inventario.xlsx",
            "_auditoria_ocupacion.xlsx",
        ),
    )


def register_control_status(
    control_df: pd.DataFrame,
    *,
    archivo_original: str,
    fecha_archivo: str,
    estado: str,
    observacion: str = "",
    archivo_excel_salida: str = "",
    archivo_csv_salida: str = "",
) -> pd.DataFrame:
    return add_control_record(
        control_df=control_df,
        archivo_original=archivo_original,
        fecha_archivo=fecha_archivo,
        estado=estado,
        archivo_excel_salida=archivo_excel_salida,
        archivo_csv_salida=archivo_csv_salida,
        observacion=observacion,
    )


def apply_force_cleanup(
    *,
    logger: logging.Logger,
    control_df: pd.DataFrame,
    filename: str,
    file_date: str,
    output_targets: OutputTargets,
) -> tuple[pd.DataFrame, OutputTargets]:
    cleanup = purge_reprocess_outputs(
        fecha_corte=file_date,
        excel_output_path=output_targets.excel_output_path,
        audit_workbook_path=output_targets.audit_workbook_path,
        fact_partitioned_dir=FACT_PARTITIONED_DIR,
        audit_partitioned_dir=AUDIT_PARTITIONED_DIR,
    )
    control_df = remove_control_records(control_df, filename, file_date)
    log_force_cleanup(logger, filename, file_date, cleanup)

    excel_output_path = output_targets.excel_output_path
    audit_workbook_path = output_targets.audit_workbook_path

    if cleanup.get("excel_locked"):
        excel_output_path = build_locked_fallback_output_path(excel_output_path)
        logger.warning(
            "[FORCE_CLEANUP] %s | se usara una salida alterna para el Excel limpio: %s",
            filename,
            excel_output_path.name,
        )

    if cleanup.get("audit_workbook_locked"):
        audit_workbook_path = build_locked_fallback_output_path(audit_workbook_path)
        logger.warning(
            "[FORCE_CLEANUP] %s | se usara una salida alterna para la auditoria Excel: %s",
            filename,
            audit_workbook_path.name,
        )

    return control_df, OutputTargets(
        excel_name=output_targets.excel_name,
        excel_output_path=excel_output_path,
        audit_workbook_path=audit_workbook_path,
    )


def handle_blocked_file(
    *,
    logger: logging.Logger,
    control_df: pd.DataFrame,
    dim_ubicacion: pd.DataFrame,
    filename: str,
    file_date: str,
    estado: str,
    observacion: str,
    warning_message: str,
) -> pd.DataFrame:
    control_df = register_control_status(
        control_df,
        archivo_original=filename,
        fecha_archivo=file_date,
        estado=estado,
        observacion=observacion,
    )
    logger.warning(warning_message)
    refresh_dimensions_from_partitions(logger, dim_ubicacion)
    return control_df


def main() -> None:
    force = "--force" in sys.argv

    ensure_directories([
        PROCESADOS_DIR,
        PROCESADOS_EXCEL_DIR,
        PROCESADOS_AUDITORIA_DIR,
        DW_DIR,
        FACT_PARTITIONED_DIR,
        AUDIT_PARTITIONED_DIR,
        LOGS_DIR,
    ])

    logger = setup_logging()

    if force:
        logger.info("Modo --force activado: se reprocesaran archivos ya procesados.")

    logger.info("=== INICIO ETL INVENTARIO FLEXY ===")
    logger.info("Cargando Lista Maestra de Productos desde: %s", MASTER_CATALOG_FILE)
    df_catalog = load_master_catalog(MASTER_CATALOG_FILE)
    catalog_lookup = get_master_catalog_lookup(df_catalog)
    logger.info("Lista Maestra cargada. SKUs catalogados: %s", len(catalog_lookup))

    logger.info(
        "[CONFIG_OCUPACION] identidad_pallet=%s | reglas_capacidad=%s | reglas_multipallet=%s",
        ",".join(PALLET_IDENTITY_FIELDS),
        len(BOX_CAPACITY_RULES),
        len(MULTIPALLET_COMPATIBILITY_RULES),
    )

    dim_ubicacion = build_dim_ubicacion()
    valid_ubicacion_keys = set(dim_ubicacion["ubicacion_key"].dropna().astype(str))
    refresh_dimensions_from_partitions(logger, dim_ubicacion, log_dim_ubicacion=True)

    historical_audit = sanitize_fact_partitions(FACT_PARTITIONED_DIR, valid_ubicacion_keys)
    if historical_audit["files_scanned"] > 0:
        logger.info(
            "[INTEGRIDAD_UBICACION][historico] Particiones revisadas: %s | reescritas: %s",
            historical_audit["files_scanned"],
            historical_audit["files_rewritten"],
        )
        log_ubicacion_audit(logger, "historico", historical_audit)
        refresh_dimensions_from_partitions(logger, dim_ubicacion)

    control_df = load_control_file(CONTROL_FILE)
    original_files = get_original_excel_files(ORIGINAL_DIR)

    if not original_files:
        logger.info("No se encontraron archivos Excel en la carpeta ORIGINAL.")
        return

    processed_count = 0
    error_count = 0
    run_summary_records = []

    for file_path in original_files:
        filename = file_path.name

        if not force and is_already_processed(control_df, filename):
            logger.info(f"[SKIP] Ya procesado: {filename}")
            continue

        logger.info(f"[PROCESANDO] {filename}")
        file_date = ""

        try:
            df_raw = read_excel_file(file_path)
            metadata_quality = run_file_metadata_checks(df_raw, filename)
            file_date = metadata_quality.file_date

            if not metadata_quality.ok:
                blocked_issue = metadata_quality.blocking_issue
                if blocked_issue is None:
                    raise ValueError("Se esperaba una incidencia bloqueante de calidad de datos.")
                control_df = handle_blocked_file(
                    logger=logger,
                    control_df=control_df,
                    dim_ubicacion=dim_ubicacion,
                    filename=filename,
                    file_date=file_date,
                    estado=blocked_issue.status_code,
                    observacion=blocked_issue.message,
                    warning_message=blocked_issue.message,
                )
                run_summary_records.append({
                    "archivo": filename,
                    "fecha_corte": file_date if file_date else "DESCONOCIDA",
                    "estado": blocked_issue.status_code,
                    "pallets": 0,
                    "consolidaciones": 0,
                    "skus_no_catalogados": 0,
                    "observacion": blocked_issue.message
                })
                error_count += 1
                continue

            output_targets = build_output_targets(file_date)

            if force:
                control_df, output_targets = apply_force_cleanup(
                    logger=logger,
                    control_df=control_df,
                    filename=filename,
                    file_date=file_date,
                    output_targets=output_targets,
                )

            input_quality = run_input_quality_checks(df_raw, filename)
            for warning_message in input_quality.warning_messages:
                logger.info(warning_message)

            df_validated = input_quality.dataframe
            if df_validated is None:
                raise ValueError("La etapa de calidad de entrada no devolvio un DataFrame valido.")

            operational_message = summarize_operational_resolution_candidates(df_validated, filename)
            if operational_message:
                logger.info(operational_message)

            df_clean = transform_inventory(df_validated, file_date, catalog_lookup=catalog_lookup)
            fact_hist_before_file = build_fact_from_partitions(FACT_PARTITIONED_DIR)
            transformed_quality = run_transformed_quality_checks(
                df_clean,
                filename,
                historical_df=fact_hist_before_file,
                catalog_lookup=catalog_lookup,
            )
            for warning_message in transformed_quality.warning_messages:
                logger.info(warning_message)
            if not transformed_quality.ok:
                blocked_issue = transformed_quality.blocking_issue
                if blocked_issue is None:
                    raise ValueError("Se esperaba una incidencia bloqueante de calidad de datos transformados.")
                control_df = handle_blocked_file(
                    logger=logger,
                    control_df=control_df,
                    dim_ubicacion=dim_ubicacion,
                    filename=filename,
                    file_date=file_date,
                    estado=blocked_issue.status_code,
                    observacion=blocked_issue.message,
                    warning_message=blocked_issue.message,
                )
                run_summary_records.append({
                    "archivo": filename,
                    "fecha_corte": file_date,
                    "estado": blocked_issue.status_code,
                    "pallets": 0,
                    "consolidaciones": 0,
                    "skus_no_catalogados": 0,
                    "observacion": blocked_issue.message
                })
                error_count += 1
                continue

            save_daily_outputs(df_clean, output_targets.excel_output_path)

            fact_resolved, audit_daily, resolution_summary = resolve_location_occupancy(
                df_clean,
                filename,
                valid_ubicacion_keys,
            )
            log_resolution_summary(logger, f"archivo:{filename}", resolution_summary)
            log_location_cases(
                logger,
                f"archivo:{filename}",
                resolution_summary.get("location_cases", []),
            )
            save_resolution_audit_workbook(
                fact_resolved,
                audit_daily,
                output_targets.audit_workbook_path,
                pd.DataFrame(resolution_summary.get("location_cases", [])),
            )

            fact_daily, daily_audit = build_fact_snapshot(fact_resolved, filename, valid_ubicacion_keys)
            log_ubicacion_audit(logger, f"archivo:{filename}", daily_audit)

            partition_path = write_fact_partition(
                fact_daily=fact_daily,
                partitioned_dir=FACT_PARTITIONED_DIR,
                replace_if_exists=True,
                fecha_corte=file_date,
            )
            audit_partition_path = write_audit_partition(
                audit_daily=audit_daily,
                partitioned_dir=AUDIT_PARTITIONED_DIR,
                replace_if_exists=True,
            )

            refresh_dimensions_from_partitions(logger, dim_ubicacion)

            control_df = register_control_status(
                control_df,
                archivo_original=filename,
                fecha_archivo=file_date,
                estado="PROCESADO",
                archivo_excel_salida=output_targets.excel_name,
                archivo_csv_salida="",
                observacion=(
                    f"OK | fact: {partition_path} | audit: {audit_partition_path} | "
                    f"pallets_logicos={resolution_summary['logical_pallets']}"
                ),
            )

            processed_count += 1
            logger.info(f"[OK] {filename}")

            unique_skus = set(df_clean["CÓDIGO"].dropna().astype(str).str.strip().str.upper().unique())
            missing_skus = [sku for sku in unique_skus if sku != "SIN_SKU" and sku not in catalog_lookup]

            run_summary_records.append({
                "archivo": filename,
                "fecha_corte": file_date,
                "estado": "PROCESADO",
                "pallets": int(resolution_summary.get('logical_pallets', 0)),
                "consolidaciones": int(resolution_summary.get('consolidated_pallets', 0)),
                "skus_no_catalogados": len(missing_skus),
                "observacion": "OK" if len(missing_skus) == 0 else f"Advertencia: {len(missing_skus)} SKU(s) no catalogados"
            })

        except Exception as e:
            control_df = register_control_status(
                control_df,
                archivo_original=filename,
                fecha_archivo=file_date,
                estado="ERROR",
                observacion=str(e),
            )
            run_summary_records.append({
                "archivo": filename,
                "fecha_corte": file_date if file_date else "DESCONOCIDA",
                "estado": "ERROR",
                "pallets": 0,
                "consolidaciones": 0,
                "skus_no_catalogados": 0,
                "observacion": str(e)
            })
            error_count += 1
            logger.exception(f"[ERROR] {filename}: {e}")
            refresh_dimensions_from_partitions(logger, dim_ubicacion)

    save_control_file(control_df, CONTROL_FILE)
    refresh_dimensions_from_partitions(logger, dim_ubicacion, log_dim_ubicacion=True)

    if run_summary_records:
        logger.info("=" * 110)
        logger.info(" REPORTE RESUMEN DE PROCESAMIENTO (PIPELINE RUN SUMMARY)")
        logger.info("=" * 110)
        logger.info(
            f"{'ARCHIVO':<35} | {'FECHA CORTE':<12} | {'ESTADO':<15} | {'PALLETS':<8} | {'CONSOLID.':<9} | {'SKUs MISSING':<12}"
        )
        logger.info("-" * 110)
        for rec in run_summary_records:
            logger.info(
                f"{rec['archivo'][:35]:<35} | "
                f"{rec['fecha_corte']:<12} | "
                f"{rec['estado']:<15} | "
                f"{rec['pallets']:<8} | "
                f"{rec['consolidaciones']:<9} | "
                f"{rec['skus_no_catalogados']:<12}"
            )
            if rec['skus_no_catalogados'] > 0 or rec['estado'] != "PROCESADO":
                logger.info(f"   --> Observación: {rec['observacion']}")
        logger.info("=" * 110)

    logger.info("=== FIN ETL INVENTARIO FLEXY ===")
    logger.info(f"Procesados nuevos: {processed_count}")
    logger.info(f"Errores: {error_count}")


if __name__ == "__main__":
    main()
