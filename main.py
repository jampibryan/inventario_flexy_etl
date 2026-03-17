import logging
import sys

from config import (
    CONTROL_FILE,
    DIM_CLIENTE_FILE,
    DIM_FECHA_FILE,
    DIM_PRODUCTO_FILE,
    DIM_UBICACION_FILE,
    DW_DIR,
    FACT_PARTITIONED_DIR,
    LOG_FILE,
    LOGS_DIR,
    ORIGINAL_DIR,
    PROCESADOS_DIR,
    PROCESADOS_EXCEL_DIR,
)
from modules.control import (
    add_control_record,
    is_already_processed,
    load_control_file,
    save_control_file,
)
from modules.dimensiones import (
    build_dim_cliente,
    build_dim_fecha,
    build_dim_producto,
    build_dim_ubicacion,
    build_fact_from_partitions,
)
from modules.extract import (
    extract_date_from_data,
    read_excel_file,
    validate_expected_columns,
    validate_no_negatives,
)
from modules.file_manager import get_original_excel_files
from modules.load import save_daily_outputs, sanitize_fact_partitions, write_fact_partition
from modules.parquet_io import save_parquet
from modules.snapshot import build_fact_snapshot
from modules.transform import transform_inventory
from modules.utils import build_output_names, ensure_directories


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
    invalid_keys = audit.get("invalid_position_keys", [])
    invalid_rows = audit.get("invalid_rows")

    if invalid_count == 0:
        logger.info(f"[INTEGRIDAD_UBICACION][{scope}] OK | sin pallets POSICION fuera de dim_ubicacion")
        return

    logger.warning(
        f"[INTEGRIDAD_UBICACION][{scope}] {invalid_count} pallet(s) con ubicacion candidata sin match en dim_ubicacion. "
        "Fueron reclasificados a SIN_UBICACION y su ubicacion_key se dejo nula."
    )

    if invalid_keys:
        logger.warning(
            f"[INTEGRIDAD_UBICACION][{scope}] Claves fact sin match: {', '.join(map(str, invalid_keys))}"
        )

    if hasattr(invalid_rows, "empty") and not invalid_rows.empty:
        logger.warning(
            "[INTEGRIDAD_UBICACION][%s] Detalle primeras filas:\n%s",
            scope,
            invalid_rows.head(20).to_string(index=False),
        )


def main() -> None:
    force = "--force" in sys.argv

    ensure_directories([
        PROCESADOS_DIR,
        PROCESADOS_EXCEL_DIR,
        DW_DIR,
        FACT_PARTITIONED_DIR,
        LOGS_DIR,
    ])

    logger = setup_logging()

    if force:
        logger.info("Modo --force activado: se reprocesaran archivos ya procesados.")

    logger.info("=== INICIO ETL INVENTARIO FLEXY ===")

    dim_ubicacion = build_dim_ubicacion()
    valid_ubicacion_keys = set(dim_ubicacion["ubicacion_key"].dropna().astype(str))

    historical_audit = sanitize_fact_partitions(FACT_PARTITIONED_DIR, valid_ubicacion_keys)
    if historical_audit["files_scanned"] > 0:
        logger.info(
            "[INTEGRIDAD_UBICACION][historico] Particiones revisadas: %s | reescritas: %s",
            historical_audit["files_scanned"],
            historical_audit["files_rewritten"],
        )
        log_ubicacion_audit(logger, "historico", historical_audit)

    control_df = load_control_file(CONTROL_FILE)
    original_files = get_original_excel_files(ORIGINAL_DIR)

    if not original_files:
        save_parquet(dim_ubicacion, DIM_UBICACION_FILE)
        logger.info("No se encontraron archivos Excel en la carpeta ORIGINAL.")
        return

    processed_count = 0
    error_count = 0

    for file_path in original_files:
        filename = file_path.name

        if not force and is_already_processed(control_df, filename):
            logger.info(f"[SKIP] Ya procesado: {filename}")
            continue

        logger.info(f"[PROCESANDO] {filename}")
        file_date = ""

        try:
            df_raw = read_excel_file(file_path)

            valid_cols, cols_message = validate_expected_columns(df_raw)
            if not valid_cols:
                raise ValueError(cols_message)

            date_ok, file_date, date_error = extract_date_from_data(df_raw)
            if not date_ok:
                raise ValueError(date_error)

            excel_name = build_output_names(file_date)
            excel_output_path = PROCESADOS_EXCEL_DIR / excel_name

            valid_nums, nums_message, df_validated = validate_no_negatives(df_raw, filename)
            if nums_message:
                logger.info(nums_message)
            if not valid_nums:
                control_df = add_control_record(
                    control_df=control_df,
                    archivo_original=filename,
                    fecha_archivo=file_date,
                    estado="ERROR_NEGATIVOS",
                    observacion="Valores negativos detectados. Corregir Excel original.",
                )
                error_count += 1
                logger.warning(f"[BLOQUEADO] {filename}: corrige el Excel original.")
                continue

            df_clean = transform_inventory(df_validated, file_date)
            save_daily_outputs(df_clean, excel_output_path)

            fact_daily, daily_audit = build_fact_snapshot(df_clean, filename, valid_ubicacion_keys)
            log_ubicacion_audit(logger, f"archivo:{filename}", daily_audit)

            partition_path = write_fact_partition(
                fact_daily=fact_daily,
                partitioned_dir=FACT_PARTITIONED_DIR,
                replace_if_exists=True,
            )

            fact_hist = build_fact_from_partitions(FACT_PARTITIONED_DIR)

            dim_cliente = build_dim_cliente(fact_hist)
            dim_producto = build_dim_producto(fact_hist)
            dim_fecha = build_dim_fecha(fact_hist)

            save_parquet(dim_cliente, DIM_CLIENTE_FILE)
            save_parquet(dim_producto, DIM_PRODUCTO_FILE)
            save_parquet(dim_fecha, DIM_FECHA_FILE)
            save_parquet(dim_ubicacion, DIM_UBICACION_FILE)

            control_df = add_control_record(
                control_df=control_df,
                archivo_original=filename,
                fecha_archivo=file_date,
                estado="PROCESADO",
                archivo_excel_salida=excel_name,
                archivo_csv_salida="",
                observacion=f"OK | particion: {partition_path}",
            )

            processed_count += 1
            logger.info(f"[OK] {filename}")

        except Exception as e:
            control_df = add_control_record(
                control_df=control_df,
                archivo_original=filename,
                fecha_archivo=file_date,
                estado="ERROR",
                observacion=str(e),
            )
            error_count += 1
            logger.exception(f"[ERROR] {filename}: {e}")

    save_control_file(control_df, CONTROL_FILE)

    logger.info("=== FIN ETL INVENTARIO FLEXY ===")
    logger.info(f"Procesados nuevos: {processed_count}")
    logger.info(f"Errores: {error_count}")


if __name__ == "__main__":
    main()
