import sys

from config import (
    ORIGINAL_DIR,
    PROCESADOS_DIR,
    PROCESADOS_EXCEL_DIR,
    CONTROL_FILE,
    FACT_PARTITIONED_DIR,
    DIM_CLIENTE_FILE,
    DIM_PRODUCTO_FILE,
    DIM_FECHA_FILE,
    DIM_UBICACION_FILE,
    DW_DIR,
    LOGS_DIR,
)

from modules.utils import ensure_directories, build_output_names
from modules.file_manager import get_original_excel_files
from modules.extract import (
    read_excel_file,
    validate_expected_columns,
    validate_no_negatives,
    extract_date_from_data,
)
from modules.transform import transform_inventory
from modules.control import (
    load_control_file,
    save_control_file,
    is_already_processed,
    add_control_record,
)
from modules.snapshot import build_fact_snapshot
from modules.load import save_daily_outputs, write_fact_partition
from modules.dimensiones import (
    build_fact_from_partitions,
    build_dim_cliente,
    build_dim_producto,
    build_dim_fecha,
    build_dim_ubicacion,
)
from modules.parquet_io import save_parquet


def main() -> None:
    force = "--force" in sys.argv
    if force:
        print("⚠ Modo --force activado: se reprocesarán archivos ya procesados.")

    print("=== INICIO ETL INVENTARIO FLEXY ===")

    ensure_directories([
        PROCESADOS_DIR,
        PROCESADOS_EXCEL_DIR,
        DW_DIR,
        FACT_PARTITIONED_DIR,
        LOGS_DIR,
    ])

    control_df = load_control_file(CONTROL_FILE)
    original_files = get_original_excel_files(ORIGINAL_DIR)

    if not original_files:
        print("No se encontraron archivos Excel en la carpeta ORIGINAL.")
        return

    processed_count = 0
    error_count = 0

    for file_path in original_files:
        filename = file_path.name

        if not force and is_already_processed(control_df, filename):
            print(f"[SKIP] Ya procesado: {filename}")
            continue

        print(f"[PROCESANDO] {filename}")
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

            valid_nums, nums_message = validate_no_negatives(df_raw, filename)
            if not valid_nums:
                print(nums_message)
                control_df = add_control_record(
                    control_df=control_df,
                    archivo_original=filename,
                    fecha_archivo=file_date,
                    estado="ERROR_NEGATIVOS",
                    observacion="Valores negativos detectados. Corregir Excel original.",
                )
                error_count += 1
                print(f"[BLOQUEADO] {filename}: corrige el Excel original.")
                continue

            df_clean = transform_inventory(df_raw, file_date)

            # Excel transformado para usuarios
            save_daily_outputs(df_clean, excel_output_path)

            # Fact snapshot diaria
            fact_daily = build_fact_snapshot(df_clean, filename)

            # Guardar partición diaria
            partition_path = write_fact_partition(
                fact_daily=fact_daily,
                partitioned_dir=FACT_PARTITIONED_DIR,
                replace_if_exists=True,
            )

            # Leer todas las particiones para regenerar dimensiones
            fact_hist = build_fact_from_partitions(FACT_PARTITIONED_DIR)

            dim_cliente = build_dim_cliente(fact_hist)
            dim_producto = build_dim_producto(fact_hist)
            dim_fecha = build_dim_fecha(fact_hist)
            dim_ubicacion = build_dim_ubicacion()

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
                observacion=f"OK | partición: {partition_path}",
            )

            processed_count += 1
            print(f"[OK] {filename}")

        except Exception as e:
            control_df = add_control_record(
                control_df=control_df,
                archivo_original=filename,
                fecha_archivo=file_date,
                estado="ERROR",
                observacion=str(e),
            )
            error_count += 1
            print(f"[ERROR] {filename}: {e}")

    save_control_file(control_df, CONTROL_FILE)

    print("=== FIN ETL INVENTARIO FLEXY ===")
    print(f"Procesados nuevos: {processed_count}")
    print(f"Errores: {error_count}")


if __name__ == "__main__":
    main()