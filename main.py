import sys
from config import (
    ORIGINAL_DIR,
    ETL_DIR,
    ETL_EXCEL_DIR,
    ETL_CSV_DIR,
    CONTROL_FILE,
    HISTORICO_FILE,
    HISTORICO_TEMP_FILE,
)
from modules.utils import ensure_directories, build_output_names
from modules.file_manager import get_original_excel_files, validate_original_file, get_file_date
from modules.extract import read_excel_file, validate_expected_columns, validate_no_negatives
from modules.transform import transform_inventory
from modules.load import save_daily_outputs, rebuild_historical
from modules.control import (
    load_control_file,
    save_control_file,
    is_already_processed,
    add_control_record,
)


def main() -> None:
    force = "--force" in sys.argv
    if force:
        print("⚠ Modo --force activado: se reprocesarán TODOS los archivos.")

    print("=== INICIO ETL INVENTARIO FLEXY ===")

    ensure_directories([
        ORIGINAL_DIR,
        ETL_DIR,
        ETL_EXCEL_DIR,
        ETL_CSV_DIR,
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

        is_valid, message = validate_original_file(file_path)
        if not is_valid:
            print(f"[ERROR] {filename}: {message}")
            control_df = add_control_record(
                control_df=control_df,
                archivo_original=filename,
                fecha_archivo="",
                estado="ERROR",
                observacion=message,
            )
            error_count += 1
            continue

        file_date = get_file_date(file_path)
        excel_name, csv_name = build_output_names(file_date)

        excel_output_path = ETL_EXCEL_DIR / excel_name
        csv_output_path = ETL_CSV_DIR / csv_name

        try:
            df_raw = read_excel_file(file_path)

            valid_cols, cols_message = validate_expected_columns(df_raw)
            if not valid_cols:
                raise ValueError(cols_message)

            # Validar que no haya valores negativos
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
                print(f"[BLOQUEADO] {filename}: No se generarán salidas hasta corregir los valores negativos.")
                continue

            df_clean = transform_inventory(df_raw, file_date)

            save_daily_outputs(df_clean, excel_output_path, csv_output_path)

            control_df = add_control_record(
                control_df=control_df,
                archivo_original=filename,
                fecha_archivo=file_date,
                estado="PROCESADO",
                archivo_excel_salida=excel_name,
                archivo_csv_salida=csv_name,
                observacion="OK",
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

    try:
        rebuild_historical(ETL_CSV_DIR, HISTORICO_TEMP_FILE, HISTORICO_FILE)
        print("[OK] Histórico reconstruido correctamente.")
    except Exception as e:
        print(f"[ERROR] No se pudo reconstruir el histórico: {e}")

    print("=== FIN ETL INVENTARIO FLEXY ===")
    print(f"Procesados nuevos: {processed_count}")
    print(f"Errores: {error_count}")


if __name__ == "__main__":
    main()