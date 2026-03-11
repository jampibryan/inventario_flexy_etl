from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

REPORTE_DIR = Path(r"D:\AG Chavin\Proyecto Flexy\Reporte")

ORIGINAL_DIR = REPORTE_DIR / "ORIGINAL"
ETL_DIR = REPORTE_DIR / "ETL"
ETL_EXCEL_DIR = ETL_DIR / "Excel"
ETL_CSV_DIR = ETL_DIR / "CSV"

CONTROL_FILE = ETL_DIR / "control_procesados.csv"
HISTORICO_FILE = ETL_CSV_DIR / "inventario_historico.csv"
HISTORICO_TEMP_FILE = ETL_CSV_DIR / "inventario_historico_temp.csv"
LOG_FILE = BASE_DIR / "logs" / "etl.log"

EXPECTED_COLUMNS = [
    "Fecha Actualización",
    "Empresa",
    "Almacén",
    "Ubicación",
    "Código",
    "Cantidad",
    "Presentación",
    "Lote",
    "Fecha Caducidad",
    "Fecha Fabricación",
    "Producto",
]

FINAL_COLUMNS = [
    "FECHA CORTE",
    "CLIENTE",
    "ALMACÉN",
    "ESTADO PRODUCTO",
    "CÁMARA",
    "RACK",
    "NIVEL",
    "POSICIÓN",
    "CÓDIGO",
    "CANTIDAD CAJAS",
    "TONELADAS",
    "LOTE",
    "FECHA FABRICACIÓN",
    "FECHA CADUCIDAD",
    "PRODUCTO",
    "PRESENTACIÓN",
    "TIPO PRODUCCIÓN",
]