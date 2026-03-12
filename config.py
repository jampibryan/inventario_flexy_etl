from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

REPORTE_DIR = Path(r"D:\AG Chavin\Proyecto Flexy\Reporte")

# ==============================
# Carpetas principales
# ==============================
ORIGINAL_DIR = REPORTE_DIR / "ORIGINAL"

PROCESADOS_DIR = REPORTE_DIR / "PROCESADOS"
PROCESADOS_EXCEL_DIR = PROCESADOS_DIR / "Excel"

DW_DIR = REPORTE_DIR / "DW"
FACT_PARTITIONED_DIR = DW_DIR / "fact_inventario"

LOGS_DIR = REPORTE_DIR / "LOGS"

# ==============================
# Archivos de control / DW
# ==============================
CONTROL_FILE = PROCESADOS_DIR / "control_procesados.csv"

DIM_CLIENTE_FILE = DW_DIR / "dim_cliente.parquet"
DIM_PRODUCTO_FILE = DW_DIR / "dim_producto.parquet"
DIM_FECHA_FILE = DW_DIR / "dim_fecha.parquet"
DIM_UBICACION_FILE = DW_DIR / "dim_ubicacion.parquet"

LOG_FILE = LOGS_DIR / "etl.log"

# ==============================
# Columnas esperadas Excel Flexy
# ==============================
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