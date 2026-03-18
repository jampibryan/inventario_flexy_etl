import csv
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CATALOGOS_DIR = BASE_DIR / "catalogos"

DEFAULT_REPORTE_DIR = Path(
    r"G:\Unidades compartidas\Departamento de TI\04. Proyectos\12. Dashboard Flexy\Power BI\Reporte"
)
REPORTE_DIR = Path(os.getenv("FLEXY_REPORTE_DIR", str(DEFAULT_REPORTE_DIR)))

# ==============================
# Carpetas principales
# ==============================
ORIGINAL_DIR = REPORTE_DIR / "ORIGINAL"

PROCESADOS_DIR = REPORTE_DIR / "PROCESADOS"
PROCESADOS_EXCEL_DIR = PROCESADOS_DIR / "Excel"
PROCESADOS_AUDITORIA_DIR = PROCESADOS_DIR / "Auditoria"

DW_DIR = REPORTE_DIR / "DW"
FACT_PARTITIONED_DIR = DW_DIR / "fact_inventario"
AUDIT_PARTITIONED_DIR = DW_DIR / "fact_inventario_auditoria"

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

CONTROLLED_INTERNAL_WAREHOUSES = [
    "CHAVIN CASMA DISPONIBLE",
    "CHAVIN CASMA REEMPAQUE",
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
    "VARIEDAD",
    "CLASIFICACIÓN",
    "CALIDAD",
    "TIPO DE CORTE",
    "PRESENTACIÓN",
]
 
# ==============================
# Reglas de ocupacion y capacidad
# ==============================
OCCUPANCY_RULES = {
    "default_max_boxes_per_location": 120,
    "default_max_logical_pallets_per_location": 2,
    "allow_mixed_products_within_location": False,
    "location_capacity_strategy": "min",
}

PALLET_IDENTITY_FIELDS = [
    "CLIENTE",
    "CODIGO",
    "PRODUCTO",
    "PRESENTACION",
    "ESTADO PRODUCTO",
]

DEFAULT_BOX_CAPACITY_RULES = [
    # Cambia a 90 los SKUs o presentaciones confirmadas por operacion.
    {"producto": "MANGO", "max_boxes": 120, "rule_name": "MANGO_DEFAULT_120"},
    {"producto": "PALTA", "max_boxes": 120, "rule_name": "PALTA_DEFAULT_120"},
    {"producto": "FRESA", "max_boxes": 120, "rule_name": "FRESA_DEFAULT_120"},
    {"producto": "GRANADA", "max_boxes": 120, "rule_name": "GRANADA_DEFAULT_120"},
    {"producto": "MARACUYA", "max_boxes": 120, "rule_name": "MARACUYA_DEFAULT_120"},
    {"producto": "PIÑA", "max_boxes": 120, "rule_name": "PINA_DEFAULT_120"},
    {"producto": "OTROS", "max_boxes": 120, "rule_name": "OTROS_DEFAULT_120"},
]

DEFAULT_MULTIPALLET_COMPATIBILITY_RULES = [
    {
        "rule_name": "SMALL_PALLETS_GENERIC",
        "almacen": "CHAVIN",
        "max_logical_pallets": 2,
        "max_total_boxes": 120,
        "max_boxes_per_pallet": 60,
        "require_distinct_products": True,
    },
]

BOX_CAPACITY_RULES_FILE = CATALOGOS_DIR / "box_capacity_rules.csv"
MULTIPALLET_COMPATIBILITY_RULES_FILE = CATALOGOS_DIR / "multipallet_compatibility_rules.csv"


def _parse_bool(value: str, default: bool = True) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().upper()
    if normalized in {"1", "TRUE", "SI", "S", "YES", "Y"}:
        return True
    if normalized in {"0", "FALSE", "NO", "N"}:
        return False
    return default


def _parse_int(value: str, default: int | None = None) -> int | None:
    text = str(value).strip()
    if not text:
        return default
    return int(float(text))


def _parse_list(value: str) -> list[str]:
    text = str(value).strip()
    if not text:
        return []
    return [item.strip() for item in text.split("|") if item.strip()]


def _load_csv_catalog(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def _load_box_capacity_rules() -> list[dict[str, object]]:
    rows = _load_csv_catalog(BOX_CAPACITY_RULES_FILE)
    if not rows:
        return DEFAULT_BOX_CAPACITY_RULES

    rules: list[dict[str, object]] = []
    for idx, row in enumerate(rows, start=1):
        if not _parse_bool(row.get("activo", ""), default=True):
            continue

        max_boxes = _parse_int(row.get("max_boxes", ""), default=None)
        if max_boxes is None:
            continue

        rule = {
            "max_boxes": max_boxes,
            "rule_name": row.get("rule_name", "").strip() or f"CSV_RULE_{idx:03d}",
        }

        if row.get("codigo", "").strip():
            rule["codigo"] = row["codigo"].strip()
        if row.get("producto", "").strip():
            rule["producto"] = row["producto"].strip()
        if row.get("presentacion_contains", "").strip():
            rule["presentacion_contains"] = row["presentacion_contains"].strip()

        rules.append(rule)

    return rules or DEFAULT_BOX_CAPACITY_RULES


def _load_multipallet_compatibility_rules() -> list[dict[str, object]]:
    rows = _load_csv_catalog(MULTIPALLET_COMPATIBILITY_RULES_FILE)
    if not rows:
        return DEFAULT_MULTIPALLET_COMPATIBILITY_RULES

    rules: list[dict[str, object]] = []
    for idx, row in enumerate(rows, start=1):
        if not _parse_bool(row.get("activo", ""), default=True):
            continue

        rule = {
            "rule_name": row.get("rule_name", "").strip() or f"MULTIPALLET_RULE_{idx:03d}",
            "almacen": row.get("almacen", "").strip(),
            "max_logical_pallets": _parse_int(
                row.get("max_logical_pallets", ""),
                default=OCCUPANCY_RULES["default_max_logical_pallets_per_location"],
            ),
            "max_total_boxes": _parse_int(
                row.get("max_total_boxes", ""),
                default=OCCUPANCY_RULES["default_max_boxes_per_location"],
            ),
            "max_boxes_per_pallet": _parse_int(
                row.get("max_boxes_per_pallet", ""),
                default=OCCUPANCY_RULES["default_max_boxes_per_location"],
            ),
            "min_boxes_per_pallet": _parse_int(row.get("min_boxes_per_pallet", ""), default=1),
            "require_distinct_products": _parse_bool(
                row.get("require_distinct_products", ""),
                default=False,
            ),
        }

        camaras = _parse_list(row.get("camaras", ""))
        codigos = _parse_list(row.get("codigos", ""))
        productos = _parse_list(row.get("productos", ""))

        if camaras:
            rule["camaras"] = camaras
        if codigos:
            rule["codigos"] = codigos
        if productos:
            rule["productos"] = productos

        rules.append(rule)

    return rules or DEFAULT_MULTIPALLET_COMPATIBILITY_RULES


BOX_CAPACITY_RULES = _load_box_capacity_rules()
MULTIPALLET_COMPATIBILITY_RULES = _load_multipallet_compatibility_rules()
