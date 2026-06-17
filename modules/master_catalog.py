import shutil
import tempfile
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger("flexy_etl")

def load_master_catalog(file_path: Path) -> pd.DataFrame:
    """
    Carga la lista maestra de productos desde una ruta compartida.
    Para prevenir bloqueos de archivos (por ejemplo, si está abierto en Excel en OneDrive/GDrive),
    copia el archivo a una ubicación temporal antes de leerlo.
    """
    if not file_path.exists():
        logger.error(f"[CATALOGO_MAESTRO] El archivo no existe en la ruta: {file_path}")
        return pd.DataFrame()

    temp_dir = tempfile.gettempdir()
    temp_file = Path(temp_dir) / f"temp_{file_path.name}"
    try:
        shutil.copy2(file_path, temp_file)
        # La hoja es 'verificada' y el encabezado comienza en la fila 1 (índice 0-basado).
        df = pd.read_excel(temp_file, sheet_name="verificada", header=1, engine="openpyxl")
        return df
    except Exception as e:
        logger.error(f"[CATALOGO_MAESTRO] Error al cargar la Lista Maestra de Productos: {e}")
        return pd.DataFrame()
    finally:
        if temp_file.exists():
            try:
                temp_file.unlink()
            except Exception:
                pass

def get_master_catalog_lookup(df: pd.DataFrame) -> dict[str, dict[str, object]]:
    """
    Transforma el DataFrame del catálogo maestro en un diccionario de búsqueda rápida
    indexado por 'TIPO DE PRODUCTO' (código SKU) en mayúsculas.
    """
    if df.empty:
        return {}

    lookup = {}
    sku_col = "TIPO DE PRODUCTO"
    product_col = "MATERIA PRIMA"
    weight_col = "Peso x UM"

    # Verificar que existan las columnas clave
    if sku_col not in df.columns or product_col not in df.columns or weight_col not in df.columns:
        logger.warning(
            f"[CATALOGO_MAESTRO] Faltan columnas en la Lista Maestra. "
            f"Columnas detectadas: {list(df.columns)}"
        )
        return {}

    for _, row in df.iterrows():
        sku_val = row[sku_col]
        if pd.isna(sku_val):
            continue

        sku = str(sku_val).strip().upper()
        if not sku or sku == "NAN" or sku == "NONE":
            continue

        materia_prima = str(row[product_col]).strip().upper() if pd.notna(row[product_col]) else "OTROS"
        
        try:
            peso = float(row[weight_col])
            if pd.isna(peso) or peso <= 0:
                peso = 1.0
        except (ValueError, TypeError):
            peso = 1.0

        lookup[sku] = {
            "producto": materia_prima,
            "peso": peso,
            "descripcion_corta": str(row.get("Descripcion Corta", "")).strip() if pd.notna(row.get("Descripcion Corta")) else "",
            "descripcion_larga": str(row.get("Descripcion Larga", "")).strip() if pd.notna(row.get("Descripcion Larga")) else "",
            "um": str(row.get("UM", "")).strip() if pd.notna(row.get("UM")) else "",
        }

    return lookup
