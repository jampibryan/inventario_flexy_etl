import pandas as pd
from config import FINAL_COLUMNS


def clasificar_almacen(almacen_original: str) -> str:
    almacen_upper = str(almacen_original).upper()

    if "CHAVIN" in almacen_upper:
        return "CHAVIN"
    if "ACUAPESCA" in almacen_upper:
        return "ACUAPESCA"
    if "EMERGENT" in almacen_upper:
        return "EMERGENT COLD"

    return str(almacen_original).strip()


def clasificar_estado_producto(almacen_original: str) -> str:
    almacen_upper = str(almacen_original).upper()

    if "CHAVIN" in almacen_upper and "REEMPAQUE" in almacen_upper:
        return "Reempaque"

    return "Disponible"


def clasificar_fruta(producto: str) -> str:
    producto_upper = str(producto).upper()

    if "MANGO" in producto_upper:
        return "Mango"
    if "PALTA" in producto_upper:
        return "Palta"
    if "FRESA" in producto_upper:
        return "Fresa"
    if "PIÑA" in producto_upper:
        return "Piña"
    if "MARACUYA" in producto_upper:
        return "Maracuya"
    if "GRANADA" in producto_upper:
        return "Granada"

    return "Otros"


def transform_inventory(df: pd.DataFrame, file_date: str) -> pd.DataFrame:

    df = df.copy()

    # ---------------------------
    # 1. Tipos base
    # ---------------------------
    df["Fecha Actualización"] = pd.to_datetime(df["Fecha Actualización"], errors="coerce")
    df["Fecha Caducidad"] = pd.to_datetime(df["Fecha Caducidad"], dayfirst=True, errors="coerce").dt.date
    df["Fecha Fabricación"] = pd.to_datetime(df["Fecha Fabricación"], dayfirst=True, errors="coerce").dt.date

    for col in ["Empresa", "Almacén", "Ubicación", "Código", "Lote", "Producto"]:
        df[col] = df[col].astype(str).str.strip()

    # ---------------------------
    # 2. Limpiar Cantidad
    # ---------------------------
    df["Cantidad"] = (
        df["Cantidad"]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)
        .str.replace("\xa0", "", regex=False)
    )

    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce")

    # ---------------------------
    # 3. Presentación a numérico
    # ---------------------------
    df["Presentación"] = pd.to_numeric(df["Presentación"], errors="coerce")

    # ---------------------------
    # 4. Filtrar Código nulo
    # ---------------------------
    df["Código"] = df["Código"].replace({"nan": None, "None": None, "": None})
    df = df[df["Código"].notna()].copy()

    # ---------------------------
    # 5. Guardar almacén original
    # ---------------------------
    df["Almacén Original"] = df["Almacén"].astype(str).str.strip()

    # ---------------------------
    # 6. Dividir Ubicación
    # ---------------------------
    ubic_split = df["Ubicación"].astype(str).str.split(",", expand=True)

    df["Cámara"] = ubic_split[0].str.strip() if 0 in ubic_split.columns else None
    df["Rack"] = ubic_split[1].str.strip() if 1 in ubic_split.columns else None
    df["Nivel"] = ubic_split[2].str.strip() if 2 in ubic_split.columns else None
    df["Posición"] = ubic_split[3].str.strip() if 3 in ubic_split.columns else None

    # ---------------------------
    # 7. Convertir columnas numéricas
    # ---------------------------
    for col in ["Rack", "Nivel", "Posición"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ---------------------------
    # 8. Crear Toneladas
    # ---------------------------
    df["Toneladas"] = (df["Presentación"] / 1000).round(2)

    # ---------------------------
    # 9. Valor absoluto
    # ---------------------------
    for col in ["Rack", "Nivel", "Posición", "Cantidad", "Toneladas"]:
        df[col] = df[col].abs()

    # ---------------------------
    # 10. Fecha Corte
    # ---------------------------
    df["Fecha Corte"] = pd.to_datetime(file_date, errors="coerce").date()

    # ---------------------------
    # 11. Empresa → Cliente
    # ---------------------------
    df["Cliente"] = df["Empresa"]

    # ---------------------------
    # 12. Normalizar almacén
    # ---------------------------
    df["Almacén"] = df["Almacén Original"].apply(clasificar_almacen)

    # ---------------------------
    # 13. Estado Producto
    # ---------------------------
    df["Estado Producto"] = df["Almacén Original"].apply(clasificar_estado_producto)

    # ---------------------------
    # 14. Fruta
    # ---------------------------
    df["Fruta"] = df["Producto"].apply(clasificar_fruta)

    # ---------------------------
    # 15. Reemplazo de Cámara
    # ---------------------------
    df["Cámara"] = df["Cámara"].replace({
        "01": "Cámara 01",
        "02": "Cámara 02",
        "03": "Cámara 03",
        "RECEPCION": "Recepción",
        "Recepcion": "Recepción",
        "RECEPCIÓN": "Recepción",
    })

    # ---------------------------
    # 16. Asegurar columnas finales
    # ---------------------------
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[FINAL_COLUMNS].copy()

    # ---------------------------
    # 17. Tipos finales
    # ---------------------------
    for col in ["Rack", "Nivel", "Posición", "Cantidad"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["Toneladas"] = pd.to_numeric(df["Toneladas"], errors="coerce").round(2)

    return df