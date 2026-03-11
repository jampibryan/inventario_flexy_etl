import pandas as pd
import re
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
        return "REEMPAQUE"

    return "DISPONIBLE"


def clasificar_producto(presentacion: str) -> str:
    presentacion_upper = str(presentacion).upper()

    if "MANGO" in presentacion_upper:
        return "MANGO"
    if "PALTA" in presentacion_upper:
        return "PALTA"
    if "FRESA" in presentacion_upper:
        return "FRESA"
    if "PIÑA" in presentacion_upper:
        return "PIÑA"
    if "MARACUYA" in presentacion_upper:
        return "MARACUYA"
    if "GRANADA" in presentacion_upper:
        return "GRANADA"

    return "OTROS"


def clasificar_tipo_produccion(texto: str) -> str:
    texto_upper = str(texto).upper()
    if "ORGANICO" in texto_upper or "ORGÁNICO" in texto_upper:
        return "ORGÁNICO"
    return "CONVENCIONAL"


def limpiar_presentacion(texto: str, producto: str) -> str:
    resultado = str(texto).strip()
    # Quitar el nombre del producto del inicio
    resultado = re.sub(r'(?i)^' + re.escape(producto) + r'\s*', '', resultado)
    # Quitar ORGANICO/ORGÁNICO/CONVENCIONAL
    resultado = re.sub(r'(?i)\bORG[AÁ]NICO\b', '', resultado)
    resultado = re.sub(r'(?i)\bCONVENCIONAL\b', '', resultado)
    # Limpiar espacios extra
    resultado = re.sub(r'\s+', ' ', resultado).strip()
    return resultado


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
    # 9. Fecha Corte
    # ---------------------------
    df["Fecha Corte"] = pd.to_datetime(file_date, errors="coerce").date()

    # ---------------------------
    # 10. Empresa → Cliente
    # ---------------------------
    df["Cliente"] = df["Empresa"]

    # ---------------------------
    # 11. Renombrar Cantidad
    # ---------------------------
    df.rename(columns={"Cantidad": "Cantidad Cajas"}, inplace=True)

    # ---------------------------
    # 12. Normalizar almacén
    # ---------------------------
    df["Almacén"] = df["Almacén Original"].apply(clasificar_almacen)

    # ---------------------------
    # 13. Estado Producto
    # ---------------------------
    df["Estado Producto"] = df["Almacén Original"].apply(clasificar_estado_producto)

    # ---------------------------
    # 14. Clasificar producto, tipo producción y renombrar
    # ---------------------------
    df["Producto_clasificado"] = df["Producto"].apply(clasificar_producto)
    df["Tipo Producción"] = df["Producto"].apply(clasificar_tipo_produccion)
    df["Presentación_limpia"] = df.apply(
        lambda row: limpiar_presentacion(row["Producto"], row["Producto_clasificado"]), axis=1
    )
    df.drop(columns=["Producto", "Presentación"], inplace=True)
    df.rename(columns={"Producto_clasificado": "Producto", "Presentación_limpia": "Presentación"}, inplace=True)

    # ---------------------------
    # 15. Reemplazo de Cámara
    # ---------------------------
    df["Cámara"] = df["Cámara"].replace({
        "01": "CÁMARA 01",
        "02": "CÁMARA 02",
        "03": "CÁMARA 03",
        "RECEPCION": "RECEPCIÓN",
        "Recepcion": "RECEPCIÓN",
        "RECEPCIÓN": "RECEPCIÓN",
    })

    # ---------------------------
    # 16. Renombrar columnas a mayúsculas y asegurar finales
    # ---------------------------
    df.columns = df.columns.str.upper()

    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[FINAL_COLUMNS].copy()

    # ---------------------------
    # 17. Tipos finales
    # ---------------------------
    for col in ["RACK", "NIVEL", "POSICIÓN", "CANTIDAD CAJAS"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["TONELADAS"] = pd.to_numeric(df["TONELADAS"], errors="coerce").round(2)

    return df