import re

import pandas as pd

from config import FINAL_COLUMNS
from modules.ubicaciones import normalize_camara


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


def clasificar_producto(texto_producto: str) -> str:
    producto_upper = str(texto_producto).upper()

    if "MANGO" in producto_upper:
        return "MANGO"
    if "PALTA" in producto_upper:
        return "PALTA"
    if "FRESA" in producto_upper:
        return "FRESA"
    if "PI\u00d1A" in producto_upper:
        return "PI\u00d1A"
    if "MARACUYA" in producto_upper:
        return "MARACUYA"
    if "GRANADA" in producto_upper:
        return "GRANADA"

    return "OTROS"


def clasificar_clasificacion(texto_producto: str) -> str:
    producto_upper = str(texto_producto).upper()
    if "ORGANICO" in producto_upper or "ORG\u00c1NICO" in producto_upper:
        return "ORG\u00c1NICO"
    return "CONVENCIONAL"


def limpiar_presentacion(texto_producto: str, producto: str) -> str:
    resultado = str(texto_producto).strip()
    resultado = re.sub(r"(?i)^" + re.escape(producto) + r"\s*", "", resultado)
    resultado = re.sub(r"(?i)\bORG[A\u00c1]NICO\b", "", resultado)
    resultado = re.sub(r"(?i)\bCONVENCIONAL\b", "", resultado)
    resultado = re.sub(r"\s+", " ", resultado).strip()
    return resultado


def obtener_variedad(producto: str, presentacion: str) -> str | None:
    producto_upper = str(producto).strip().upper()
    presentacion_upper = str(presentacion).strip().upper()

    if producto_upper == "MANGO":
        if "EDWARD" in presentacion_upper:
            return "EDWARD"
        if "KENT" in presentacion_upper:
            return "KENT"
        return "OTROS"
    if producto_upper == "FRESA":
        return "SABRINA"
    if producto_upper == "PALTA":
        return "HASS"
    if producto_upper == "GRANADA":
        return "WONDERFUL"
    if producto_upper == "MARACUYA":
        return "CRIOLLA"
    if producto_upper == "PI\u00d1A":
        return "GOLDEN"

    return None


def obtener_calidad(producto: str) -> str | None:
    if str(producto).strip().upper() == "MANGO":
        return None
    return "EST\u00c1NDAR"


def extraer_kg_por_caja(texto_producto: str) -> float | None:
    texto = str(texto_producto).upper().replace(",", ".")
    patterns = [
        r"\bX\s*(\d+(?:\.\d+)?)\s*KG\b",
        r"\bCJ\s*X\s*(\d+(?:\.\d+)?)\s*KG\b",
        r"\bCAJA\s*X\s*(\d+(?:\.\d+)?)\s*KG\b",
        r"\bCAJA\s*(\d+(?:\.\d+)?)\s*KG\b",
        r"\bCJ\s*(\d+(?:\.\d+)?)\s*KG\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, texto)
        if match:
            return float(match.group(1))

    return None


def resolver_presentacion_kg(
    texto_producto: str,
    presentacion_raw: float | int | None,
    cantidad_cajas: float | int | None,
) -> float | None:
    kg_por_caja = extraer_kg_por_caja(texto_producto)
    if kg_por_caja is not None:
        return kg_por_caja

    if pd.isna(presentacion_raw):
        return None

    presentacion_value = float(presentacion_raw)
    cantidad_value = pd.to_numeric(cantidad_cajas, errors="coerce")

    if pd.notna(cantidad_value) and float(cantidad_value) > 0 and presentacion_value > 100:
        return round(presentacion_value / float(cantidad_value), 4)

    return presentacion_value


def transform_inventory(df: pd.DataFrame, file_date: str) -> pd.DataFrame:
    df = df.copy()

    df["Fecha Actualizaci\u00f3n"] = pd.to_datetime(df["Fecha Actualizaci\u00f3n"], errors="coerce")
    df["Fecha Caducidad"] = pd.to_datetime(df["Fecha Caducidad"], dayfirst=True, errors="coerce").dt.date
    df["Fecha Fabricaci\u00f3n"] = pd.to_datetime(df["Fecha Fabricaci\u00f3n"], dayfirst=True, errors="coerce").dt.date

    for col in ["Empresa", "Almac\u00e9n", "Ubicaci\u00f3n", "C\u00f3digo", "Lote", "Producto"]:
        df[col] = df[col].astype(str).str.strip()

    df["Cantidad"] = (
        df["Cantidad"]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)
        .str.replace("\xa0", "", regex=False)
    )
    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce")
    df["Presentaci\u00f3n"] = pd.to_numeric(df["Presentaci\u00f3n"], errors="coerce")

    df["C\u00f3digo"] = df["C\u00f3digo"].replace({"nan": None, "None": None, "": None})
    df = df[df["C\u00f3digo"].notna()].copy()

    df["Almac\u00e9n Original"] = df["Almac\u00e9n"].astype(str).str.strip()
    df["Ubicaci\u00f3n Original"] = df["Ubicaci\u00f3n"].astype(str).str.strip()

    ubic_split = df["Ubicaci\u00f3n"].astype(str).str.split(",", expand=True)
    df["C\u00e1mara"] = ubic_split[0].str.strip() if 0 in ubic_split.columns else None
    df["Rack"] = ubic_split[1].str.strip() if 1 in ubic_split.columns else None
    df["Nivel"] = ubic_split[2].str.strip() if 2 in ubic_split.columns else None
    df["Posici\u00f3n"] = ubic_split[3].str.strip() if 3 in ubic_split.columns else None

    for col in ["Rack", "Nivel", "Posici\u00f3n"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["Presentacion Kg"] = df.apply(
        lambda row: resolver_presentacion_kg(
            row["Producto"],
            row["Presentaci\u00f3n"],
            row["Cantidad"],
        ),
        axis=1,
    )
    df["Toneladas"] = ((df["Cantidad"] * df["Presentacion Kg"]) / 1000).round(2)
    df["Fecha Corte"] = pd.to_datetime(file_date, errors="coerce").date()
    df["Cliente"] = df["Empresa"]
    df.rename(columns={"Cantidad": "Cantidad Cajas"}, inplace=True)

    df["Almac\u00e9n"] = df["Almac\u00e9n Original"].apply(clasificar_almacen)
    df["Estado Producto"] = df["Almac\u00e9n Original"].apply(clasificar_estado_producto)

    df["Producto Clasificado"] = df["Producto"].apply(clasificar_producto)
    df["Clasificaci\u00f3n"] = df["Producto"].apply(clasificar_clasificacion)
    df["Presentaci\u00f3n Limpia"] = df.apply(
        lambda row: limpiar_presentacion(row["Producto"], row["Producto Clasificado"]),
        axis=1,
    )
    df["Variedad"] = df.apply(
        lambda row: obtener_variedad(row["Producto Clasificado"], row["Presentaci\u00f3n Limpia"]),
        axis=1,
    )
    df["Calidad"] = df["Producto Clasificado"].apply(obtener_calidad)
    df["Tipo de Corte"] = None

    df.drop(columns=["Producto", "Presentaci\u00f3n", "Presentacion Kg"], inplace=True)
    df.rename(
        columns={
            "Producto Clasificado": "Producto",
            "Presentaci\u00f3n Limpia": "Presentaci\u00f3n",
        },
        inplace=True,
    )

    df["C\u00e1mara"] = df["C\u00e1mara"].apply(normalize_camara)

    df.columns = df.columns.str.upper()

    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    output_columns = FINAL_COLUMNS.copy()
    if "_SOURCE_ROW_NUM" in df.columns:
        output_columns.append("_SOURCE_ROW_NUM")
    for extra_col in ["ALMAC\u00c9N ORIGINAL", "UBICACI\u00d3N ORIGINAL"]:
        if extra_col in df.columns:
            output_columns.append(extra_col)

    df = df[output_columns].copy()

    for col in ["RACK", "NIVEL", "POSICI\u00d3N", "CANTIDAD CAJAS"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["TONELADAS"] = pd.to_numeric(df["TONELADAS"], errors="coerce").round(2)

    return df
