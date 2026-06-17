import pandas as pd

from config import FINAL_COLUMNS
from modules.fechas import parse_datetime_series
from modules.productos import (
    clasificar_almacen,
    clasificar_clasificacion,
    clasificar_estado_producto,
    clasificar_producto,
    limpiar_presentacion,
    obtener_calidad,
    obtener_variedad,
    resolver_presentacion_kg,
)
from modules.ubicaciones import normalize_camara


TEXT_COLUMNS = [
    "Empresa",
    "Almac\u00e9n",
    "Ubicaci\u00f3n",
    "C\u00f3digo",
    "Lote",
    "Producto",
]


def _normalize_text_columns(df: pd.DataFrame) -> None:
    for column in TEXT_COLUMNS:
        df[column] = df[column].astype(str).str.strip()


def _normalize_numeric_columns(df: pd.DataFrame) -> None:
    df["Cantidad"] = (
        df["Cantidad"]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)
        .str.replace("\xa0", "", regex=False)
    )
    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce")
    df["Presentaci\u00f3n"] = pd.to_numeric(df["Presentaci\u00f3n"], errors="coerce")


def _split_ubicacion(df: pd.DataFrame) -> None:
    ubic_split = df["Ubicaci\u00f3n"].astype(str).str.split(",", expand=True)
    df["C\u00e1mara"] = ubic_split[0].str.strip() if 0 in ubic_split.columns else None
    df["Rack"] = ubic_split[1].str.strip() if 1 in ubic_split.columns else None
    df["Nivel"] = ubic_split[2].str.strip() if 2 in ubic_split.columns else None
    df["Posici\u00f3n"] = ubic_split[3].str.strip() if 3 in ubic_split.columns else None

    for column in ["Rack", "Nivel", "Posici\u00f3n"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")


def _build_output_columns(df: pd.DataFrame) -> list[str]:
    output_columns = FINAL_COLUMNS.copy()

    if "_SOURCE_ROW_NUM" in df.columns:
        output_columns.append("_SOURCE_ROW_NUM")

    for extra_column in ["ALMAC\u00c9N ORIGINAL", "UBICACI\u00d3N ORIGINAL"]:
        if extra_column in df.columns:
            output_columns.append(extra_column)

    return output_columns


def transform_inventory(df: pd.DataFrame, file_date: str, catalog_lookup: dict | None = None) -> pd.DataFrame:
    df = df.copy()

    df["Fecha Actualizaci\u00f3n"] = parse_datetime_series(df["Fecha Actualizaci\u00f3n"])
    df["Fecha Caducidad"] = parse_datetime_series(df["Fecha Caducidad"]).dt.date
    df["Fecha Fabricaci\u00f3n"] = parse_datetime_series(df["Fecha Fabricaci\u00f3n"]).dt.date

    _normalize_text_columns(df)
    _normalize_numeric_columns(df)

    # En lugar de eliminar filas con Código nulo, las rellenamos con 'SIN_SKU'
    df["C\u00f3digo"] = df["C\u00f3digo"].astype(str).str.strip().str.upper()
    df["C\u00f3digo"] = df["C\u00f3digo"].replace({"NAN": "SIN_SKU", "NONE": "SIN_SKU", "": "SIN_SKU", "<NA>": "SIN_SKU", "NAT": "SIN_SKU"})
    df["C\u00f3digo"] = df["C\u00f3digo"].fillna("SIN_SKU")

    df["Almac\u00e9n Original"] = df["Almac\u00e9n"].astype(str).str.strip()
    df["Ubicaci\u00f3n Original"] = df["Ubicaci\u00f3n"].astype(str).str.strip()

    _split_ubicacion(df)

    # Resolver Peso x UM (Presentacion Kg)
    presentacion_kg_list = []
    for codigo, producto, presentacion, cantidad in zip(
        df["C\u00f3digo"],
        df["Producto"],
        df["Presentaci\u00f3n"],
        df["Cantidad"],
    ):
        sku = str(codigo).strip().upper()
        if catalog_lookup and sku in catalog_lookup:
            peso = catalog_lookup[sku]["peso"]
            presentacion_kg_list.append(peso)
        elif sku == "SIN_SKU":
            presentacion_kg_list.append(1.0)
        else:
            peso = resolver_presentacion_kg(producto, presentacion, cantidad)
            presentacion_kg_list.append(peso if peso is not None else 1.0)

    df["Presentacion Kg"] = presentacion_kg_list
    df["Toneladas"] = ((df["Cantidad"] * df["Presentacion Kg"]) / 1000).round(2)
    df["Fecha Corte"] = pd.to_datetime(file_date, errors="coerce").date()
    df["Cliente"] = df["Empresa"]
    df.rename(columns={"Cantidad": "Cantidad Cajas"}, inplace=True)

    df["Almac\u00e9n"] = df["Almac\u00e9n Original"].apply(clasificar_almacen)
    df["Estado Producto"] = df["Almac\u00e9n Original"].apply(clasificar_estado_producto)

    # Clasificar Producto, Clasificación, Presentación, Variedad y Calidad
    producto_clasificado_list = []
    clasificacion_list = []
    presentacion_limpia_list = []
    variedad_list = []
    calidad_list = []

    for codigo, texto_producto in zip(df["C\u00f3digo"], df["Producto"]):
        sku = str(codigo).strip().upper()
        if catalog_lookup and sku in catalog_lookup:
            materia_prima = catalog_lookup[sku]["producto"]
            desc_corta = catalog_lookup[sku]["descripcion_corta"]
            
            p_clasificado = materia_prima
            clasif = clasificar_clasificacion(texto_producto) # mantiene lógica de orgánico/convencional del texto
            
            if desc_corta:
                pres_limpia = limpiar_presentacion(desc_corta, p_clasificado)
            else:
                pres_limpia = limpiar_presentacion(texto_producto, p_clasificado)
                
            var = obtener_variedad(p_clasificado, pres_limpia)
            cal = obtener_calidad(p_clasificado)
        elif sku == "SIN_SKU":
            p_clasificado = "PRODUCTO SIN CLASIFICAR"
            clasif = "SIN CLASIFICAR"
            pres_limpia = "SIN CLASIFICAR"
            var = "SIN CLASIFICAR"
            cal = "SIN CLASIFICAR"
        else:
            p_clasificado = clasificar_producto(texto_producto)
            clasif = clasificar_clasificacion(texto_producto)
            pres_limpia = limpiar_presentacion(texto_producto, p_clasificado)
            var = obtener_variedad(p_clasificado, pres_limpia)
            cal = obtener_calidad(p_clasificado)

        producto_clasificado_list.append(p_clasificado)
        clasificacion_list.append(clasif)
        presentacion_limpia_list.append(pres_limpia)
        variedad_list.append(var)
        calidad_list.append(cal)

    df["Producto Clasificado"] = producto_clasificado_list
    df["Clasificaci\u00f3n"] = clasificacion_list
    df["Presentaci\u00f3n Limpia"] = presentacion_limpia_list
    df["Variedad"] = variedad_list
    df["Calidad"] = calidad_list
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

    df = df[_build_output_columns(df)].copy()

    for col in ["RACK", "NIVEL", "POSICI\u00d3N", "CANTIDAD CAJAS"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["TONELADAS"] = pd.to_numeric(df["TONELADAS"], errors="coerce").round(2)

    return df
