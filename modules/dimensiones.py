from itertools import product
from pathlib import Path

import pandas as pd

from modules.ubicaciones import build_ubicacion_key


CAPACITY_CONFIG = [
    {"camara": "C\u00c1MARA 01", "racks": 10, "niveles": 5, "posiciones": 15, "camara_orden": 1, "es_operativa": 1, "es_estructural": 1},
    {"camara": "C\u00c1MARA 02", "racks": 20, "niveles": 3, "posiciones": 4, "camara_orden": 2, "es_operativa": 1, "es_estructural": 1},
    {"camara": "C\u00c1MARA 03", "racks": 20, "niveles": 3, "posiciones": 4, "camara_orden": 3, "es_operativa": 1, "es_estructural": 1},
    {"camara": "C\u00c1MARA 04", "racks": 13, "niveles": 11, "posiciones": 3, "camara_orden": 4, "es_operativa": 0, "es_estructural": 1},
]


def build_fact_from_partitions(partitioned_dir: Path) -> pd.DataFrame:
    parquet_files = list(partitioned_dir.glob("fecha_corte=*/data.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(p) for p in parquet_files]
    return pd.concat(dfs, ignore_index=True)


def build_dim_cliente(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        return pd.DataFrame(columns=["cliente_key", "cliente"])

    return (
        fact_df[["cliente_key", "CLIENTE"]]
        .drop_duplicates()
        .rename(columns={"CLIENTE": "cliente"})
        .sort_values("cliente")
        .reset_index(drop=True)
    )


def build_dim_producto(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        return pd.DataFrame(
            columns=["producto_key", "codigo", "producto", "variedad", "clasificacion", "calidad", "tipo_corte", "presentacion"]
        )

    df = fact_df[["C\u00d3DIGO", "PRODUCTO", "PRESENTACI\u00d3N", "CLASIFICACI\u00d3N"]].drop_duplicates().copy()
    df["producto"] = df["PRODUCTO"]

    def obtener_variedad(row: pd.Series) -> str | None:
        prod = str(row["PRODUCTO"]).strip().upper()
        present = str(row["PRESENTACI\u00d3N"]).strip().upper()
        if prod == "MANGO":
            if "EDWARD" in present:
                return "EDWARD"
            if "KENT" in present:
                return "KENT"
            return "OTROS"
        if prod == "FRESA":
            return "SABRINA"
        if prod == "PALTA":
            return "HASS"
        if prod == "GRANADA":
            return "WONDERFUL"
        if prod == "MARACUYA":
            return "CRIOLLA"
        if prod == "PI\u00d1A":
            return "GOLDEN"
        return None

    df["variedad"] = df.apply(obtener_variedad, axis=1)
    df["clasificacion"] = df["CLASIFICACI\u00d3N"]

    def obtener_calidad(row: pd.Series) -> str | None:
        prod = str(row["PRODUCTO"]).strip().upper()
        if prod == "MANGO":
            return None
        return "EST\u00c1NDAR"

    df["calidad"] = df.apply(obtener_calidad, axis=1)
    df["tipo_corte"] = pd.Series(pd.NA, index=df.index, dtype="string")
    df["presentacion"] = df["PRESENTACI\u00d3N"]

    df["producto_key"] = df["C\u00d3DIGO"].astype(str).str.strip().str.upper()
    df["codigo"] = df["C\u00d3DIGO"].astype(str).str.strip().str.upper()

    df_final = df[
        ["producto_key", "codigo", "producto", "variedad", "clasificacion", "calidad", "tipo_corte", "presentacion"]
    ].copy()
    df_final["tipo_corte"] = df_final["tipo_corte"].astype("string")
    return df_final.reset_index(drop=True)


MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}


def build_dim_fecha(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        return pd.DataFrame(
            columns=["fecha_key", "fecha", "anio", "mes_numero", "mes", "anio_mes", "trimestre", "semana", "dia"]
        ).astype({"fecha_key": "Int64"})

    fechas = pd.DataFrame({
        "fecha": pd.to_datetime(fact_df["FECHA CORTE"]).dropna().sort_values().unique()
    })

    fechas["fecha_key"] = fechas["fecha"].dt.strftime("%Y%m%d").astype(int)
    fechas["anio"] = fechas["fecha"].dt.year
    fechas["mes_numero"] = fechas["fecha"].dt.month
    fechas["mes"] = fechas["mes_numero"].map(MESES_ES)
    fechas["anio_mes"] = fechas["fecha"].dt.strftime("%Y-%m")
    fechas["trimestre"] = "T" + fechas["fecha"].dt.quarter.astype(str)
    fechas["semana"] = fechas["fecha"].dt.isocalendar().week.astype(int)
    fechas["dia"] = fechas["fecha"].dt.day
    fechas["fecha"] = fechas["fecha"].dt.date

    return fechas


def build_dim_ubicacion() -> pd.DataFrame:
    rows = []

    for cfg in CAPACITY_CONFIG:
        for rack, nivel, posicion in product(
            range(1, cfg["racks"] + 1),
            range(1, cfg["niveles"] + 1),
            range(1, cfg["posiciones"] + 1),
        ):
            rows.append({
                "ubicacion_key": build_ubicacion_key(cfg["camara"], rack, nivel, posicion),
                "almacen": "CHAVIN",
                "camara": cfg["camara"],
                "rack": rack,
                "nivel": nivel,
                "posicion": posicion,
                "camara_orden": cfg["camara_orden"],
                "es_operativa": cfg["es_operativa"],
                "es_estructural": cfg["es_estructural"],
            })

    return pd.DataFrame(rows)
