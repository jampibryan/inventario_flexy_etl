from itertools import product
from pathlib import Path
import pandas as pd


CAPACITY_CONFIG = [
    {"camara": "CÁMARA 01", "racks": 10, "niveles": 5, "posiciones": 15, "camara_orden": 1, "es_operativa": 1, "es_estructural": 1},
    {"camara": "CÁMARA 02", "racks": 20, "niveles": 3, "posiciones": 4, "camara_orden": 2, "es_operativa": 1, "es_estructural": 1},
    {"camara": "CÁMARA 03", "racks": 20, "niveles": 3, "posiciones": 4, "camara_orden": 3, "es_operativa": 1, "es_estructural": 1},
    {"camara": "CÁMARA 04", "racks": 13, "niveles": 11, "posiciones": 3, "camara_orden": 4, "es_operativa": 0, "es_estructural": 1},
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
        return pd.DataFrame(columns=["producto_key", "codigo", "producto", "presentacion", "tipo_produccion"])

    return (
        fact_df[
            ["producto_key", "CÓDIGO", "PRODUCTO", "PRESENTACIÓN", "TIPO PRODUCCIÓN"]
        ]
        .drop_duplicates()
        .rename(columns={
            "CÓDIGO": "codigo",
            "PRODUCTO": "producto",
            "PRESENTACIÓN": "presentacion",
            "TIPO PRODUCCIÓN": "tipo_produccion",
        })
        .sort_values(["producto", "codigo"])
        .reset_index(drop=True)
    )


def build_dim_fecha(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        return pd.DataFrame(columns=["fecha_key", "fecha", "anio", "mes_numero", "mes", "anio_mes", "trimestre", "semana", "dia"])

    fechas = pd.DataFrame({
        "fecha": pd.to_datetime(fact_df["FECHA CORTE"]).dropna().sort_values().unique()
    })

    fechas["fecha_key"] = fechas["fecha"].dt.strftime("%Y%m%d")
    fechas["anio"] = fechas["fecha"].dt.year
    fechas["mes_numero"] = fechas["fecha"].dt.month
    fechas["mes"] = fechas["fecha"].dt.strftime("%B")
    fechas["anio_mes"] = fechas["fecha"].dt.strftime("%Y-%m")
    fechas["trimestre"] = "T" + fechas["fecha"].dt.quarter.astype(str)
    fechas["semana"] = fechas["fecha"].dt.isocalendar().week.astype(int)
    fechas["dia"] = fechas["fecha"].dt.day

    return fechas


def build_dim_ubicacion() -> pd.DataFrame:
    rows = []

    for cfg in CAPACITY_CONFIG:
        for rack, nivel, posicion in product(
            range(1, cfg["racks"] + 1),
            range(1, cfg["niveles"] + 1),
            range(1, cfg["posiciones"] + 1),
        ):
            cam_num = cfg["camara"].replace("CÁMARA", "").strip().zfill(2)
            ubicacion_key = f"CAM{cam_num}-R{rack:03d}-N{nivel:02d}-P{posicion:02d}"

            rows.append({
                "ubicacion_key": ubicacion_key,
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