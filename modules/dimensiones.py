from itertools import product
from pathlib import Path
import re

import pandas as pd

from modules.productos import obtener_calidad, obtener_variedad
from modules.resolucion_ocupacion import resolve_box_capacity_rule
from modules.ubicaciones import POSICION_LABEL, build_ubicacion_key, strip_accents


CAPACITY_CONFIG = [
    {
        "camara": "C\u00c1MARA 01",
        "racks": 10,
        "niveles": 5,
        "posiciones": 15,
        "camara_orden": 1,
        "es_operativa": 1,
        "es_estructural": 1,
        "capacidad_operativa_real": 700,
        "capacidad_operativa_mode": "MANUAL_TEMPORAL",
    },
    {
        "camara": "C\u00c1MARA 02",
        "racks": 20,
        "niveles": 3,
        "posiciones": 4,
        "camara_orden": 2,
        "es_operativa": 1,
        "es_estructural": 1,
        "capacidad_operativa_real": 232,
        "capacidad_operativa_mode": "MANUAL_TEMPORAL",
    },
    {
        "camara": "C\u00c1MARA 03",
        "racks": 20,
        "niveles": 3,
        "posiciones": 4,
        "camara_orden": 3,
        "es_operativa": 1,
        "es_estructural": 1,
        "capacidad_operativa_real": 232,
        "capacidad_operativa_mode": "MANUAL_TEMPORAL",
    },
    {
        "camara": "C\u00c1MARA 04",
        "sections": [
            {
                "rack_inicio": 1,
                "rack_fin": 13,
                "niveles": 3,
                "posiciones": 6,
            },
            {
                "rack_inicio": 14,
                "rack_fin": 26,
                "niveles": 3,
                "posiciones": 5,
            },
        ],
        "camara_orden": 4,
        "es_operativa": 1,
        "es_estructural": 1,
        "capacidad_operativa_real": 429,
        "capacidad_operativa_mode": "FULL_STRUCTURE",
    },
]


def get_camera_structural_sections(cfg: dict[str, object]) -> list[dict[str, int]]:
    raw_sections = cfg.get("sections")
    if raw_sections:
        sections: list[dict[str, int]] = []
        for section in raw_sections:
            sections.append(
                {
                    "rack_inicio": int(section["rack_inicio"]),
                    "rack_fin": int(section["rack_fin"]),
                    "niveles": int(section["niveles"]),
                    "posiciones": int(section["posiciones"]),
                }
            )
        return sections

    return [
        {
            "rack_inicio": 1,
            "rack_fin": int(cfg["racks"]),
            "niveles": int(cfg["niveles"]),
            "posiciones": int(cfg["posiciones"]),
        }
    ]


def resolve_camera_section(cfg: dict[str, object], rack: int | None) -> dict[str, int] | None:
    if rack is None:
        return None

    for section in get_camera_structural_sections(cfg):
        if section["rack_inicio"] <= int(rack) <= section["rack_fin"]:
            return section

    return None


def get_camera_capacity_limits(cfg: dict[str, object]) -> dict[str, int]:
    sections = get_camera_structural_sections(cfg)
    rack_min = min(section["rack_inicio"] for section in sections)
    rack_max = max(section["rack_fin"] for section in sections)
    nivel_max = max(section["niveles"] for section in sections)
    posicion_max = max(section["posiciones"] for section in sections)
    capacidad_estructural = sum(
        (section["rack_fin"] - section["rack_inicio"] + 1)
        * section["niveles"]
        * section["posiciones"]
        for section in sections
    )
    return {
        "rack_min": rack_min,
        "rack_max": rack_max,
        "nivel_max": nivel_max,
        "posicion_max": posicion_max,
        "capacidad_estructural": capacidad_estructural,
    }


def _canonicalize_product_fact_columns(fact_df: pd.DataFrame) -> pd.DataFrame:
    rename_map: dict[str, str] = {}
    canonical_map = {
        "CODIGO": "CÓDIGO",
        "CDIGO": "CÓDIGO",
        "PRESENTACION": "PRESENTACIÓN",
        "PRESENTACIN": "PRESENTACIÓN",
        "CLASIFICACION": "CLASIFICACIÓN",
        "CLASIFICACIN": "CLASIFICACIÓN",
    }

    for column in fact_df.columns:
        normalized = strip_accents(str(column)).upper()
        normalized = re.sub(r"[^A-Z0-9 ]+", "", normalized)
        normalized = " ".join(normalized.split())
        rename_target = canonical_map.get(normalized)
        if rename_target:
            rename_map[column] = rename_target

    return fact_df.rename(columns=rename_map)


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
            columns=[
                "producto_key",
                "codigo",
                "producto",
                "variedad",
                "clasificacion",
                "calidad",
                "tipo_corte",
                "presentacion",
                "max_cajas_configuradas",
                "regla_capacidad",
            ]
        )

    fact_df = _canonicalize_product_fact_columns(fact_df.copy())
    ranking_df = fact_df.copy()
    ranking_df["producto_key"] = ranking_df["C\u00d3DIGO"].astype(str).str.strip().str.upper()
    ranking_df["fecha_corte_rank"] = pd.to_datetime(ranking_df.get("FECHA CORTE"), errors="coerce")
    ranking_df["cantidad_cajas_rank"] = pd.to_numeric(
        ranking_df.get("CANTIDAD CAJAS"), errors="coerce"
    ).fillna(0)

    product_rank = (
        ranking_df.groupby(
            ["producto_key", "C\u00d3DIGO", "PRODUCTO", "PRESENTACI\u00d3N", "CLASIFICACI\u00d3N"],
            dropna=False,
        )
        .agg(
            fecha_corte_rank=("fecha_corte_rank", "max"),
            apariciones=("producto_key", "size"),
            cantidad_cajas_rank=("cantidad_cajas_rank", "sum"),
        )
        .reset_index()
    )

    df = product_rank[["C\u00d3DIGO", "PRODUCTO", "PRESENTACI\u00d3N", "CLASIFICACI\u00d3N"]].copy()
    df["producto"] = df["PRODUCTO"]
    df["variedad"] = [
        obtener_variedad(producto, presentacion)
        for producto, presentacion in zip(df["PRODUCTO"], df["PRESENTACI\u00d3N"])
    ]
    df["clasificacion"] = df["CLASIFICACI\u00d3N"]
    df["calidad"] = [obtener_calidad(producto) for producto in df["PRODUCTO"]]
    df["tipo_corte"] = pd.Series(pd.NA, index=df.index, dtype="string")
    df["presentacion"] = df["PRESENTACI\u00d3N"]
    capacity_rules = df.apply(resolve_box_capacity_rule, axis=1)
    df["max_cajas_configuradas"] = capacity_rules.apply(lambda value: value[0])
    df["regla_capacidad"] = capacity_rules.apply(lambda value: value[1])

    df["producto_key"] = df["C\u00d3DIGO"].astype(str).str.strip().str.upper()
    df["codigo"] = df["C\u00d3DIGO"].astype(str).str.strip().str.upper()

    df_final = df[
        [
            "producto_key",
            "codigo",
            "producto",
            "variedad",
            "clasificacion",
            "calidad",
            "tipo_corte",
            "presentacion",
            "max_cajas_configuradas",
            "regla_capacidad",
            "PRODUCTO",
            "PRESENTACI\u00d3N",
            "CLASIFICACI\u00d3N",
        ]
    ].copy()
    df_final["tipo_corte"] = df_final["tipo_corte"].astype("string")
    df_final = df_final.merge(
        product_rank[
            [
                "producto_key",
                "PRODUCTO",
                "PRESENTACI\u00d3N",
                "CLASIFICACI\u00d3N",
                "fecha_corte_rank",
                "apariciones",
                "cantidad_cajas_rank",
            ]
        ],
        on=["producto_key", "PRODUCTO", "PRESENTACI\u00d3N", "CLASIFICACI\u00d3N"],
        how="left",
    )
    df_final = df_final.sort_values(
        [
            "producto_key",
            "fecha_corte_rank",
            "apariciones",
            "cantidad_cajas_rank",
            "presentacion",
            "producto",
            "clasificacion",
        ],
        ascending=[True, False, False, False, True, True, True],
        na_position="last",
    )
    df_final = df_final.drop_duplicates(subset=["producto_key"], keep="first")
    df_final = df_final[
        [
            "producto_key",
            "codigo",
            "producto",
            "variedad",
            "clasificacion",
            "calidad",
            "tipo_corte",
            "presentacion",
            "max_cajas_configuradas",
            "regla_capacidad",
        ]
    ]
    return df_final.reset_index(drop=True)


MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}


def build_dim_fecha(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        return pd.DataFrame(
            columns=[
                "fecha_key",
                "fecha",
                "anio",
                "mes_numero",
                "mes",
                "anio_mes",
                "trimestre",
                "semana",
                "dia",
                "es_fecha_snapshot_flag",
                "es_ultimo_snapshot_flag",
            ]
        ).astype({"fecha_key": "Int64"})

    snapshot_dates = pd.to_datetime(fact_df["FECHA CORTE"], errors="coerce").dropna().dt.normalize()
    if snapshot_dates.empty:
        return pd.DataFrame(
            columns=[
                "fecha_key",
                "fecha",
                "anio",
                "mes_numero",
                "mes",
                "anio_mes",
                "trimestre",
                "semana",
                "dia",
                "es_fecha_snapshot_flag",
                "es_ultimo_snapshot_flag",
            ]
        ).astype({"fecha_key": "Int64"})

    calendar_range = pd.date_range(snapshot_dates.min(), snapshot_dates.max(), freq="D")
    fechas = pd.DataFrame({"fecha": calendar_range})
    snapshot_date_set = set(snapshot_dates.unique().tolist())
    latest_snapshot = snapshot_dates.max()

    fechas["fecha_key"] = fechas["fecha"].dt.strftime("%Y%m%d").astype(int)
    fechas["anio"] = fechas["fecha"].dt.year
    fechas["mes_numero"] = fechas["fecha"].dt.month
    fechas["mes"] = fechas["mes_numero"].map(MESES_ES)
    fechas["anio_mes"] = fechas["fecha"].dt.strftime("%Y-%m")
    fechas["trimestre"] = "T" + fechas["fecha"].dt.quarter.astype(str)
    fechas["semana"] = fechas["fecha"].dt.isocalendar().week.astype(int)
    fechas["dia"] = fechas["fecha"].dt.day
    fechas["es_fecha_snapshot_flag"] = fechas["fecha"].isin(snapshot_date_set).astype(int)
    fechas["es_ultimo_snapshot_flag"] = fechas["fecha"].eq(latest_snapshot).astype(int)
    fechas["fecha"] = fechas["fecha"].dt.date

    return fechas


def build_dim_ubicacion() -> pd.DataFrame:
    rows = []

    for cfg in CAPACITY_CONFIG:
        sections = get_camera_structural_sections(cfg)
        capacidad_estructural = get_camera_capacity_limits(cfg)["capacidad_estructural"]
        capacidad_operativa_real = int(cfg.get("capacidad_operativa_real", capacidad_estructural))
        capacidad_operativa_real = max(0, min(capacidad_operativa_real, capacidad_estructural))
        mode = str(cfg.get("capacidad_operativa_mode", "FULL_STRUCTURE"))
        secuencia = 0

        for section in sections:
            for rack, nivel, posicion in product(
                range(section["rack_inicio"], section["rack_fin"] + 1),
                range(1, section["niveles"] + 1),
                range(1, section["posiciones"] + 1),
            ):
                secuencia += 1
                rows.append({
                    "ubicacion_key": build_ubicacion_key(cfg["camara"], rack, nivel, posicion),
                    "almacen": "CHAVIN",
                    "camara": cfg["camara"],
                    "rack": rack,
                    "nivel": nivel,
                    "posicion": posicion,
                    "camara_orden": cfg["camara_orden"],
                    "es_operativa": 1 if int(cfg["es_operativa"]) == 1 and secuencia <= capacidad_operativa_real else 0,
                    "es_estructural": cfg["es_estructural"],
                    "capacidad_estructural_camara": capacidad_estructural,
                    "capacidad_operativa_real_camara": capacidad_operativa_real,
                    "capacidad_operativa_mode": mode,
                    "posicion_operativa_temporal_flag": 1 if secuencia <= capacidad_operativa_real else 0,
                    "secuencia_posicion_camara": secuencia,
                })

    dim = pd.DataFrame(rows)
    dim["ubicacion_invalida_estructura_flag"] = 0
    return dim


def append_observed_invalid_positions(
    dim_ubicacion: pd.DataFrame,
    fact_df: pd.DataFrame,
) -> pd.DataFrame:
    if dim_ubicacion.empty or fact_df.empty:
        return dim_ubicacion

    required_columns = {"ubicacion_key", "CÁMARA", "RACK", "NIVEL", "POSICIÓN"}
    if not required_columns.issubset(set(fact_df.columns)):
        return dim_ubicacion

    base_keys = set(dim_ubicacion["ubicacion_key"].dropna().astype(str))
    observed = fact_df.copy()

    if "tipo_ubicacion" in observed.columns:
        observed = observed[observed["tipo_ubicacion"] == POSICION_LABEL].copy()

    observed = observed[observed["ubicacion_key"].notna()].copy()
    observed["ubicacion_key"] = observed["ubicacion_key"].astype(str)
    observed = observed[~observed["ubicacion_key"].isin(base_keys)].copy()

    if observed.empty:
        return dim_ubicacion

    observed_dim = (
        observed[["ubicacion_key", "CÁMARA", "RACK", "NIVEL", "POSICIÓN"]]
        .drop_duplicates()
        .rename(
            columns={
                "CÁMARA": "camara",
                "RACK": "rack",
                "NIVEL": "nivel",
                "POSICIÓN": "posicion",
            }
        )
        .copy()
    )
    observed_dim["almacen"] = "CHAVIN"
    observed_dim["camara_orden"] = observed_dim["camara"].astype(str).str.extract(r"(\d+)").fillna("999").astype(int)
    observed_dim["es_operativa"] = 0
    observed_dim["es_estructural"] = 0
    observed_dim["capacidad_estructural_camara"] = 0
    observed_dim["capacidad_operativa_real_camara"] = 0
    observed_dim["capacidad_operativa_mode"] = "OBSERVADA_INVALIDA"
    observed_dim["posicion_operativa_temporal_flag"] = 0
    observed_dim["secuencia_posicion_camara"] = pd.NA
    observed_dim["ubicacion_invalida_estructura_flag"] = 1

    dim_final = pd.concat([dim_ubicacion, observed_dim], ignore_index=True, sort=False)
    return dim_final


def summarize_dim_ubicacion_operativa(dim_ubicacion: pd.DataFrame) -> pd.DataFrame:
    if dim_ubicacion.empty:
        return pd.DataFrame(
            columns=[
                "camara",
                "capacidad_estructural",
                "capacidad_operativa_real",
                "ubicaciones_es_operativa_1",
                "ubicaciones_es_operativa_0",
            ]
        )

    summary = (
        dim_ubicacion.groupby("camara", dropna=False)
        .agg(
            capacidad_estructural=("es_estructural", "sum"),
            capacidad_operativa_real=("es_operativa", "sum"),
            ubicaciones_es_operativa_1=("es_operativa", "sum"),
            ubicaciones_es_operativa_0=("es_operativa", lambda serie: int((serie == 0).sum())),
        )
        .reset_index()
        .sort_values("camara")
        .reset_index(drop=True)
    )
    return summary
