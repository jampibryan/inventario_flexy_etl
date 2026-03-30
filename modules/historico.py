from pathlib import Path

import pandas as pd


FACT_ACTUAL_DATE_COLUMNS = [
    "FECHA CORTE",
    "FECHA FABRICACIÓN",
    "FECHA CADUCIDAD",
]


SNAPSHOT_CONTROL_COLUMNS = [
    "fecha_corte",
    "fecha_key",
    "fact_rows",
    "pallets_logicos",
    "ubicaciones_ocupadas",
    "ubicaciones_distintas",
    "toneladas_total",
    "clientes_distintos",
    "productos_distintos",
    "source_files",
    "audit_rows",
    "conflictos_auditoria",
    "sobrecapacidad_auditoria",
    "descartados_auditoria",
    "partition_integrity_ok",
    "partition_folder_fecha",
    "partition_data_fecha",
    "snapshot_row_id_duplicados",
    "es_ultimo_snapshot_flag",
]


def _empty_snapshot_control() -> pd.DataFrame:
    return pd.DataFrame(columns=SNAPSHOT_CONTROL_COLUMNS)


def _normalize_partition_date(value: object) -> object:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return pd.NaT
    return timestamp.normalize()


def _extract_partition_folder_date(path: Path) -> str:
    folder_name = path.parent.name
    if "=" not in folder_name:
        return ""
    return folder_name.split("=", 1)[1].strip()


def _safe_int_sum(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return 0
    return int(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())


def _safe_float_sum(df: pd.DataFrame, column: str) -> float:
    if column not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())


def _safe_nunique(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return 0
    return int(df[column].dropna().nunique())


def _safe_source_files(df: pd.DataFrame) -> str:
    if "source_file" not in df.columns:
        return ""
    values = sorted({str(value).strip() for value in df["source_file"].dropna() if str(value).strip()})
    return " | ".join(values)


def _coerce_date_object(series: pd.Series) -> pd.Series:
    dt_series = pd.to_datetime(series, errors="coerce")
    return dt_series.dt.date.where(dt_series.notna(), None)


def _coerce_object_columns_to_string(df: pd.DataFrame, excluded_columns: set[str] | None = None) -> pd.DataFrame:
    excluded = excluded_columns or set()
    for column in df.columns:
        if column in excluded:
            continue
        if pd.api.types.is_object_dtype(df[column]) or pd.api.types.is_string_dtype(df[column]):
            df[column] = df[column].astype("string")
            if df[column].dropna().empty and len(df.index) > 0:
                df[column] = pd.Series([""] * len(df.index), index=df.index, dtype="string")
    return df


def build_fact_actual(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        return fact_df.copy()

    fact_actual = fact_df.copy()
    fact_actual["FECHA CORTE"] = pd.to_datetime(fact_actual["FECHA CORTE"], errors="coerce")
    latest_snapshot = fact_actual["FECHA CORTE"].dropna().max()

    if pd.isna(latest_snapshot):
        return fact_actual.iloc[0:0].copy()

    fact_actual = fact_actual[fact_actual["FECHA CORTE"] == latest_snapshot].copy()
    fact_actual["es_ultimo_snapshot_flag"] = 1
    for column in FACT_ACTUAL_DATE_COLUMNS:
        if column in fact_actual.columns:
            fact_actual[column] = _coerce_date_object(fact_actual[column])
    fact_actual = _coerce_object_columns_to_string(
        fact_actual,
        excluded_columns=set(FACT_ACTUAL_DATE_COLUMNS),
    )
    return fact_actual.reset_index(drop=True)


def build_snapshot_control(
    fact_partitioned_dir: Path,
    audit_partitioned_dir: Path | None = None,
) -> pd.DataFrame:
    fact_files = sorted(fact_partitioned_dir.glob("fecha_corte=*/data.parquet"))
    if not fact_files:
        return _empty_snapshot_control()

    rows: list[dict[str, object]] = []

    for fact_file in fact_files:
        folder_fecha = _extract_partition_folder_date(fact_file)
        fact_df = pd.read_parquet(fact_file)

        fechas = (
            pd.to_datetime(fact_df.get("FECHA CORTE"), errors="coerce")
            .dropna()
            .dt.normalize()
            .unique()
        )
        partition_data_fecha = ""
        if len(fechas) == 1:
            partition_data_fecha = pd.Timestamp(fechas[0]).strftime("%Y-%m-%d")
        elif len(fechas) > 1:
            partition_data_fecha = "MULTIPLE"

        snapshot_row_duplicates = 0
        if "snapshot_row_id" in fact_df.columns:
            snapshot_row_duplicates = int(fact_df["snapshot_row_id"].duplicated().sum())

        integrity_ok = int(
            len(fechas) == 1
            and folder_fecha == partition_data_fecha
            and snapshot_row_duplicates == 0
        )

        audit_rows = 0
        conflictos_auditoria = 0
        sobrecapacidad_auditoria = 0
        descartados_auditoria = 0
        if audit_partitioned_dir is not None:
            audit_file = audit_partitioned_dir / f"fecha_corte={folder_fecha}" / "data.parquet"
            if audit_file.exists():
                audit_df = pd.read_parquet(audit_file)
                audit_rows = len(audit_df)
                conflictos_auditoria = _safe_int_sum(audit_df, "conflicto_flag")
                sobrecapacidad_auditoria = _safe_int_sum(audit_df, "sobrecapacidad_flag")
                if "registro_vigente_flag" in audit_df.columns:
                    descartados_auditoria = int(
                        (pd.to_numeric(audit_df["registro_vigente_flag"], errors="coerce").fillna(0) == 0).sum()
                    )

        occupied_mask = pd.Series([True] * len(fact_df))
        if "ubicacion_ocupada_flag" in fact_df.columns:
            occupied_mask = pd.to_numeric(fact_df["ubicacion_ocupada_flag"], errors="coerce").fillna(0).eq(1)

        ubicaciones_distintas = 0
        if "ubicacion_key" in fact_df.columns:
            ubicaciones_distintas = int(fact_df.loc[occupied_mask, "ubicacion_key"].dropna().nunique())

        fecha_corte = partition_data_fecha or folder_fecha
        fecha_key = int(fecha_corte.replace("-", "")) if fecha_corte and fecha_corte != "MULTIPLE" else pd.NA

        rows.append(
            {
                "fecha_corte": fecha_corte,
                "fecha_key": fecha_key,
                "fact_rows": int(len(fact_df)),
                "pallets_logicos": _safe_int_sum(fact_df, "pallets") or int(len(fact_df)),
                "ubicaciones_ocupadas": _safe_int_sum(fact_df, "ubicacion_ocupada_flag"),
                "ubicaciones_distintas": ubicaciones_distintas,
                "toneladas_total": round(_safe_float_sum(fact_df, "TONELADAS"), 2),
                "clientes_distintos": _safe_nunique(fact_df, "cliente_key"),
                "productos_distintos": _safe_nunique(fact_df, "producto_key"),
                "source_files": _safe_source_files(fact_df),
                "audit_rows": int(audit_rows),
                "conflictos_auditoria": int(conflictos_auditoria),
                "sobrecapacidad_auditoria": int(sobrecapacidad_auditoria),
                "descartados_auditoria": int(descartados_auditoria),
                "partition_integrity_ok": integrity_ok,
                "partition_folder_fecha": folder_fecha,
                "partition_data_fecha": partition_data_fecha,
                "snapshot_row_id_duplicados": int(snapshot_row_duplicates),
                "es_ultimo_snapshot_flag": 0,
            }
        )

    summary = pd.DataFrame(rows)
    if summary.empty:
        return _empty_snapshot_control()

    valid_dates = pd.to_datetime(summary["fecha_corte"], errors="coerce")
    if valid_dates.notna().any():
        latest_date = valid_dates.max().strftime("%Y-%m-%d")
        summary.loc[summary["fecha_corte"] == latest_date, "es_ultimo_snapshot_flag"] = 1

    summary["fecha_corte"] = _coerce_date_object(summary["fecha_corte"])
    summary = _coerce_object_columns_to_string(summary, excluded_columns={"fecha_corte"})
    summary["fecha_key"] = summary["fecha_key"].astype("Int64")
    summary = summary.sort_values("fecha_corte").reset_index(drop=True)
    return summary


def summarize_snapshot_control(snapshot_control: pd.DataFrame) -> dict[str, int]:
    if snapshot_control.empty:
        return {
            "snapshots": 0,
            "integrity_ok": 0,
            "integrity_error": 0,
            "latest_rows": 0,
        }

    return {
        "snapshots": int(len(snapshot_control)),
        "integrity_ok": int(snapshot_control["partition_integrity_ok"].sum()),
        "integrity_error": int((snapshot_control["partition_integrity_ok"] == 0).sum()),
        "latest_rows": int(snapshot_control["es_ultimo_snapshot_flag"].sum()),
    }
