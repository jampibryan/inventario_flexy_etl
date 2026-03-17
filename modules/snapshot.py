import hashlib

import pandas as pd

from modules.ubicaciones import sanitize_fact_ubicaciones


def build_fact_snapshot(
    df: pd.DataFrame,
    source_file: str,
    valid_ubicacion_keys: set[str],
) -> tuple[pd.DataFrame, dict[str, object]]:
    fact = df.copy()

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

    def obtener_clasificacion(row: pd.Series) -> str | None:
        return row["CLASIFICACI\u00d3N"]

    def obtener_calidad(row: pd.Series) -> str | None:
        prod = str(row["PRODUCTO"]).strip().upper()
        if prod == "MANGO":
            return None
        return "EST\u00c1NDAR"

    fact["VARIEDAD"] = fact.apply(obtener_variedad, axis=1)
    fact["CLASIFICACI\u00d3N"] = fact.apply(obtener_clasificacion, axis=1)
    fact["CALIDAD"] = fact.apply(obtener_calidad, axis=1)
    fact["TIPO DE CORTE"] = pd.Series(pd.NA, index=fact.index, dtype="string")

    fact["FECHA CORTE"] = pd.to_datetime(fact["FECHA CORTE"], errors="coerce")
    fact["fecha_key"] = fact["FECHA CORTE"].dt.strftime("%Y%m%d").astype(int)

    fact["cliente_key"] = fact["CLIENTE"].astype(str).str.strip().str.upper()
    fact["producto_key"] = fact["C\u00d3DIGO"].astype(str).str.strip().str.upper()

    fact["almacen_grupo"] = fact["ALMAC\u00c9N"].astype(str).str.upper()
    fact["tipo_almacen"] = fact["almacen_grupo"].apply(
        lambda x: "INTERNO" if x == "CHAVIN" else "EXTERNO" 
    )

    fact["pallets"] = 1
    fact["source_file"] = source_file
    if "_SOURCE_ROW_NUM" in fact.columns:
        fact["source_row_num"] = pd.to_numeric(fact["_SOURCE_ROW_NUM"], errors="coerce").astype("Int64")
        fact.drop(columns=["_SOURCE_ROW_NUM"], inplace=True)
    else:
        fact["source_row_num"] = range(2, len(fact) + 2)

    fact["snapshot_row_id"] = fact.apply(
        lambda r: hashlib.sha1(
            f"{r['FECHA CORTE'].date()}|{source_file}|{r['source_row_num']}".encode("utf-8")
        ).hexdigest(),
        axis=1,
    )

    fact, ubicacion_audit = sanitize_fact_ubicaciones(fact, valid_ubicacion_keys)
    fact["FECHA CORTE"] = fact["FECHA CORTE"].dt.date

    return fact, ubicacion_audit
