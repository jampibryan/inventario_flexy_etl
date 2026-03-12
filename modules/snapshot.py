import hashlib
import pandas as pd


def build_ubicacion_key(row: pd.Series) -> str | None:
    camara = str(row.get("CÁMARA", "")).strip().upper()
    rack = row.get("RACK")
    nivel = row.get("NIVEL")
    posicion = row.get("POSICIÓN")

    if (
        camara.startswith("CÁMARA")
        and pd.notna(rack)
        and pd.notna(nivel)
        and pd.notna(posicion)
    ):
        cam_num = camara.replace("CÁMARA", "").strip().zfill(2)
        return f"CAM{cam_num}-R{int(rack):03d}-N{int(nivel):02d}-P{int(posicion):02d}"

    return None


def build_fact_snapshot(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    fact = df.copy()

    fact["FECHA CORTE"] = pd.to_datetime(fact["FECHA CORTE"], errors="coerce")
    fact["fecha_key"] = fact["FECHA CORTE"].dt.strftime("%Y%m%d").astype("string")

    fact["cliente_key"] = fact["CLIENTE"].astype(str).str.strip().str.upper()
    fact["producto_key"] = fact["CÓDIGO"].astype(str).str.strip().str.upper()
    fact["ubicacion_key"] = fact.apply(build_ubicacion_key, axis=1)

    fact["almacen_grupo"] = fact["ALMACÉN"].astype(str).str.upper().apply(
        lambda x: "CHAVIN" if x == "CHAVIN" else "EXTERNOS"
    )

    fact["tipo_ubicacion"] = fact.apply(
        lambda r: (
            "POSICION" if pd.notna(r["ubicacion_key"])
            else "RECEPCION" if str(r["CÁMARA"]).strip().upper() == "RECEPCIÓN"
            else "EXTERNO" if str(r["ALMACÉN"]).strip().upper() != "CHAVIN"
            else "SIN_UBICACION"
        ),
        axis=1,
    )

    fact["pallets"] = 1
    fact["source_file"] = source_file
    fact["source_row_num"] = range(2, len(fact) + 2)

    fact["snapshot_row_id"] = fact.apply(
        lambda r: hashlib.sha1(
            f"{r['FECHA CORTE'].date()}|{source_file}|{r['source_row_num']}".encode("utf-8")
        ).hexdigest(),
        axis=1,
    )

    return fact