from pathlib import Path
import unicodedata

import pandas as pd

from config import EXPECTED_COLUMNS
from modules.dimensiones import CAPACITY_CONFIG
from modules.ubicaciones import RECEPCION_LABEL, normalize_camara


def _normalize_text(value: str) -> str:
    return unicodedata.normalize("NFC", str(value))


def _normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_text(col) for col in df.columns]
    return df


def _build_capacity_lookup() -> dict[str, dict[str, int | str]]:
    return {cfg["camara"]: cfg for cfg in CAPACITY_CONFIG}


CAPACITY_LOOKUP = _build_capacity_lookup()
STRUCTURAL_CAMERAS = set(CAPACITY_LOOKUP)


def read_excel_file(file_path: Path) -> pd.DataFrame:
    """
    Lee el archivo Excel original.
    """
    df = pd.read_excel(file_path)
    df = _normalize_dataframe_columns(df)
    df["_source_row_num"] = df.index + 2
    return df


def validate_expected_columns(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Valida que el Excel tenga todas las columnas esperadas.
    """
    df = _normalize_dataframe_columns(df)
    expected_columns = [_normalize_text(col) for col in EXPECTED_COLUMNS]
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        return False, f"Faltan columnas: {', '.join(missing)}"
    return True, ""


def extract_date_from_data(df: pd.DataFrame) -> tuple[bool, str, str]:
    """
    Extrae la fecha desde la columna 'Fecha Actualizacion' del DataFrame.
    Toma la primera fecha valida encontrada.
    Retorna: (exito, fecha_str_yyyy_mm_dd, mensaje_error)
    """
    df = _normalize_dataframe_columns(df)

    if "Fecha Actualizaci\u00f3n" not in df.columns:
        return False, "", "No se encontro la columna 'Fecha Actualizacion'"

    fechas = pd.to_datetime(df["Fecha Actualizaci\u00f3n"], errors="coerce").dropna()

    if fechas.empty:
        return False, "", "No se encontraron fechas validas en 'Fecha Actualizacion'"

    primera_fecha = fechas.iloc[0]
    fecha_str = primera_fecha.strftime("%Y-%m-%d")
    return True, fecha_str, ""


def _prepare_numeric_validation_frame(df: pd.DataFrame) -> pd.DataFrame:
    df_check = _normalize_dataframe_columns(df)

    df_check["Cantidad"] = (
        df_check["Cantidad"]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)
        .str.replace("\xa0", "", regex=False)
    )
    df_check["Cantidad"] = pd.to_numeric(df_check["Cantidad"], errors="coerce")
    df_check["Presentaci\u00f3n"] = pd.to_numeric(df_check["Presentaci\u00f3n"], errors="coerce")

    ubic_split = df_check["Ubicaci\u00f3n"].astype(str).str.split(",", expand=True)
    if 1 in ubic_split.columns:
        df_check["Rack"] = pd.to_numeric(ubic_split[1].str.strip(), errors="coerce")
    if 2 in ubic_split.columns:
        df_check["Nivel"] = pd.to_numeric(ubic_split[2].str.strip(), errors="coerce")
    if 3 in ubic_split.columns:
        df_check["Posici\u00f3n"] = pd.to_numeric(ubic_split[3].str.strip(), errors="coerce")

    return df_check


def _format_pair_row_detail(row: pd.Series, idx: int) -> str:
    fila_excel = idx + 2
    codigo = row.get("C\u00f3digo", "?")
    producto = row.get("Producto", "?")
    cantidad = row.get("Cantidad")
    presentacion = row.get("Presentaci\u00f3n")
    ubicacion = str(row.get("Ubicaci\u00f3n", "")).strip()
    return (
        f"     - Fila {fila_excel} | Cantidad = {cantidad} | Presentacion = {presentacion} "
        f"| Ubicacion: {ubicacion} | Codigo: {codigo} | Producto: {producto}"
    )


def _normalize_compensation_location(value: object) -> str:
    return " ".join(str(value).strip().upper().split())


def _find_compensated_negative_pairs(df_check: pd.DataFrame) -> tuple[set[int], list[str]]:
    drop_indices: set[int] = set()
    detail_lines: list[str] = []
    tolerance = 1e-9

    for pos in range(1, len(df_check)):
        prev_idx = df_check.index[pos - 1]
        curr_idx = df_check.index[pos]

        if prev_idx in drop_indices or curr_idx in drop_indices:
            continue

        prev = df_check.loc[prev_idx]
        curr = df_check.loc[curr_idx]

        prev_cantidad = prev.get("Cantidad")
        curr_cantidad = curr.get("Cantidad")
        prev_presentacion = prev.get("Presentaci\u00f3n")
        curr_presentacion = curr.get("Presentaci\u00f3n")
        prev_ubicacion = _normalize_compensation_location(prev.get("Ubicaci\u00f3n", ""))
        curr_ubicacion = _normalize_compensation_location(curr.get("Ubicaci\u00f3n", ""))

        if not (
            pd.notna(prev_cantidad)
            and pd.notna(curr_cantidad)
            and pd.notna(prev_presentacion)
            and pd.notna(curr_presentacion)
            and prev_ubicacion
            and curr_ubicacion
            and prev_ubicacion == curr_ubicacion
            and curr_cantidad < 0
            and curr_presentacion < 0
            and abs(prev_cantidad + curr_cantidad) <= tolerance
            and abs(prev_presentacion + curr_presentacion) <= tolerance
        ):
            continue

        drop_indices.update({prev_idx, curr_idx})
        detail_lines.append(
            "     -> Se eliminan 2 filas consecutivas porque se compensan entre si:"
        )
        detail_lines.append(_format_pair_row_detail(prev, prev_idx))
        detail_lines.append(_format_pair_row_detail(curr, curr_idx))
        detail_lines.append(
            "        Suma del par: "
            f"Cantidad = {prev_cantidad + curr_cantidad}, "
            f"Presentacion = {prev_presentacion + curr_presentacion}"
        )

    return drop_indices, detail_lines


def validate_no_negatives(df: pd.DataFrame, filename: str) -> tuple[bool, str, pd.DataFrame]:
    """
    Valida que no haya valores negativos en columnas numericas.
    Revisa: Cantidad, Presentacion, y Rack/Nivel/Posicion (desde Ubicacion).
    Si detecta pares consecutivos cuya suma en Cantidad y Presentacion da 0,
    elimina ambas filas y permite continuar. Si quedan negativos no resueltos,
    el archivo se bloquea.
    """
    warnings = []
    detail_lines = []
    df_check = _prepare_numeric_validation_frame(df)
    drop_indices, compensated_detail_lines = _find_compensated_negative_pairs(df_check)

    df_filtered = df.drop(index=list(drop_indices)).copy() if drop_indices else df.copy()
    df_filtered_check = _prepare_numeric_validation_frame(df_filtered)

    columns_to_check = ["Cantidad", "Presentaci\u00f3n", "Rack", "Nivel", "Posici\u00f3n"]
    total_negativos = 0

    for col in columns_to_check:
        if col not in df_filtered_check.columns:
            continue
        neg_mask = df_filtered_check[col] < 0
        if neg_mask.any():
            count = int(neg_mask.sum())
            total_negativos += count
            warnings.append(f"  - {col}: {count} valor(es) negativo(s)")

            neg_data = df_filtered_check[neg_mask]
            for idx, row in neg_data.iterrows():
                fila_excel = idx + 2
                codigo = row.get("C\u00f3digo", "?")
                producto = row.get("Producto", "?")
                valor = row[col]
                detail_lines.append(
                    f"     -> Fila {fila_excel} | {col} = {valor} | Codigo: {codigo} | Producto: {producto}"
                )

    if warnings:
        sep = "=" * 70
        header = (
            f"\n{sep}\n"
            f"  ARCHIVO BLOQUEADO: {filename}\n"
            f"  Total valores negativos encontrados: {total_negativos}\n"
            f"{sep}"
        )
        resumen = "\n".join(warnings)
        detalle_header = "\n  DETALLE POR FILA:"
        detalle = "\n".join(detail_lines)
        compensadas = ""
        if compensated_detail_lines:
            compensadas = (
                "\n\n  FILAS ELIMINADAS AUTOMATICAMENTE POR COMPENSACION:\n"
                + "\n".join(compensated_detail_lines)
            )
        footer = f"\n  Corrige estos valores en el Excel original y vuelve a ejecutar.\n{sep}"
        msg = f"{header}\n{resumen}\n{detalle_header}\n{detalle}{compensadas}\n{footer}"
        return False, msg, df

    if compensated_detail_lines:
        sep = "=" * 70
        msg = (
            f"\n{sep}\n"
            f"  ARCHIVO AJUSTADO: {filename}\n"
            f"  Se eliminaron {len(drop_indices)} fila(s) porque formaban pares consecutivos\n"
            f"  en la misma Ubicacion y cuya suma en Cantidad y Presentacion era 0.\n"
            f"{sep}\n"
            "  DETALLE DE FILAS ELIMINADAS:\n"
            f"{chr(10).join(compensated_detail_lines)}\n"
            f"{sep}"
        )
        return True, msg, df_filtered

    return True, "", df_filtered


def _prepare_location_structure_frame(df: pd.DataFrame) -> pd.DataFrame:
    df_check = _normalize_dataframe_columns(df).copy()

    ubicacion = df_check["Ubicaci\u00f3n"].fillna("").astype(str)
    ubic_split = ubicacion.str.split(",", expand=True)

    df_check["camara_raw"] = ubic_split[0].fillna("").astype(str).str.strip() if 0 in ubic_split.columns else ""
    df_check["rack_raw"] = ubic_split[1].fillna("").astype(str).str.strip() if 1 in ubic_split.columns else ""
    df_check["nivel_raw"] = ubic_split[2].fillna("").astype(str).str.strip() if 2 in ubic_split.columns else ""
    df_check["posicion_raw"] = ubic_split[3].fillna("").astype(str).str.strip() if 3 in ubic_split.columns else ""

    df_check["camara_normalizada"] = df_check["camara_raw"].apply(normalize_camara)
    df_check["rack_num"] = pd.to_numeric(df_check["rack_raw"], errors="coerce")
    df_check["nivel_num"] = pd.to_numeric(df_check["nivel_raw"], errors="coerce")
    df_check["posicion_num"] = pd.to_numeric(df_check["posicion_raw"], errors="coerce")
    df_check["ubicacion_componentes"] = ubic_split.shape[1]
    df_check["almacen_upper"] = df_check["Almac\u00e9n"].fillna("").astype(str).str.upper()
    df_check["es_chavin"] = df_check["almacen_upper"].str.contains("CHAVIN", na=False)
    df_check["componentes_no_vacios"] = df_check[["camara_raw", "rack_raw", "nivel_raw", "posicion_raw"]].apply(
        lambda row: sum(bool(str(value).strip()) for value in row),
        axis=1,
    )

    extra_cols = [col for col in ubic_split.columns if col > 3]
    if extra_cols:
        df_check["componentes_extra"] = ubic_split[extra_cols].fillna("").astype(str).apply(
            lambda row: [value.strip() for value in row if value.strip()],
            axis=1,
        )
    else:
        df_check["componentes_extra"] = [[] for _ in range(len(df_check))]

    return df_check


def _format_location_issue(row: pd.Series, motivo: str) -> str:
    fila_excel = int(row.get("_source_row_num", row.name + 2))
    codigo = row.get("C\u00f3digo", "?")
    producto = row.get("Producto", "?")
    ubicacion = row.get("Ubicaci\u00f3n", "")
    camara = row.get("camara_normalizada", "")
    rack = row.get("rack_num")
    nivel = row.get("nivel_num")
    posicion = row.get("posicion_num")

    return (
        f"     -> Fila {fila_excel} | Ubicacion = {ubicacion} | Camara = {camara or '[VACIA]'} "
        f"| Rack = {rack} | Nivel = {nivel} | Posicion = {posicion} "
        f"| Codigo: {codigo} | Producto: {producto} | Error: {motivo}"
    )


def _find_non_numeric_components(row: pd.Series) -> list[str]:
    invalid_components: list[str] = []

    for label, raw_col, num_col in [
        ("rack", "rack_raw", "rack_num"),
        ("nivel", "nivel_raw", "nivel_num"),
        ("posicion", "posicion_raw", "posicion_num"),
    ]:
        raw_value = str(row.get(raw_col, "")).strip()
        num_value = row.get(num_col)
        if raw_value and pd.isna(num_value):
            invalid_components.append(label)

    return invalid_components


def validate_location_structure(df: pd.DataFrame, filename: str) -> tuple[bool, str]:
    """
    Valida que las ubicaciones del Excel respeten la estructura fisica definida
    en CAPACITY_CONFIG. Si encuentra una ubicacion interna fuera de rango o con
    formato inconsistente, bloquea el archivo.
    """
    df_check = _prepare_location_structure_frame(df)
    issues: list[str] = []

    for _, row in df_check.iterrows():
        if not bool(row.get("es_chavin")):
            continue

        camara = row.get("camara_normalizada", "")
        camara_raw = row.get("camara_raw", "")
        rack_raw = str(row.get("rack_raw", "")).strip()
        nivel_raw = str(row.get("nivel_raw", "")).strip()
        posicion_raw = str(row.get("posicion_raw", "")).strip()
        rack = row.get("rack_num")
        nivel = row.get("nivel_num")
        posicion = row.get("posicion_num")
        extra_components = row.get("componentes_extra", [])
        non_numeric_components = _find_non_numeric_components(row)

        if extra_components:
            issues.append(
                _format_location_issue(
                    row,
                    f"ubicacion tiene componentes extra no permitidos: {extra_components}",
                )
            )
            continue

        if not camara_raw:
            if any([rack_raw, nivel_raw, posicion_raw]):
                issues.append(
                    _format_location_issue(
                        row,
                        "ubicacion parcial: hay coordenadas pero falta la camara",
                    )
                )
            continue

        if camara == RECEPCION_LABEL:
            if any([rack_raw, nivel_raw, posicion_raw]):
                issues.append(
                    _format_location_issue(
                        row,
                        "RECEPCION no debe tener rack, nivel ni posicion",
                    )
                )
            continue

        if camara not in STRUCTURAL_CAMERAS:
            issues.append(
                _format_location_issue(
                    row,
                    "camara interna no existe en CAPACITY_CONFIG",
                )
            )
            continue

        if row.get("componentes_no_vacios", 0) != 4:
            issues.append(
                _format_location_issue(
                    row,
                    "ubicacion estructural debe tener exactamente 4 componentes: camara,rack,nivel,posicion",
                )
            )
            continue

        if non_numeric_components:
            issues.append(
                _format_location_issue(
                    row,
                    f"componentes no numericos en: {', '.join(non_numeric_components)}",
                )
            )
            continue

        if any(pd.isna(value) for value in [rack, nivel, posicion]):
            issues.append(
                _format_location_issue(
                    row,
                    "ubicacion estructural incompleta: faltan rack, nivel o posicion",
                )
            )
            continue

        cfg = CAPACITY_LOOKUP[camara]
        ranges = [
            ("rack", int(rack), 1, int(cfg["racks"])),
            ("nivel", int(nivel), 1, int(cfg["niveles"])),
            ("posicion", int(posicion), 1, int(cfg["posiciones"])),
        ]

        for label, value, min_value, max_value in ranges:
            if value < min_value or value > max_value:
                issues.append(
                    _format_location_issue(
                        row,
                        f"{label} fuera de rango para {camara}: permitido {min_value}..{max_value}",
                    )
                )
                break

    if not issues:
        return True, ""

    sep = "=" * 90
    msg = (
        f"\n{sep}\n"
        f"  ARCHIVO BLOQUEADO: {filename}\n"
        f"  Total inconsistencias de estructura de ubicacion: {len(issues)}\n"
        f"  Regla: las ubicaciones internas deben respetar CAPACITY_CONFIG.\n"
        f"{sep}\n"
        "  DETALLE POR FILA:\n"
        f"{chr(10).join(issues)}\n"
        f"{sep}\n"
        "  Corrige estas ubicaciones en el Excel original y vuelve a ejecutar.\n"
        f"{sep}"
    )
    return False, msg
