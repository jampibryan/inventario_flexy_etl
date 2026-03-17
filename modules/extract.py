from pathlib import Path
import unicodedata

import pandas as pd

from config import EXPECTED_COLUMNS


def _normalize_text(value: str) -> str:
    return unicodedata.normalize("NFC", str(value))


def _normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_text(col) for col in df.columns]
    return df


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

    if "Fecha Actualización" not in df.columns:
        return False, "", "No se encontro la columna 'Fecha Actualizacion'"

    fechas = pd.to_datetime(df["Fecha Actualización"], errors="coerce").dropna()

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
    df_check["Presentación"] = pd.to_numeric(df_check["Presentación"], errors="coerce")

    ubic_split = df_check["Ubicación"].astype(str).str.split(",", expand=True)
    if 1 in ubic_split.columns:
        df_check["Rack"] = pd.to_numeric(ubic_split[1].str.strip(), errors="coerce")
    if 2 in ubic_split.columns:
        df_check["Nivel"] = pd.to_numeric(ubic_split[2].str.strip(), errors="coerce")
    if 3 in ubic_split.columns:
        df_check["Posición"] = pd.to_numeric(ubic_split[3].str.strip(), errors="coerce")

    return df_check


def _format_pair_row_detail(row: pd.Series, idx: int) -> str:
    fila_excel = idx + 2
    codigo = row.get("Código", "?")
    producto = row.get("Producto", "?")
    cantidad = row.get("Cantidad")
    presentacion = row.get("Presentación")
    return (
        f"     - Fila {fila_excel} | Cantidad = {cantidad} | Presentacion = {presentacion} "
        f"| Codigo: {codigo} | Producto: {producto}"
    )


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
        prev_presentacion = prev.get("Presentación")
        curr_presentacion = curr.get("Presentación")

        if not (
            pd.notna(prev_cantidad)
            and pd.notna(curr_cantidad)
            and pd.notna(prev_presentacion)
            and pd.notna(curr_presentacion)
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

    columns_to_check = ["Cantidad", "Presentación", "Rack", "Nivel", "Posición"]
    total_negativos = 0

    for col in columns_to_check:
        if col not in df_filtered_check.columns:
            continue
        neg_mask = df_filtered_check[col] < 0
        if neg_mask.any():
            count = neg_mask.sum()
            total_negativos += count
            warnings.append(f"  - {col}: {count} valor(es) negativo(s)")

            neg_data = df_filtered_check[neg_mask]
            for idx, row in neg_data.iterrows():
                fila_excel = idx + 2
                codigo = row.get("Código", "?")
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
            f"  cuya suma en Cantidad y Presentacion era 0.\n"
            f"{sep}\n"
            "  DETALLE DE FILAS ELIMINADAS:\n"
            f"{chr(10).join(compensated_detail_lines)}\n"
            f"{sep}"
        )
        return True, msg, df_filtered

    return True, "", df_filtered
