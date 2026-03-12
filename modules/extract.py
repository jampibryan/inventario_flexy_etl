from pathlib import Path
import pandas as pd
from config import EXPECTED_COLUMNS


def read_excel_file(file_path: Path) -> pd.DataFrame:
    """
    Lee el archivo Excel original.
    """
    df = pd.read_excel(file_path)
    return df


def validate_expected_columns(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Valida que el Excel tenga todas las columnas esperadas.
    """
    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        return False, f"Faltan columnas: {', '.join(missing)}"
    return True, ""


def extract_date_from_data(df: pd.DataFrame) -> tuple[bool, str, str]:
    """
    Extrae la fecha desde la columna 'Fecha Actualización' del DataFrame.
    Toma la primera fecha válida encontrada.
    Retorna: (exito, fecha_str_yyyy_mm_dd, mensaje_error)
    """
    if "Fecha Actualización" not in df.columns:
        return False, "", "No se encontró la columna 'Fecha Actualización'"

    fechas = pd.to_datetime(df["Fecha Actualización"], errors="coerce").dropna()

    if fechas.empty:
        return False, "", "No se encontraron fechas válidas en 'Fecha Actualización'"

    primera_fecha = fechas.iloc[0]
    fecha_str = primera_fecha.strftime("%Y-%m-%d")
    return True, fecha_str, ""


def validate_no_negatives(df: pd.DataFrame, filename: str) -> tuple[bool, str]:
    """
    Valida que no haya valores negativos en columnas numéricas.
    Revisa: Cantidad, Presentación, y Rack/Nivel/Posición (desde Ubicación).
    Retorna (True, "") si todo OK, o (False, mensaje_detallado) si hay negativos.
    """
    warnings = []
    detail_lines = []
    df_check = df.copy()

    # Limpiar y convertir Cantidad
    df_check["Cantidad"] = (
        df_check["Cantidad"]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)
        .str.replace("\xa0", "", regex=False)
    )
    df_check["Cantidad"] = pd.to_numeric(df_check["Cantidad"], errors="coerce")

    # Convertir Presentación
    df_check["Presentación"] = pd.to_numeric(df_check["Presentación"], errors="coerce")

    # Dividir Ubicación para obtener Rack, Nivel, Posición
    ubic_split = df_check["Ubicación"].astype(str).str.split(",", expand=True)
    if 1 in ubic_split.columns:
        df_check["Rack"] = pd.to_numeric(ubic_split[1].str.strip(), errors="coerce")
    if 2 in ubic_split.columns:
        df_check["Nivel"] = pd.to_numeric(ubic_split[2].str.strip(), errors="coerce")
    if 3 in ubic_split.columns:
        df_check["Posición"] = pd.to_numeric(ubic_split[3].str.strip(), errors="coerce")

    # Revisar cada columna numérica
    columns_to_check = ["Cantidad", "Presentación", "Rack", "Nivel", "Posición"]
    total_negativos = 0

    for col in columns_to_check:
        if col not in df_check.columns:
            continue
        neg_mask = df_check[col] < 0
        if neg_mask.any():
            count = neg_mask.sum()
            total_negativos += count
            warnings.append(f"  ⚠ {col}: {count} valor(es) negativo(s)")

            # Detalle fila por fila
            neg_data = df_check[neg_mask]
            for idx, row in neg_data.iterrows():
                fila_excel = idx + 2
                codigo = row.get("Código", "?")
                producto = row.get("Producto", "?")
                valor = row[col]
                detail_lines.append(
                    f"     → Fila {fila_excel}  |  {col} = {valor}  |  Código: {codigo}  |  Producto: {producto}"
                )

    if warnings:
        sep = "=" * 70
        header = (
            f"\n{sep}\n"
            f"  ❌ ARCHIVO BLOQUEADO: {filename}\n"
            f"  📊 Total valores negativos encontrados: {total_negativos}\n"
            f"{sep}"
        )
        resumen = "\n".join(warnings)
        detalle_header = "\n  📋 DETALLE POR FILA:"
        detalle = "\n".join(detail_lines)
        footer = f"\n  💡 Corrige estos valores en el Excel original y vuelve a ejecutar.\n{sep}"
        msg = f"{header}\n{resumen}\n{detalle_header}\n{detalle}\n{footer}"
        return False, msg

    return True, ""