from pathlib import Path
import os
import pandas as pd


def save_daily_outputs(df: pd.DataFrame, excel_path: Path, csv_path: Path) -> None:
    """
    Guarda las salidas diarias en Excel y CSV.
    """
    df.to_excel(excel_path, index=False, engine="xlsxwriter")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def rebuild_historical(csv_dir: Path, historico_temp: Path, historico_final: Path) -> None:
    """
    Reconstruye el histórico uniendo todos los CSV diarios limpios.
    Usa archivo temporal y luego reemplazo seguro.
    """
    daily_csvs = sorted([
        f for f in csv_dir.glob("inventario_*.csv")
        if f.name not in {"inventario_historico.csv", "inventario_historico_temp.csv"}
    ])

    if not daily_csvs:
        return

    dfs = []
    for csv_file in daily_csvs:
        df = pd.read_csv(csv_file, dtype={"CÓDIGO": str, "LOTE": str, "CANTIDAD CAJAS": str}, low_memory=False)
        dfs.append(df)

    historico_df = pd.concat(dfs, ignore_index=True)
    historico_df = historico_df.drop_duplicates()

    historico_df.to_csv(historico_temp, index=False, encoding="utf-8-sig")

    os.replace(historico_temp, historico_final)