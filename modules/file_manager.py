from pathlib import Path


def get_original_excel_files(original_dir: Path) -> list[Path]:
    """
    Devuelve todos los archivos .xlsx en la carpeta ORIGINAL, ordenados.
    Ignora archivos temporales de Excel (~$).
    """
    return sorted(
        f for f in original_dir.glob("*.xlsx")
        if not f.name.startswith("~$")
    )