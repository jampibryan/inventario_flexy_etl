from pathlib import Path
from modules.utils import is_valid_original_filename, extract_date_from_filename


def get_original_excel_files(original_dir: Path) -> list[Path]:
    """
    Devuelve todos los archivos .xlsx en la carpeta ORIGINAL, ordenados.
    Ignora archivos temporales de Excel (~$).
    """
    return sorted(
        f for f in original_dir.glob("*.xlsx")
        if not f.name.startswith("~$")
    )


def validate_original_file(file_path: Path) -> tuple[bool, str]:
    """
    Valida el nombre del archivo original.
    """
    if not is_valid_original_filename(file_path.name):
        return False, "Nombre inválido. Debe ser yyyy-mm-dd.xlsx"
    return True, ""


def get_file_date(file_path: Path) -> str:
    """
    Obtiene la fecha yyyy-mm-dd desde el nombre del archivo.
    """
    return extract_date_from_filename(file_path.name)