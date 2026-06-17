import pandas as pd

from modules.data_quality.models import DataQualityIssue, DataQualityResult
from modules.data_quality.rules_catalog import validate_sku_catalog_consistency
from modules.data_quality.rules_input import (
    extract_date_from_data,
    validate_expected_columns,
    validate_location_structure,
    validate_no_negatives,
)


def run_input_quality_checks(df_raw: pd.DataFrame, filename: str) -> DataQualityResult:
    result = DataQualityResult(dataframe=df_raw)

    negatives_ok, negatives_message, validated_df = validate_no_negatives(df_raw, filename)
    result.dataframe = validated_df if negatives_ok else df_raw
    if negatives_message:
        result.issues.append(
            DataQualityIssue(
                rule_id="NO_NEGATIVES",
                status_code="WARN_NEGATIVOS_COMPENSADOS" if negatives_ok else "ERROR_NEGATIVOS",
                severity="warning" if negatives_ok else "error",
                message=negatives_message,
            )
        )
    if not negatives_ok:
        return result

    location_ok, location_message = validate_location_structure(validated_df, filename)
    if location_message:
        result.issues.append(
            DataQualityIssue(
                rule_id="LOCATION_STRUCTURE",
                status_code="WARN_UBICACION" if location_ok else "ERROR_UBICACION",
                severity="warning" if location_ok else "error",
                message=location_message,
            )
        )

    return result


def run_file_metadata_checks(df_raw: pd.DataFrame, filename: str = "") -> DataQualityResult:
    result = DataQualityResult(dataframe=df_raw)

    columns_ok, columns_message = validate_expected_columns(df_raw)
    if not columns_ok:
        result.issues.append(
            DataQualityIssue(
                rule_id="REQUIRED_COLUMNS",
                status_code="ERROR_COLUMNAS",
                severity="error",
                message=columns_message,
            )
        )
        return result

    date_ok, file_date, date_error = extract_date_from_data(df_raw, filename)
    if not date_ok:
        result.issues.append(
            DataQualityIssue(
                rule_id="FILE_DATE_PRESENT",
                status_code="ERROR_FECHA",
                severity="error",
                message=date_error,
            )
        )
        return result

    result.file_date = file_date
    return result


def run_transformed_quality_checks(
    df_clean: pd.DataFrame,
    filename: str,
    historical_df: pd.DataFrame | None = None,
    catalog_lookup: dict | None = None,
) -> DataQualityResult:
    result = DataQualityResult(dataframe=df_clean)

    # Validar consistencia SKU
    sku_ok, sku_message = validate_sku_catalog_consistency(
        df_clean,
        filename,
        historical_df=historical_df,
    )
    if sku_message:
        result.issues.append(
            DataQualityIssue(
                rule_id="SKU_CATALOG_CONSISTENCY",
                status_code="ERROR_SKU_CATALOGO",
                severity="warning" if sku_ok else "error",
                message=sku_message,
            )
        )

    # Validar existencia en Lista Maestra de Productos (Opcion 2: warning)
    if catalog_lookup is not None:
        unique_skus = set(df_clean["CÓDIGO"].dropna().astype(str).str.strip().str.upper().unique())
        missing_skus = [sku for sku in unique_skus if sku != "SIN_SKU" and sku not in catalog_lookup]
        if missing_skus:
            skus_str = ", ".join(sorted(missing_skus))
            msg = (
                f"[VALIDACION_LISTA_MAESTRA] {filename} | advertencia | "
                f"Se encontraron {len(missing_skus)} SKU(s) que no existen en la Lista Maestra de Productos: {skus_str}. "
                "Se procesarán con clasificación genérica ('OTROS' y peso por defecto)."
            )
            result.issues.append(
                DataQualityIssue(
                    rule_id="MASTER_CATALOG_SKU_CHECK",
                    status_code="WARN_SKU_NO_CATALOGADO",
                    severity="warning",
                    message=msg,
                )
            )

    return result
