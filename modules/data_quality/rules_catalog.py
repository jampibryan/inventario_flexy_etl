import pandas as pd


def validate_sku_catalog_consistency(
    df: pd.DataFrame,
    filename: str,
    historical_df: pd.DataFrame | None = None,
) -> tuple[bool, str]:
    """
    Valida consistencia de catálogo por SKU.

    - Bloquea si un mismo CÓDIGO cambia de PRODUCTO o CLASIFICACIÓN.
    - Advierte si un mismo CÓDIGO solo cambia de PRESENTACIÓN.
    """
    required_columns = ["CÓDIGO", "PRODUCTO", "PRESENTACIÓN", "CLASIFICACIÓN"]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns or df.empty:
        return True, ""

    def _prepare_catalog_frame(source_df: pd.DataFrame) -> pd.DataFrame:
        extra_columns = [
            column
            for column in ["_SOURCE_ROW_NUM", "FECHA CORTE", "source_file"]
            if column in source_df.columns
        ]
        prepared_df = source_df[required_columns + extra_columns].copy()
        for column in required_columns:
            prepared_df[column] = prepared_df[column].fillna("").astype(str).str.strip()

        prepared_df["producto_key"] = prepared_df["CÓDIGO"].str.upper()
        prepared_df = prepared_df[prepared_df["producto_key"].ne("")]
        return prepared_df

    def _build_signatures(source_df: pd.DataFrame) -> pd.DataFrame:
        return (
            source_df.groupby(
                ["producto_key", "PRODUCTO", "PRESENTACIÓN", "CLASIFICACIÓN"],
                dropna=False,
            )
            .size()
            .reset_index(name="ocurrencias")
        )

    def _summarize_distinct_values(signature_df: pd.DataFrame) -> pd.DataFrame:
        if signature_df.empty:
            return pd.DataFrame(
                columns=[
                    "producto_key",
                    "productos_distintos",
                    "clasificaciones_distintas",
                    "presentaciones_distintas",
                ]
            )

        return (
            signature_df.groupby("producto_key", dropna=False)
            .agg(
                productos_distintos=("PRODUCTO", lambda series: series.nunique(dropna=False)),
                clasificaciones_distintas=(
                    "CLASIFICACIÓN",
                    lambda series: series.nunique(dropna=False),
                ),
                presentaciones_distintas=(
                    "PRESENTACIÓN",
                    lambda series: series.nunique(dropna=False),
                ),
            )
            .reset_index()
        )

    current_df = _prepare_catalog_frame(df)
    if current_df.empty:
        return True, ""

    historical_catalog_df = pd.DataFrame()
    if historical_df is not None and not historical_df.empty:
        historical_required = [column for column in required_columns if column in historical_df.columns]
        if len(historical_required) == len(required_columns):
            historical_catalog_df = _prepare_catalog_frame(historical_df)

    current_signatures = _build_signatures(current_df)
    current_keys = set(current_df["producto_key"].dropna().tolist())

    combined_signatures = current_signatures.copy()
    if not historical_catalog_df.empty:
        historical_signatures = _build_signatures(historical_catalog_df)
        historical_signatures = historical_signatures[
            historical_signatures["producto_key"].isin(current_keys)
        ].copy()
        combined_signatures = pd.concat(
            [combined_signatures, historical_signatures],
            ignore_index=True,
        ).drop_duplicates(
            subset=["producto_key", "PRODUCTO", "PRESENTACIÓN", "CLASIFICACIÓN"]
        )

    summary_df = _summarize_distinct_values(combined_signatures)
    if summary_df.empty:
        return True, ""

    blocking_keys = {
        row["producto_key"]
        for _, row in summary_df.iterrows()
        if row["productos_distintos"] > 1 or row["clasificaciones_distintas"] > 1
    }
    warning_keys = {
        row["producto_key"]
        for _, row in summary_df.iterrows()
        if row["presentaciones_distintas"] > 1 and row["producto_key"] not in blocking_keys
    }

    impacted_keys = sorted((blocking_keys | warning_keys) & current_keys)
    if not impacted_keys:
        return True, ""

    detail_lines: list[str] = []
    for producto_key in impacted_keys[:10]:
        severity_label = "bloqueo" if producto_key in blocking_keys else "warning"
        detail_parts: list[str] = [f"tipo={severity_label}"]

        current_rows = current_df[current_df["producto_key"] == producto_key].copy()
        current_rows = (
            current_rows.groupby(
                ["PRODUCTO", "PRESENTACIÓN", "CLASIFICACIÓN"],
                dropna=False,
            )
            .agg(
                ocurrencias=("producto_key", "size"),
                filas_excel=(
                    "_SOURCE_ROW_NUM",
                    lambda series: sorted(
                        {
                            int(value)
                            for value in pd.to_numeric(series, errors="coerce").dropna()
                        }
                    ),
                )
                if "_SOURCE_ROW_NUM" in current_rows.columns
                else ("producto_key", "size"),
            )
            .reset_index()
            .sort_values(
                ["ocurrencias", "PRODUCTO", "PRESENTACIÓN", "CLASIFICACIÓN"],
                ascending=[False, True, True, True],
            )
        )
        current_descriptions: list[str] = []
        for _, row in current_rows.iterrows():
            signature = " | ".join(
                [
                    str(row["PRODUCTO"]).strip() or "<VACIO>",
                    str(row["PRESENTACIÓN"]).strip() or "<VACIO>",
                    str(row["CLASIFICACIÓN"]).strip() or "<VACIO>",
                ]
            )
            if isinstance(row["filas_excel"], list):
                filas_text = ",".join(str(value) for value in row["filas_excel"]) if row["filas_excel"] else "sin_fila"
                current_descriptions.append(f"{signature} [filas_excel={filas_text}]")
            else:
                current_descriptions.append(signature)
        if current_descriptions:
            detail_parts.append("actual: " + " || ".join(current_descriptions))

        if not historical_catalog_df.empty:
            historical_rows = historical_catalog_df[
                historical_catalog_df["producto_key"] == producto_key
            ].copy()
            if not historical_rows.empty:
                historical_rows = (
                    historical_rows.groupby(
                        ["PRODUCTO", "PRESENTACIÓN", "CLASIFICACIÓN"],
                        dropna=False,
                    )
                    .agg(
                        ocurrencias=("producto_key", "size"),
                        fechas=(
                            "FECHA CORTE",
                            lambda series: sorted(
                                {
                                    str(value).strip()
                                    for value in series.dropna()
                                    if str(value).strip()
                                }
                            ),
                        )
                        if "FECHA CORTE" in historical_rows.columns
                        else ("producto_key", "size"),
                    )
                    .reset_index()
                    .sort_values(
                        ["ocurrencias", "PRODUCTO", "PRESENTACIÓN", "CLASIFICACIÓN"],
                        ascending=[False, True, True, True],
                    )
                )
                historical_descriptions: list[str] = []
                for _, row in historical_rows.iterrows():
                    signature = " | ".join(
                        [
                            str(row["PRODUCTO"]).strip() or "<VACIO>",
                            str(row["PRESENTACIÓN"]).strip() or "<VACIO>",
                            str(row["CLASIFICACIÓN"]).strip() or "<VACIO>",
                        ]
                    )
                    if isinstance(row["fechas"], list):
                        fechas_text = ",".join(row["fechas"][-3:]) if row["fechas"] else "sin_fecha"
                        historical_descriptions.append(f"{signature} [fechas={fechas_text}]")
                    else:
                        historical_descriptions.append(signature)
                if historical_descriptions:
                    detail_parts.append("historico: " + " || ".join(historical_descriptions))

        detail_lines.append(f"{producto_key} => " + " | ".join(detail_parts))

    remaining = len(impacted_keys) - len(detail_lines)
    suffix = f" | conflictos_adicionales={remaining}" if remaining > 0 else ""

    if blocking_keys:
        warning_suffix = (
            f" | sku_presentaciones_multiples={len(warning_keys)}"
            if warning_keys
            else ""
        )
        message = (
            f"[VALIDACION_SKU] {filename} | bloqueado | sku_conflictivos={len(blocking_keys)}"
            f"{warning_suffix}. "
            "Un mismo CÓDIGO no puede cambiar de PRODUCTO o CLASIFICACIÓN entre el archivo actual "
            "y el histórico. Si solo cambia la PRESENTACIÓN, se reporta como advertencia pero no se bloquea. "
            f"Detalle: {' ; '.join(detail_lines)}{suffix}"
        )
        return False, message

    message = (
        f"[VALIDACION_SKU] {filename} | advertencia | sku_presentaciones_multiples={len(warning_keys)}. "
        "Un mismo CÓDIGO apareció con múltiples PRESENTACIÓN, pero mantuvo PRODUCTO y CLASIFICACIÓN "
        "consistentes, así que el archivo sí se procesa. "
        f"Detalle: {' ; '.join(detail_lines)}{suffix}"
    )
    return True, message
