import hashlib
import re
from typing import Any

import pandas as pd

from config import (
    BOX_CAPACITY_RULES,
    MULTIPALLET_COMPATIBILITY_RULES,
    OCCUPANCY_RULES,
    PALLET_IDENTITY_FIELDS,
)
from modules.ubicaciones import (
    POSICION_LABEL,
    build_ubicacion_key,
    normalize_camara,
    resolve_almacen_control_reference,
    resolve_tipo_ubicacion,
    strip_accents,
)


CANONICAL_COLUMN_MAP = {
    "ALMACEN": "ALMACEN",
    "ALMACN": "ALMACEN",
    "CAMARA": "CAMARA",
    "CMARA": "CAMARA",
    "POSICION": "POSICION",
    "POSICIN": "POSICION",
    "CODIGO": "CODIGO",
    "CDIGO": "CODIGO",
    "PRESENTACION": "PRESENTACION",
    "PRESENTACIN": "PRESENTACION",
    "FECHA FABRICACION": "FECHA FABRICACION",
    "FECHA FABRICACIN": "FECHA FABRICACION",
    "CLASIFICACION": "CLASIFICACION",
    "CLASIFICACIN": "CLASIFICACION",
}


def _normalize_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().upper().split())


def _normalize_code(value: Any) -> str:
    return _normalize_text(value)


def _normalize_date_token(value: Any) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _row_get(row: pd.Series, *aliases: str) -> Any:
    for alias in aliases:
        if alias in row.index:
            return row.get(alias)
    return None


def _build_identity_key(row: pd.Series) -> str:
    tokens: list[str] = []

    for field in PALLET_IDENTITY_FIELDS:
        field_normalized = str(field).upper()
        if field_normalized in {"CODIGO", "CÓDIGO"}:
            tokens.append(_normalize_code(_row_get(row, "CODIGO", "CÓDIGO")))
        elif "FECHA" in field_normalized:
            if "FABRIC" in field_normalized:
                tokens.append(_normalize_date_token(_row_get(row, "FECHA FABRICACION", "FECHA FABRICACIÓN")))
            elif "CADUC" in field_normalized:
                tokens.append(_normalize_date_token(_row_get(row, "FECHA CADUCIDAD")))
            else:
                tokens.append(_normalize_date_token(row.get(field)))
        else:
            tokens.append(_normalize_text(row.get(field)))

    return "|".join(tokens)


def _match_rule(row: pd.Series, rule: dict[str, Any]) -> bool:
    codigo = _normalize_code(_row_get(row, "CODIGO", "CÓDIGO"))
    producto = _normalize_text(_row_get(row, "PRODUCTO"))
    presentacion = _normalize_text(_row_get(row, "PRESENTACION", "PRESENTACIÓN"))

    if rule.get("codigo") and codigo != _normalize_code(rule["codigo"]):
        return False
    if rule.get("producto") and producto != _normalize_text(rule["producto"]):
        return False
    if rule.get("presentacion_contains"):
        token = _normalize_text(rule["presentacion_contains"])
        if token not in presentacion:
            return False
    return True


def resolve_box_capacity_rule(row: pd.Series) -> tuple[int, str]:
    default_boxes = int(OCCUPANCY_RULES["default_max_boxes_per_location"])

    for idx, rule in enumerate(BOX_CAPACITY_RULES, start=1):
        if _match_rule(row, rule):
            return int(rule["max_boxes"]), str(rule.get("rule_name", f"RULE_{idx:02d}"))

    return default_boxes, "DEFAULT"


def _match_multipallet_rule(candidate: dict[str, Any], rule: dict[str, Any]) -> bool:
    base_row = candidate["base_row"]
    codigo = _normalize_code(base_row.get("CODIGO"))
    producto = _normalize_text(base_row.get("PRODUCTO"))
    camara = normalize_camara(base_row.get("CAMARA"))
    almacen = _normalize_text(base_row.get("ALMACEN"))

    if rule.get("almacen") and almacen != _normalize_text(rule["almacen"]):
        return False
    if rule.get("camaras"):
        valid_camaras = {normalize_camara(value) for value in rule["camaras"]}
        if camara not in valid_camaras:
            return False
    if rule.get("codigos"):
        valid_codigos = {_normalize_code(value) for value in rule["codigos"]}
        if codigo not in valid_codigos:
            return False
    if rule.get("productos"):
        valid_productos = {_normalize_text(value) for value in rule["productos"]}
        if producto not in valid_productos:
            return False
    return True


def resolve_multipallet_rule(
    candidates: list[dict[str, Any]],
    boxes_total: int,
    capacity_limit: int,
) -> tuple[bool, str, str]:
    if len(candidates) <= 1:
        return True, "SINGLE_PALLET", "Una sola unidad logica en la ubicacion."

    distinct_products = {
        _normalize_code(candidate["base_row"].get("CODIGO")) for candidate in candidates
    }

    for rule in MULTIPALLET_COMPATIBILITY_RULES:
        if not all(_match_multipallet_rule(candidate, rule) for candidate in candidates):
            continue

        max_logical_pallets = int(rule.get("max_logical_pallets", OCCUPANCY_RULES["default_max_logical_pallets_per_location"]))
        max_total_boxes = int(rule.get("max_total_boxes", capacity_limit))
        max_boxes_per_pallet = int(rule.get("max_boxes_per_pallet", max_total_boxes))
        min_boxes_per_pallet = int(rule.get("min_boxes_per_pallet", 1))
        require_distinct_products = bool(rule.get("require_distinct_products", False))

        if len(candidates) > max_logical_pallets:
            continue
        if boxes_total > max_total_boxes:
            continue
        if require_distinct_products and len(distinct_products) != len(candidates):
            continue

        pallet_boxes = [int(candidate["base_row"]["CANTIDAD CAJAS"]) for candidate in candidates]
        if any(boxes < min_boxes_per_pallet or boxes > max_boxes_per_pallet for boxes in pallet_boxes):
            continue

        return True, str(rule["rule_name"]), "Ubicacion valida segun regla de compatibilidad multipallet."

    return False, "SIN_REGLA_COMPATIBILIDAD", "No existe una regla multipallet que valide la coexistencia en esta ubicacion."


def _derive_location_capacity(candidates: list[dict[str, Any]]) -> int:
    if not candidates:
        return int(OCCUPANCY_RULES["default_max_boxes_per_location"])

    strategy = str(OCCUPANCY_RULES.get("location_capacity_strategy", "min")).lower()
    values = [int(candidate["base_row"]["max_cajas_permitidas_pallet"]) for candidate in candidates]

    if strategy == "max":
        return max(values)
    if strategy == "sum":
        return sum(values)
    return min(values)


def _sort_rows_for_recency(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["_fecha_fabricacion_sort"] = pd.to_datetime(work["FECHA FABRICACION"], errors="coerce")
    work["_fecha_caducidad_sort"] = pd.to_datetime(work["FECHA CADUCIDAD"], errors="coerce")
    work["_source_row_num_sort"] = pd.to_numeric(work["source_row_num"], errors="coerce")
    return work.sort_values(
        by=["_fecha_fabricacion_sort", "_fecha_caducidad_sort", "_source_row_num_sort"],
        ascending=[False, False, False],
        na_position="last",
    )


def _candidate_recency_key(candidate: dict[str, Any]) -> tuple[int, int, int]:
    fecha_fabricacion = pd.to_datetime(candidate["base_row"].get("FECHA FABRICACION"), errors="coerce")
    fecha_caducidad = pd.to_datetime(candidate["base_row"].get("FECHA CADUCIDAD"), errors="coerce")
    source_row_num = pd.to_numeric(candidate["base_row"].get("source_row_num"), errors="coerce")

    fecha_fabricacion_value = int(fecha_fabricacion.value) if pd.notna(fecha_fabricacion) else -1
    fecha_caducidad_value = int(fecha_caducidad.value) if pd.notna(fecha_caducidad) else -1
    source_row_num_value = int(source_row_num) if pd.notna(source_row_num) else -1
    return (fecha_fabricacion_value, fecha_caducidad_value, source_row_num_value)


def _build_candidate_payload(
    candidate_rows: pd.DataFrame,
    source_file: str,
    ubicacion_key: str,
) -> dict[str, Any]:
    sorted_rows = _sort_rows_for_recency(candidate_rows)
    representative = sorted_rows.iloc[0].copy()
    source_row_nums = (
        pd.to_numeric(sorted_rows["source_row_num"], errors="coerce")
        .dropna()
        .astype(int)
        .tolist()
    )
    max_boxes, rule_name = resolve_box_capacity_rule(representative)

    base = representative.to_dict()
    base["CANTIDAD CAJAS"] = int(pd.to_numeric(candidate_rows["CANTIDAD CAJAS"], errors="coerce").fillna(0).sum())
    base["TONELADAS"] = round(pd.to_numeric(candidate_rows["TONELADAS"], errors="coerce").fillna(0).sum(), 2)
    base["source_row_num"] = min(source_row_nums) if source_row_nums else pd.NA
    base["source_row_nums"] = ",".join(str(num) for num in source_row_nums)
    base["cantidad_registros_fuente"] = len(source_row_nums)
    base["pallet_consolidado_flag"] = 1 if len(source_row_nums) > 1 else 0
    base["max_cajas_permitidas_pallet"] = int(max_boxes)
    base["regla_capacidad_pallet"] = rule_name
    base["identity_key"] = _build_identity_key(representative)
    base["ubicacion_key_candidata"] = ubicacion_key
    base["candidate_signature"] = hashlib.sha1(
        f"{base.get('FECHA CORTE')}|{source_file}|{ubicacion_key}|{base['identity_key']}".encode("utf-8")
    ).hexdigest()
    return {
        "base_row": base,
        "rows": candidate_rows.copy(),
    }


def _build_conflict_overcapacity_audit_rows(
    winner: dict[str, Any],
    pallets_logicos_ubicacion: int,
    cajas_totales_ubicacion: int,
    reglas_capacidad_ubicacion: str,
    regla_compatibilidad_ubicacion: str,
) -> list[dict[str, Any]]:
    return _build_audit_rows(
        candidate=winner,
        pallet_logico_id=None,
        tipo_registro="ERROR_SOBRECAPACIDAD_CONFLICTO",
        registro_vigente_flag=0,
        conflicto_flag=1,
        sobrecapacidad_flag=1,
        detalle_resolucion=(
            "El registro mas reciente fue identificado como vigente para resolver el conflicto, "
            "pero aun asi excede la capacidad maxima permitida en cajas."
        ),
        pallets_logicos_ubicacion=pallets_logicos_ubicacion,
        cajas_totales_ubicacion=cajas_totales_ubicacion,
        max_cajas_permitidas_ubicacion=int(winner["base_row"]["max_cajas_permitidas_pallet"]),
        reglas_capacidad_ubicacion=reglas_capacidad_ubicacion,
        regla_compatibilidad_ubicacion=regla_compatibilidad_ubicacion,
    )


def _build_clean_row(
    candidate: dict[str, Any],
    source_file: str,
    tipo_registro: str,
    ubicacion_ocupada_flag: int,
    conflicto_flag: int,
    sobrecapacidad_flag: int,
    pallets_logicos_ubicacion: int,
    cajas_totales_ubicacion: int,
    max_cajas_permitidas_ubicacion: int,
    reglas_capacidad_ubicacion: str,
    regla_compatibilidad_ubicacion: str,
) -> dict[str, Any]:
    clean_row = dict(candidate["base_row"])
    clean_row["tipo_registro_resuelto"] = tipo_registro
    clean_row["pallet_logico_id"] = hashlib.sha1(
        f"{clean_row.get('FECHA CORTE')}|{source_file}|{candidate['base_row']['candidate_signature']}".encode("utf-8")
    ).hexdigest()
    clean_row["ubicacion_ocupada_flag"] = int(ubicacion_ocupada_flag)
    clean_row["conflicto_flag"] = int(conflicto_flag)
    clean_row["sobrecapacidad_flag"] = int(sobrecapacidad_flag)
    clean_row["registro_vigente_flag"] = 1
    clean_row["multipallet_flag"] = 1 if pallets_logicos_ubicacion > 1 else 0
    clean_row["pallets_logicos_ubicacion"] = int(pallets_logicos_ubicacion)
    clean_row["cajas_totales_ubicacion"] = int(cajas_totales_ubicacion)
    clean_row["max_cajas_permitidas_ubicacion"] = int(max_cajas_permitidas_ubicacion)
    clean_row["regla_capacidad_ubicacion"] = reglas_capacidad_ubicacion
    clean_row["regla_compatibilidad_ubicacion"] = regla_compatibilidad_ubicacion
    clean_row["source_file_resolucion"] = source_file
    return clean_row


def _build_audit_rows(
    candidate: dict[str, Any],
    pallet_logico_id: str | None,
    tipo_registro: str,
    registro_vigente_flag: int,
    conflicto_flag: int,
    sobrecapacidad_flag: int,
    detalle_resolucion: str,
    pallets_logicos_ubicacion: int,
    cajas_totales_ubicacion: int,
    max_cajas_permitidas_ubicacion: int,
    reglas_capacidad_ubicacion: str,
    regla_compatibilidad_ubicacion: str,
) -> list[dict[str, Any]]:
    audit_rows: list[dict[str, Any]] = []

    for _, row in candidate["rows"].iterrows():
        audit_row = row.to_dict()
        audit_row["pallet_logico_id"] = pallet_logico_id
        audit_row["tipo_registro_resuelto"] = tipo_registro
        audit_row["registro_vigente_flag"] = int(registro_vigente_flag)
        audit_row["conflicto_flag"] = int(conflicto_flag)
        audit_row["sobrecapacidad_flag"] = int(sobrecapacidad_flag)
        audit_row["detalle_resolucion"] = detalle_resolucion
        audit_row["pallet_consolidado_flag"] = int(candidate["base_row"]["pallet_consolidado_flag"])
        audit_row["cantidad_registros_fuente"] = int(candidate["base_row"]["cantidad_registros_fuente"])
        audit_row["source_row_nums"] = candidate["base_row"]["source_row_nums"]
        audit_row["max_cajas_permitidas_pallet"] = int(candidate["base_row"]["max_cajas_permitidas_pallet"])
        audit_row["regla_capacidad_pallet"] = candidate["base_row"]["regla_capacidad_pallet"]
        audit_row["pallets_logicos_ubicacion"] = int(pallets_logicos_ubicacion)
        audit_row["cajas_totales_ubicacion"] = int(cajas_totales_ubicacion)
        audit_row["max_cajas_permitidas_ubicacion"] = int(max_cajas_permitidas_ubicacion)
        audit_row["regla_capacidad_ubicacion"] = reglas_capacidad_ubicacion
        audit_row["regla_compatibilidad_ubicacion"] = regla_compatibilidad_ubicacion
        audit_rows.append(audit_row)

    return audit_rows


def _prepare_workframe(
    df: pd.DataFrame,
    valid_ubicacion_keys: set[str],
) -> pd.DataFrame:
    work = df.copy()
    rename_map: dict[str, str] = {}

    for column in work.columns:
        normalized = strip_accents(str(column)).upper()
        normalized = re.sub(r"[^A-Z0-9 ]+", "", normalized)
        normalized = " ".join(normalized.split())
        rename_target = CANONICAL_COLUMN_MAP.get(normalized)
        if rename_target:
            rename_map[column] = rename_target

    work.rename(columns=rename_map, inplace=True)

    if "_SOURCE_ROW_NUM" in work.columns:
        work["source_row_num"] = pd.to_numeric(work["_SOURCE_ROW_NUM"], errors="coerce").astype("Int64")
        work.drop(columns=["_SOURCE_ROW_NUM"], inplace=True)
    elif "source_row_num" not in work.columns:
        work["source_row_num"] = pd.Series(range(2, len(work) + 2), dtype="Int64")

    work["ubicacion_key_candidata"] = work.apply(
        lambda row: build_ubicacion_key(
            row.get("CAMARA"),
            row.get("RACK"),
            row.get("NIVEL"),
            row.get("POSICION"),
        ),
        axis=1,
    )
    work["tipo_ubicacion_preliminar"] = work.apply(
        lambda row: resolve_tipo_ubicacion(
            resolve_almacen_control_reference(row),
            row.get("CAMARA"),
            row.get("ubicacion_key_candidata"),
            valid_ubicacion_keys,
        ),
        axis=1,
    )
    work["identity_key"] = work.apply(_build_identity_key, axis=1)
    return work


def _build_passthrough_rows(
    row: pd.Series,
    source_file: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    base_row = row.to_dict()
    cantidad_cajas = pd.to_numeric(base_row.get("CANTIDAD CAJAS"), errors="coerce")
    pallet_logico_id = hashlib.sha1(
        f"{base_row.get('FECHA CORTE')}|{source_file}|{base_row.get('source_row_num')}".encode("utf-8")
    ).hexdigest()
    tipo_ubicacion = str(base_row.get("tipo_ubicacion_preliminar", "SIN_UBICACION"))
    tipo_registro = f"PASSTHROUGH_{tipo_ubicacion}"

    clean_row = dict(base_row)
    clean_row["pallet_logico_id"] = pallet_logico_id
    clean_row["tipo_registro_resuelto"] = tipo_registro
    clean_row["ubicacion_ocupada_flag"] = 0
    clean_row["conflicto_flag"] = 0
    clean_row["sobrecapacidad_flag"] = 0
    clean_row["registro_vigente_flag"] = 1
    clean_row["pallet_consolidado_flag"] = 0
    clean_row["cantidad_registros_fuente"] = 1
    clean_row["source_row_nums"] = str(base_row.get("source_row_num"))
    clean_row["max_cajas_permitidas_pallet"] = pd.NA
    clean_row["regla_capacidad_pallet"] = pd.NA
    clean_row["pallets_logicos_ubicacion"] = 1
    clean_row["cajas_totales_ubicacion"] = int(cantidad_cajas) if pd.notna(cantidad_cajas) else 0
    clean_row["max_cajas_permitidas_ubicacion"] = pd.NA
    clean_row["regla_capacidad_ubicacion"] = pd.NA
    clean_row["regla_compatibilidad_ubicacion"] = pd.NA
    clean_row["multipallet_flag"] = 0
    clean_row["source_file_resolucion"] = source_file

    audit_row = dict(clean_row)
    audit_row["detalle_resolucion"] = "Registro fuera de una ubicacion estructural POSICION; se conserva sin consolidacion."
    return clean_row, audit_row


def resolve_location_occupancy(
    df: pd.DataFrame,
    source_file: str,
    valid_ubicacion_keys: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    work = _prepare_workframe(df, valid_ubicacion_keys)

    clean_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    summary = {
        "source_rows": int(len(work)),
        "logical_pallets": 0,
        "consolidated_pallets": 0,
        "occupied_locations": 0,
        "multipallet_locations": 0,
        "conflict_locations": 0,
        "discarded_rows": 0,
        "overcapacity_locations": 0,
        "audit_rows": 0,
    }

    passthrough_mask = work["tipo_ubicacion_preliminar"] != POSICION_LABEL
    for _, row in work[passthrough_mask].iterrows():
        clean_row, audit_row = _build_passthrough_rows(row, source_file)
        clean_rows.append(clean_row)
        audit_rows.append(audit_row)

    position_rows = work[~passthrough_mask].copy()
    max_logical_pallets = int(OCCUPANCY_RULES["default_max_logical_pallets_per_location"])
    allow_mixed_products = bool(OCCUPANCY_RULES.get("allow_mixed_products_within_location", True))

    for ubicacion_key, location_rows in position_rows.groupby("ubicacion_key_candidata", dropna=False):
        if pd.isna(ubicacion_key):
            for _, row in location_rows.iterrows():
                clean_row, audit_row = _build_passthrough_rows(row, source_file)
                clean_rows.append(clean_row)
                audit_rows.append(audit_row)
            continue

        candidates: list[dict[str, Any]] = []
        for _, candidate_rows in location_rows.groupby("identity_key", dropna=False):
            candidates.append(_build_candidate_payload(candidate_rows, source_file, str(ubicacion_key)))

        boxes_total = int(sum(candidate["base_row"]["CANTIDAD CAJAS"] for candidate in candidates))
        capacity_limit = _derive_location_capacity(candidates)
        rules_used = sorted({str(candidate["base_row"]["regla_capacidad_pallet"]) for candidate in candidates})
        rules_used_str = ",".join(rules_used)
        logical_pallets_in_location = len(candidates)
        mixed_products = len({_normalize_code(candidate["base_row"].get("CODIGO")) for candidate in candidates}) > 1
        compatibility_allowed, compatibility_rule_name, compatibility_reason = resolve_multipallet_rule(
            candidates,
            boxes_total,
            capacity_limit,
        )
        conflict_by_mix = mixed_products and not (allow_mixed_products or compatibility_allowed)
        conflict_by_count = logical_pallets_in_location > max_logical_pallets
        if conflict_by_count or conflict_by_mix:
            summary["conflict_locations"] += 1
            candidates_sorted = sorted(
                candidates,
                key=lambda candidate: candidate["base_row"]["candidate_signature"],
            )
            winner = max(candidates_sorted, key=_candidate_recency_key)
            winner_clean_row = _build_clean_row(
                candidate=winner,
                source_file=source_file,
                tipo_registro="CONFLICTO_RESUELTO_MAS_RECIENTE",
                ubicacion_ocupada_flag=1,
                conflicto_flag=1,
                sobrecapacidad_flag=0,
                pallets_logicos_ubicacion=1,
                cajas_totales_ubicacion=boxes_total,
                max_cajas_permitidas_ubicacion=capacity_limit,
                reglas_capacidad_ubicacion=rules_used_str,
                regla_compatibilidad_ubicacion=compatibility_rule_name,
            )
            winner_boxes = int(winner["base_row"]["CANTIDAD CAJAS"])
            winner_capacity_limit = int(winner["base_row"]["max_cajas_permitidas_pallet"])

            if winner_boxes > winner_capacity_limit:
                summary["overcapacity_locations"] += 1
                summary["discarded_rows"] += int(len(winner["rows"]))
                audit_rows.extend(
                    _build_conflict_overcapacity_audit_rows(
                        winner=winner,
                        pallets_logicos_ubicacion=logical_pallets_in_location,
                        cajas_totales_ubicacion=boxes_total,
                        reglas_capacidad_ubicacion=rules_used_str,
                        regla_compatibilidad_ubicacion=compatibility_rule_name,
                    )
                )
            else:
                clean_rows.append(winner_clean_row)
                audit_rows.extend(
                    _build_audit_rows(
                        candidate=winner,
                        pallet_logico_id=winner_clean_row["pallet_logico_id"],
                        tipo_registro="CONFLICTO_RESUELTO_MAS_RECIENTE",
                        registro_vigente_flag=1,
                        conflicto_flag=1,
                        sobrecapacidad_flag=0,
                        detalle_resolucion=(
                            "Se conserva el pallet logico mas reciente segun FECHA FABRICACION; "
                            f"los demas se descartan por conflicto. Motivo: {compatibility_reason}"
                        ),
                        pallets_logicos_ubicacion=logical_pallets_in_location,
                        cajas_totales_ubicacion=boxes_total,
                        max_cajas_permitidas_ubicacion=winner_capacity_limit,
                        reglas_capacidad_ubicacion=rules_used_str,
                        regla_compatibilidad_ubicacion=compatibility_rule_name,
                    )
                )

            for candidate in candidates:
                if candidate is winner:
                    continue
                summary["discarded_rows"] += int(len(candidate["rows"]))
                audit_rows.extend(
                    _build_audit_rows(
                        candidate=candidate,
                        pallet_logico_id=None,
                        tipo_registro="DESCARTADO_CONFLICTO",
                        registro_vigente_flag=0,
                        conflicto_flag=1,
                        sobrecapacidad_flag=0,
                        detalle_resolucion=(
                            "Registro descartado porque coexistia con un pallet mas reciente en la misma ubicacion. "
                            f"Motivo: {compatibility_reason}"
                        ),
                        pallets_logicos_ubicacion=logical_pallets_in_location,
                        cajas_totales_ubicacion=boxes_total,
                        max_cajas_permitidas_ubicacion=capacity_limit,
                        reglas_capacidad_ubicacion=rules_used_str,
                        regla_compatibilidad_ubicacion=compatibility_rule_name,
                    )
                )
            continue

        overcapacity = boxes_total > capacity_limit

        if overcapacity:
            summary["overcapacity_locations"] += 1
            summary["discarded_rows"] += int(len(location_rows))
            for candidate in candidates:
                audit_rows.extend(
                    _build_audit_rows(
                        candidate=candidate,
                        pallet_logico_id=None,
                        tipo_registro="ERROR_SOBRECAPACIDAD",
                        registro_vigente_flag=0,
                        conflicto_flag=0,
                        sobrecapacidad_flag=1,
                        detalle_resolucion="Ubicacion descartada por superar la capacidad maxima configurada en cajas.",
                        pallets_logicos_ubicacion=logical_pallets_in_location,
                        cajas_totales_ubicacion=boxes_total,
                        max_cajas_permitidas_ubicacion=capacity_limit,
                        reglas_capacidad_ubicacion=rules_used_str,
                        regla_compatibilidad_ubicacion=compatibility_rule_name,
                    )
                )
            continue

        if logical_pallets_in_location > 1:
            summary["multipallet_locations"] += 1

        for idx, candidate in enumerate(
            sorted(candidates, key=lambda item: item["base_row"]["candidate_signature"])
        ):
            if logical_pallets_in_location == 1:
                tipo_registro = (
                    "PALLET_LOGICO_CONSOLIDADO"
                    if candidate["base_row"]["pallet_consolidado_flag"] == 1
                    else "PALLET_LOGICO_DIRECTO"
                )
            else:
                tipo_registro = (
                    "MULTIPALLET_VALIDO_CONSOLIDADO"
                    if candidate["base_row"]["pallet_consolidado_flag"] == 1
                    else "MULTIPALLET_VALIDO"
                )

            clean_row = _build_clean_row(
                candidate=candidate,
                source_file=source_file,
                tipo_registro=tipo_registro,
                ubicacion_ocupada_flag=1 if idx == 0 else 0,
                conflicto_flag=0,
                sobrecapacidad_flag=0,
                pallets_logicos_ubicacion=logical_pallets_in_location,
                cajas_totales_ubicacion=boxes_total,
                max_cajas_permitidas_ubicacion=capacity_limit,
                reglas_capacidad_ubicacion=rules_used_str,
                regla_compatibilidad_ubicacion=compatibility_rule_name,
            )
            clean_rows.append(clean_row)
            audit_rows.extend(
                _build_audit_rows(
                    candidate=candidate,
                    pallet_logico_id=clean_row["pallet_logico_id"],
                    tipo_registro=tipo_registro,
                    registro_vigente_flag=1,
                    conflicto_flag=0,
                    sobrecapacidad_flag=0,
                    detalle_resolucion="Registro vigente despues de resolver la ocupacion por ubicacion.",
                    pallets_logicos_ubicacion=logical_pallets_in_location,
                    cajas_totales_ubicacion=boxes_total,
                    max_cajas_permitidas_ubicacion=capacity_limit,
                    reglas_capacidad_ubicacion=rules_used_str,
                    regla_compatibilidad_ubicacion=compatibility_rule_name,
                )
            )

    clean_df = pd.DataFrame(clean_rows)
    audit_df = pd.DataFrame(audit_rows)

    summary["logical_pallets"] = int(len(clean_df))
    if not clean_df.empty and "pallet_consolidado_flag" in clean_df.columns:
        summary["consolidated_pallets"] = int(
            pd.to_numeric(clean_df["pallet_consolidado_flag"], errors="coerce").fillna(0).sum()
        )
    if not clean_df.empty and "ubicacion_ocupada_flag" in clean_df.columns:
        summary["occupied_locations"] = int(
            pd.to_numeric(clean_df["ubicacion_ocupada_flag"], errors="coerce").fillna(0).sum()
        )
    summary["audit_rows"] = int(len(audit_df))

    reverse_rename_map = {
        "CODIGO": "CÓDIGO",
        "PRESENTACION": "PRESENTACIÓN",
        "FECHA FABRICACION": "FECHA FABRICACIÓN",
        "CLASIFICACION": "CLASIFICACIÓN",
        "POSICION": "POSICIÓN",
        "CAMARA": "CÁMARA",
        "ALMACEN": "ALMACÉN",
    }
    clean_df.rename(columns=reverse_rename_map, inplace=True)
    audit_df.rename(columns=reverse_rename_map, inplace=True)

    return clean_df, audit_df, summary
