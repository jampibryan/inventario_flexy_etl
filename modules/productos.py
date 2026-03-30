import re
from typing import Any

import pandas as pd


PRODUCT_KEYWORDS = (
    ("MANGO", "MANGO"),
    ("PALTA", "PALTA"),
    ("FRESA", "FRESA"),
    ("PI\u00d1A", "PI\u00d1A"),
    ("MARACUYA", "MARACUYA"),
    ("GRANADA", "GRANADA"),
)

VARIEDADES_FIJAS = {
    "FRESA": "SABRINA",
    "PALTA": "HASS",
    "GRANADA": "WONDERFUL",
    "MARACUYA": "CRIOLLA",
    "PI\u00d1A": "GOLDEN",
}

KG_PATTERNS = (
    r"\bX\s*(\d+(?:\.\d+)?)\s*KG\b",
    r"\bCJ\s*X\s*(\d+(?:\.\d+)?)\s*KG\b",
    r"\bCAJA\s*X\s*(\d+(?:\.\d+)?)\s*KG\b",
    r"\bCAJA\s*(\d+(?:\.\d+)?)\s*KG\b",
    r"\bCJ\s*(\d+(?:\.\d+)?)\s*KG\b",
)


def _normalize_upper_text(value: Any) -> str:
    return str(value).strip().upper()


def clasificar_almacen(almacen_original: str) -> str:
    almacen_upper = _normalize_upper_text(almacen_original)

    if "CHAVIN" in almacen_upper:
        return "CHAVIN"
    if "ACUAPESCA" in almacen_upper:
        return "ACUAPESCA"
    if "EMERGENT" in almacen_upper:
        return "EMERGENT COLD"

    return str(almacen_original).strip()


def clasificar_estado_producto(almacen_original: str) -> str:
    almacen_upper = _normalize_upper_text(almacen_original)

    if "CHAVIN" in almacen_upper and "REEMPAQUE" in almacen_upper:
        return "REEMPAQUE"

    return "DISPONIBLE"


def clasificar_producto(texto_producto: str) -> str:
    producto_upper = _normalize_upper_text(texto_producto)

    for token, producto in PRODUCT_KEYWORDS:
        if token in producto_upper:
            return producto

    return "OTROS"


def clasificar_clasificacion(texto_producto: str) -> str:
    producto_upper = _normalize_upper_text(texto_producto)
    if "ORGANICO" in producto_upper or "ORG\u00c1NICO" in producto_upper:
        return "ORG\u00c1NICO"
    return "CONVENCIONAL"


def limpiar_presentacion(texto_producto: str, producto: str) -> str:
    resultado = str(texto_producto).strip()
    resultado = re.sub(r"(?i)^" + re.escape(producto) + r"\s*", "", resultado)
    resultado = re.sub(r"(?i)\bORG[A\u00c1]NICO\b", "", resultado)
    resultado = re.sub(r"(?i)\bCONVENCIONAL\b", "", resultado)
    return re.sub(r"\s+", " ", resultado).strip()


def obtener_variedad(producto: str, presentacion: str) -> str | None:
    producto_upper = _normalize_upper_text(producto)
    presentacion_upper = _normalize_upper_text(presentacion)

    if producto_upper == "MANGO":
        if "EDWARD" in presentacion_upper:
            return "EDWARD"
        if "KENT" in presentacion_upper:
            return "KENT"
        return "OTROS"

    return VARIEDADES_FIJAS.get(producto_upper)


def obtener_calidad(producto: str) -> str | None:
    if _normalize_upper_text(producto) == "MANGO":
        return None
    return "EST\u00c1NDAR"


def extraer_kg_por_caja(texto_producto: str) -> float | None:
    texto = _normalize_upper_text(texto_producto).replace(",", ".")

    for pattern in KG_PATTERNS:
        match = re.search(pattern, texto)
        if match:
            return float(match.group(1))

    return None


def resolver_presentacion_kg(
    texto_producto: str,
    presentacion_raw: float | int | None,
    cantidad_cajas: float | int | None,
) -> float | None:
    kg_por_caja = extraer_kg_por_caja(texto_producto)
    if kg_por_caja is not None:
        return kg_por_caja

    if pd.isna(presentacion_raw):
        return None

    presentacion_value = float(presentacion_raw)
    cantidad_value = pd.to_numeric(cantidad_cajas, errors="coerce")

    if pd.notna(cantidad_value) and float(cantidad_value) > 0 and presentacion_value > 100:
        return round(presentacion_value / float(cantidad_value), 4)

    return presentacion_value
