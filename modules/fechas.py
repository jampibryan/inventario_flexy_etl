from datetime import date, datetime
import re

import pandas as pd


DATE_TOKEN_PATTERN = re.compile(
    r"^(?P<day>\d{1,2})[/-](?P<month>\d{1,2})[/-](?P<year>\d{4})(?P<time>\s+\d{1,2}:\d{2}(?::\d{2})?)?$"
)


def normalize_date_text(value: object) -> str:
    text = str(value).strip()
    if not text or text.upper() in {"NAN", "NAT", "NONE"}:
        return ""

    match = DATE_TOKEN_PATTERN.fullmatch(text)
    if not match:
        return text

    day = match.group("day").zfill(2)
    month = match.group("month").zfill(2)
    year = match.group("year")
    time_part = match.group("time") or ""
    return f"{day}/{month}/{year}{time_part}"


def parse_datetime_value(value: object) -> object:
    if value is None or pd.isna(value):
        return pd.NaT

    if isinstance(value, pd.Timestamp):
        return value
    if isinstance(value, datetime):
        return pd.Timestamp(value)
    if isinstance(value, date):
        return pd.Timestamp(value)

    normalized = normalize_date_text(value)
    if not normalized:
        return pd.NaT

    parsed = pd.to_datetime(normalized, dayfirst=True, errors="coerce")
    return parsed


def parse_datetime_series(series: pd.Series) -> pd.Series:
    return series.apply(parse_datetime_value)
