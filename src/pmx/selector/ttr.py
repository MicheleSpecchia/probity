from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

TtrBucket = str

BUCKET_0_24H = "0_24h"
BUCKET_1_7D = "1_7d"
BUCKET_7_30D = "7_30d"
BUCKET_30D_PLUS = "30d_plus"
BUCKET_UNKNOWN = "unknown"

ALL_BUCKETS: tuple[TtrBucket, ...] = (
    BUCKET_0_24H,
    BUCKET_1_7D,
    BUCKET_7_30D,
    BUCKET_30D_PLUS,
    BUCKET_UNKNOWN,
)


_ISO_DATE_RE = re.compile(r"\b(20\d{2})[-_/](0[1-9]|1[0-2])[-_/](0[1-9]|[12]\d|3[01])\b")
_MONTH_NAME_RE = re.compile(
    r"\b("
    r"january|february|march|april|may|june|july|august|"
    r"september|october|november|december"
    r")\s+([0-3]?\d),?\s+(20\d{2})\b"
)
_DAY_MONTH_NAME_RE = re.compile(
    r"\b([0-3]?\d)\s+("
    r"january|february|march|april|may|june|july|august|"
    r"september|october|november|december"
    r"),?\s+(20\d{2})\b"
)
_MONTH_NAME_NO_YEAR_RE = re.compile(
    r"\b("
    r"january|february|march|april|may|june|july|august|"
    r"september|october|november|december"
    r")\s+([0-3]?\d)\b"
)
_DAY_MONTH_NAME_NO_YEAR_RE = re.compile(
    r"\b([0-3]?\d)\s+("
    r"january|february|march|april|may|june|july|august|"
    r"september|october|november|december"
    r")\b"
)
_MONTH_TO_INT: dict[str, int] = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_RESOLUTION_KEYS: tuple[str, ...] = (
    "resolution_ts",
    "close_ts",
    "end_ts",
    "resolved_ts",
    "resolution_time",
    "close_time",
    "end_time",
    "resolve_date",
)
_TEXT_KEYS: tuple[str, ...] = (
    "question",
    "title",
    "slug",
    "description",
)
_NESTED_PAYLOAD_KEYS: tuple[str, ...] = (
    "metadata",
    "meta",
    "raw",
    "rule_parse_json",
)


def estimate_resolution_ts(
    market_payload: Mapping[str, Any],
    *,
    decision_ts: datetime | None = None,
) -> datetime | None:
    for key in _RESOLUTION_KEYS:
        parsed = _parse_datetime(market_payload.get(key))
        if parsed is not None:
            return parsed

    for nested_key in _NESTED_PAYLOAD_KEYS:
        nested = market_payload.get(nested_key)
        if not isinstance(nested, Mapping):
            continue
        for key in _RESOLUTION_KEYS:
            parsed = _parse_datetime(nested.get(key))
            if parsed is not None:
                return parsed

    for key in _TEXT_KEYS:
        parsed = _extract_date_from_text(
            market_payload.get(key),
            decision_ts=decision_ts,
        )
        if parsed is not None:
            return parsed

    return None


def estimate_ttr_bucket(market_payload: Mapping[str, Any], decision_ts: datetime) -> TtrBucket:
    decision = _as_utc_datetime(decision_ts)
    resolution_ts = estimate_resolution_ts(market_payload, decision_ts=decision)
    if resolution_ts is None:
        status = _as_text(market_payload.get("status")) or ""
        if status.lower() in {"resolved", "closed", "ended"}:
            return BUCKET_0_24H
        return BUCKET_UNKNOWN

    delta_seconds = (resolution_ts - decision).total_seconds()
    if delta_seconds <= 24 * 3600:
        return BUCKET_0_24H
    if delta_seconds <= 7 * 24 * 3600:
        return BUCKET_1_7D
    if delta_seconds <= 45 * 24 * 3600:
        return BUCKET_7_30D
    return BUCKET_30D_PLUS


def _parse_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc_datetime(raw)
    if isinstance(raw, (int, float)):
        return _parse_unix_timestamp(raw)
    text = _as_text(raw)
    if text is None:
        return None

    numeric = _parse_unix_text(text)
    if numeric is not None:
        return numeric

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return _as_utc_datetime(parsed)


def _parse_unix_timestamp(raw: int | float) -> datetime | None:
    value = float(raw)
    if value <= 0:
        return None
    if value > 10_000_000_000:
        value = value / 1000.0
    try:
        parsed = datetime.fromtimestamp(value, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None
    return _as_utc_datetime(parsed)


def _parse_unix_text(text: str) -> datetime | None:
    normalized = text.strip()
    if not normalized:
        return None
    if not re.fullmatch(r"-?\d+(\.\d+)?", normalized):
        return None
    try:
        numeric = float(normalized)
    except ValueError:
        return None
    return _parse_unix_timestamp(numeric)


def _extract_date_from_text(
    raw: Any,
    *,
    decision_ts: datetime | None = None,
) -> datetime | None:
    text = _as_text(raw)
    if text is None:
        return None
    lowered = text.lower()

    iso_match = _ISO_DATE_RE.search(lowered)
    if iso_match is not None:
        year = int(iso_match.group(1))
        month = int(iso_match.group(2))
        day = int(iso_match.group(3))
        return _build_date(year, month, day)

    month_match = _MONTH_NAME_RE.search(lowered)
    if month_match is not None:
        month_opt = _MONTH_TO_INT.get(month_match.group(1))
        day = int(month_match.group(2))
        year = int(month_match.group(3))
        if month_opt is not None:
            month = month_opt
            return _build_date(year, month, day)

    day_month_match = _DAY_MONTH_NAME_RE.search(lowered)
    if day_month_match is not None:
        day = int(day_month_match.group(1))
        month_opt = _MONTH_TO_INT.get(day_month_match.group(2))
        year = int(day_month_match.group(3))
        if month_opt is not None:
            month = month_opt
            return _build_date(year, month, day)

    if decision_ts is None:
        return None

    month_name_no_year = _MONTH_NAME_NO_YEAR_RE.search(lowered)
    if month_name_no_year is not None:
        month_opt = _MONTH_TO_INT.get(month_name_no_year.group(1))
        day = int(month_name_no_year.group(2))
        if month_opt is not None:
            month = month_opt
            return _build_month_day_without_year(
                decision_ts=decision_ts,
                month=month,
                day=day,
            )

    day_month_no_year = _DAY_MONTH_NAME_NO_YEAR_RE.search(lowered)
    if day_month_no_year is not None:
        day = int(day_month_no_year.group(1))
        month_opt = _MONTH_TO_INT.get(day_month_no_year.group(2))
        if month_opt is not None:
            month = month_opt
            return _build_month_day_without_year(
                decision_ts=decision_ts,
                month=month,
                day=day,
            )

    return None


def _build_date(year: int, month: int, day: int) -> datetime | None:
    try:
        return datetime(year, month, day, tzinfo=UTC)
    except ValueError:
        return None


def _build_month_day_without_year(
    *,
    decision_ts: datetime,
    month: int,
    day: int,
) -> datetime | None:
    decision = _as_utc_datetime(decision_ts)
    candidate = _build_date(decision.year, month, day)
    if candidate is None:
        return None
    if candidate < decision:
        candidate = _build_date(decision.year + 1, month, day)
        if candidate is None:
            return None
    max_delta_seconds = 400 * 24 * 3600
    if (candidate - decision).total_seconds() > max_delta_seconds:
        return None
    return candidate


def _as_text(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
