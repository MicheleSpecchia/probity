from __future__ import annotations

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


def estimate_ttr_bucket(market_payload: dict[str, Any], decision_ts: datetime) -> TtrBucket:
    decision = _as_utc_datetime(decision_ts)
    resolution_ts = _parse_datetime(market_payload.get("resolution_ts"))
    if resolution_ts is None:
        resolution_ts = _parse_datetime(market_payload.get("resolved_ts"))
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
    if delta_seconds <= 30 * 24 * 3600:
        return BUCKET_7_30D
    return BUCKET_30D_PLUS


def _parse_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc_datetime(raw)
    text = _as_text(raw)
    if text is None:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return _as_utc_datetime(parsed)


def _as_text(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
