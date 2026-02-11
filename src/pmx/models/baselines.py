from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

DEFAULT_BASELINE_B_INTERCEPT = 0.0
DEFAULT_BASELINE_B_WEIGHTS: dict[str, float] = {
    "mid_price_centered": 2.0,
    "book_imbalance_1": 1.2,
    "return_5m": 1.5,
    "realized_vol_1h": -0.6,
    "spread_bps_scaled": -0.8,
    "trade_count_5m_scaled": 0.7,
    "volume_5m_scaled": 0.4,
    "stale_trade_scaled": 0.5,
    "stale_book_scaled": 0.3,
}


def baseline_a_price(price_prob: float) -> float:
    """Baseline A: identity over as-of market probability."""
    return _clamp(float(price_prob), 0.0, 1.0)


def baseline_b_micro(
    features: Mapping[str, Any],
    *,
    intercept: float = DEFAULT_BASELINE_B_INTERCEPT,
    weights: Mapping[str, float] | None = None,
) -> float:
    """Baseline B: deterministic microstructure logistic scoring."""
    weight_map = dict(DEFAULT_BASELINE_B_WEIGHTS if weights is None else weights)
    transformed = _transform_features(features)
    score = float(intercept)
    for key in sorted(weight_map.keys()):
        score += float(weight_map[key]) * transformed.get(key, 0.0)
    return _sigmoid(score)


def _transform_features(features: Mapping[str, Any]) -> dict[str, float]:
    mid_price = _clamp(_as_float(features.get("mid_price"), default=0.5), 0.0, 1.0)
    spread_bps = _clamp(_as_float(features.get("spread_bps"), default=0.0), 0.0, 2000.0)
    book_imbalance = _clamp(_as_float(features.get("book_imbalance_1"), default=0.0), -1.0, 1.0)
    return_5m = _clamp(_as_float(features.get("return_5m"), default=0.0), -0.5, 0.5)
    realized_vol = _clamp(_as_float(features.get("realized_vol_1h"), default=0.0), 0.0, 2.0)
    trade_count_5m = _clamp(_as_float(features.get("trade_count_5m"), default=0.0), 0.0, 100.0)
    volume_5m = _clamp(_as_float(features.get("volume_5m"), default=0.0), 0.0, 1_000_000.0)
    stale_trade = _clamp(
        _as_float(features.get("stale_seconds_last_trade"), default=3600.0),
        0.0,
        14_400.0,
    )
    stale_book = _clamp(
        _as_float(features.get("stale_seconds_last_book"), default=3600.0),
        0.0,
        14_400.0,
    )

    return {
        "mid_price_centered": mid_price - 0.5,
        "book_imbalance_1": book_imbalance,
        "return_5m": return_5m,
        "realized_vol_1h": realized_vol,
        "spread_bps_scaled": spread_bps / 1000.0,
        "trade_count_5m_scaled": trade_count_5m / 100.0,
        "volume_5m_scaled": math.log1p(volume_5m) / 10.0,
        "stale_trade_scaled": -(stale_trade / 3600.0),
        "stale_book_scaled": -(stale_book / 3600.0),
    }


def _as_float(raw: Any, *, default: float) -> float:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value
