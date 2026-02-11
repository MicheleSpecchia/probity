from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any

from pmx.models.baselines import (
    DEFAULT_BASELINE_B_INTERCEPT,
    DEFAULT_BASELINE_B_WEIGHTS,
    baseline_a_price,
)

DEFAULT_STACKER_INTERCEPT = -0.15
DEFAULT_STACKER_WEIGHTS: dict[str, float] = {
    "agreement": 0.35,
    "p_a": 0.55,
    "p_b": 1.05,
}


@dataclass(frozen=True, slots=True)
class DriverContribution:
    feature: str
    value: float
    coefficient: float
    contribution: float

    def as_dict(self) -> dict[str, float | str]:
        return {
            "feature": self.feature,
            "value": round(self.value, 8),
            "coefficient": round(self.coefficient, 8),
            "contribution": round(self.contribution, 8),
        }


@dataclass(frozen=True, slots=True)
class LogisticModel:
    name: str
    intercept: float
    coefficients: dict[str, float]

    def predict(self, features: dict[str, float]) -> float:
        score = float(self.intercept)
        for key in sorted(self.coefficients.keys()):
            score += float(self.coefficients[key]) * float(features.get(key, 0.0))
        return _sigmoid(score)

    def contributions(self, features: dict[str, float], *, prefix: str) -> list[DriverContribution]:
        out: list[DriverContribution] = []
        for key in sorted(self.coefficients.keys()):
            value = float(features.get(key, 0.0))
            coefficient = float(self.coefficients[key])
            out.append(
                DriverContribution(
                    feature=f"{prefix}.{key}",
                    value=value,
                    coefficient=coefficient,
                    contribution=coefficient * value,
                )
            )
        return out

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "intercept": round(float(self.intercept), 8),
            "coefficients": {
                key: round(float(value), 8) for key, value in sorted(self.coefficients.items())
            },
        }


def baseline_b_model() -> LogisticModel:
    return LogisticModel(
        name="baseline_b_micro",
        intercept=float(DEFAULT_BASELINE_B_INTERCEPT),
        coefficients={
            key: float(value) for key, value in sorted(DEFAULT_BASELINE_B_WEIGHTS.items())
        },
    )


def stacker_model() -> LogisticModel:
    return LogisticModel(
        name="stacker_v1",
        intercept=float(DEFAULT_STACKER_INTERCEPT),
        coefficients={key: float(value) for key, value in sorted(DEFAULT_STACKER_WEIGHTS.items())},
    )


def compute_probabilities(
    *,
    price_prob: float,
    features: dict[str, Any],
    b_model: LogisticModel | None = None,
    ensemble_model: LogisticModel | None = None,
) -> tuple[float, float, float]:
    p_a = baseline_a_price(price_prob)
    transformed = transform_micro_features(features)
    resolved_b_model = b_model or baseline_b_model()
    p_b = resolved_b_model.predict(transformed)
    meta_features = build_ensemble_features(p_a=p_a, p_b=p_b)
    resolved_ensemble_model = ensemble_model or stacker_model()
    p_raw = resolved_ensemble_model.predict(meta_features)
    return p_a, p_b, p_raw


def extract_top_drivers(
    *,
    features: dict[str, Any],
    price_prob: float,
    top_k: int = 5,
    b_model: LogisticModel | None = None,
    ensemble_model: LogisticModel | None = None,
) -> list[dict[str, float | str]]:
    if top_k <= 0:
        return []
    resolved_b_model = b_model or baseline_b_model()
    resolved_ensemble_model = ensemble_model or stacker_model()

    p_a = baseline_a_price(price_prob)
    transformed = transform_micro_features(features)
    p_b = resolved_b_model.predict(transformed)
    meta_features = build_ensemble_features(p_a=p_a, p_b=p_b)

    contributions = resolved_b_model.contributions(transformed, prefix="micro")
    contributions.extend(resolved_ensemble_model.contributions(meta_features, prefix="meta"))
    contributions.sort(key=lambda item: (-abs(item.contribution), item.feature))
    return [item.as_dict() for item in contributions[:top_k]]


def build_model_hash(
    *,
    b_model: LogisticModel | None = None,
    ensemble_model: LogisticModel | None = None,
) -> str:
    payload = {
        "baseline_b": (b_model or baseline_b_model()).as_dict(),
        "stacker": (ensemble_model or stacker_model()).as_dict(),
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def transform_micro_features(features: dict[str, Any]) -> dict[str, float]:
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
        "book_imbalance_1": book_imbalance,
        "mid_price_centered": mid_price - 0.5,
        "realized_vol_1h": realized_vol,
        "return_5m": return_5m,
        "spread_bps_scaled": spread_bps / 1000.0,
        "stale_book_scaled": -(stale_book / 3600.0),
        "stale_trade_scaled": -(stale_trade / 3600.0),
        "trade_count_5m_scaled": trade_count_5m / 100.0,
        "volume_5m_scaled": math.log1p(volume_5m) / 10.0,
    }


def build_ensemble_features(*, p_a: float, p_b: float) -> dict[str, float]:
    p_a_clamped = _clamp(float(p_a), 0.0, 1.0)
    p_b_clamped = _clamp(float(p_b), 0.0, 1.0)
    return {
        "agreement": 1.0 - abs(p_a_clamped - p_b_clamped),
        "p_a": p_a_clamped,
        "p_b": p_b_clamped,
    }


def _as_float(raw: Any, *, default: float) -> float:
    if raw is None or isinstance(raw, bool):
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
