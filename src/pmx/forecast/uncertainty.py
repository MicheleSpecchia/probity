from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ConformalIntervalModel:
    q50: float
    q90: float
    residual_count: int
    calibration_count: int
    method: str = "split_conformal_abs_residual"

    def interval_50(self, probability: float) -> tuple[float, float]:
        return _clamp_interval(probability - self.q50, probability + self.q50)

    def interval_90(self, probability: float) -> tuple[float, float]:
        return _clamp_interval(probability - self.q90, probability + self.q90)

    def as_dict(self) -> dict[str, object]:
        return {
            "method": self.method,
            "q50": round(self.q50, 8),
            "q90": round(self.q90, 8),
            "residual_count": self.residual_count,
            "calibration_count": self.calibration_count,
        }


def fit_split_conformal(
    labels: list[int],
    calibrated_probabilities: list[float],
    *,
    min_calibration: int = 20,
    calibration_fraction: float = 0.25,
) -> ConformalIntervalModel:
    if len(labels) != len(calibrated_probabilities):
        raise ValueError("labels and calibrated_probabilities must have same length")
    if not labels:
        return ConformalIntervalModel(
            q50=0.15,
            q90=0.35,
            residual_count=0,
            calibration_count=0,
        )

    n = len(labels)
    calibration_count = max(min_calibration, int(math.ceil(n * calibration_fraction)))
    calibration_count = min(calibration_count, n)
    start_idx = n - calibration_count

    residuals: list[float] = []
    for y_value, p_value in zip(
        labels[start_idx:],
        calibrated_probabilities[start_idx:],
        strict=True,
    ):
        residuals.append(abs(float(y_value) - _clamp(float(p_value), 0.0, 1.0)))
    residuals.sort()
    q50 = _empirical_quantile(residuals, level=0.50)
    q90 = _empirical_quantile(residuals, level=0.90)
    return ConformalIntervalModel(
        q50=q50,
        q90=q90,
        residual_count=len(residuals),
        calibration_count=calibration_count,
    )


def build_intervals(
    model: ConformalIntervalModel,
    probability: float,
) -> dict[str, tuple[float, float]]:
    return {
        "interval_50": model.interval_50(probability),
        "interval_90": model.interval_90(probability),
    }


def interval_quality_report(
    *,
    labels: list[int],
    calibrated_probabilities: list[float],
    model: ConformalIntervalModel,
) -> dict[str, float]:
    if not labels:
        return {
            "coverage_50": 0.0,
            "coverage_90": 0.0,
            "sharpness_50": 0.0,
            "sharpness_90": 0.0,
        }

    hit_50 = 0
    hit_90 = 0
    width_50_sum = 0.0
    width_90_sum = 0.0
    for y_value, p_value in zip(labels, calibrated_probabilities, strict=True):
        lo50, hi50 = model.interval_50(p_value)
        lo90, hi90 = model.interval_90(p_value)
        y_float = float(y_value)
        if lo50 <= y_float <= hi50:
            hit_50 += 1
        if lo90 <= y_float <= hi90:
            hit_90 += 1
        width_50_sum += hi50 - lo50
        width_90_sum += hi90 - lo90

    total = float(len(labels))
    return {
        "coverage_50": hit_50 / total,
        "coverage_90": hit_90 / total,
        "sharpness_50": width_50_sum / total,
        "sharpness_90": width_90_sum / total,
    }


def conformal_hash(model: ConformalIntervalModel) -> str:
    serialized = json.dumps(model.as_dict(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _empirical_quantile(values: list[float], *, level: float) -> float:
    if not values:
        return 0.25 if level >= 0.9 else 0.10
    rank = int(math.ceil(level * len(values))) - 1
    clamped_rank = max(0, min(rank, len(values) - 1))
    return _clamp(values[clamped_rank], 0.0, 1.0)


def _clamp_interval(lo: float, hi: float) -> tuple[float, float]:
    lower = _clamp(lo, 0.0, 1.0)
    upper = _clamp(hi, 0.0, 1.0)
    if lower > upper:
        lower, upper = upper, lower
    return lower, upper


def _clamp(value: float, minimum: float, maximum: float) -> float:
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value
