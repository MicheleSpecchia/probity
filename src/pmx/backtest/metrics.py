from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class CalibrationBin:
    index: int
    lower: float
    upper: float
    count: int
    mean_pred: float
    mean_true: float

    def as_dict(self) -> dict[str, float | int]:
        return {
            "index": self.index,
            "lower": self.lower,
            "upper": self.upper,
            "count": self.count,
            "mean_pred": self.mean_pred,
            "mean_true": self.mean_true,
        }


def brier_score(y_true: list[int], p_pred: list[float]) -> float:
    _validate_inputs(y_true, p_pred)
    if not y_true:
        return 0.0
    total = 0.0
    for y, p in zip(y_true, p_pred, strict=True):
        total += (float(p) - float(y)) ** 2
    return total / len(y_true)


def calibration_bins(
    y_true: list[int],
    p_pred: list[float],
    *,
    n_bins: int = 10,
) -> tuple[CalibrationBin, ...]:
    _validate_inputs(y_true, p_pred)
    if n_bins <= 0:
        raise ValueError("n_bins must be > 0")

    counts = [0 for _ in range(n_bins)]
    pred_sums = [0.0 for _ in range(n_bins)]
    true_sums = [0.0 for _ in range(n_bins)]

    for y, p in zip(y_true, p_pred, strict=True):
        prob = _clamp(float(p), 0.0, 1.0)
        index = min(int(prob * n_bins), n_bins - 1)
        counts[index] += 1
        pred_sums[index] += prob
        true_sums[index] += float(y)

    bins: list[CalibrationBin] = []
    width = 1.0 / n_bins
    for index in range(n_bins):
        count = counts[index]
        mean_pred = pred_sums[index] / count if count else 0.0
        mean_true = true_sums[index] / count if count else 0.0
        bins.append(
            CalibrationBin(
                index=index,
                lower=round(index * width, 6),
                upper=round((index + 1) * width, 6),
                count=count,
                mean_pred=mean_pred,
                mean_true=mean_true,
            )
        )
    return tuple(bins)


def ece_score(y_true: list[int], p_pred: list[float], *, n_bins: int = 10) -> float:
    bins = calibration_bins(y_true, p_pred, n_bins=n_bins)
    total = len(y_true)
    if total == 0:
        return 0.0
    error = 0.0
    for bucket in bins:
        if bucket.count == 0:
            continue
        error += abs(bucket.mean_pred - bucket.mean_true) * (bucket.count / total)
    return error


def sharpness_score(p_pred: list[float]) -> float:
    if not p_pred:
        return 0.0
    mean = sum(p_pred) / len(p_pred)
    variance = sum((p - mean) ** 2 for p in p_pred) / len(p_pred)
    return variance


def coverage_placeholder() -> dict[str, Any]:
    return {
        "coverage_50": None,
        "coverage_90": None,
        "note": "interval coverage not implemented in baseline-only backtest",
    }


def aggregate_metrics(y_true: list[int], p_pred: list[float]) -> dict[str, Any]:
    bins = calibration_bins(y_true, p_pred, n_bins=10)
    return {
        "n_examples": len(y_true),
        "brier": brier_score(y_true, p_pred),
        "ece": ece_score(y_true, p_pred, n_bins=10),
        "sharpness": sharpness_score(p_pred),
        "calibration_bins": [item.as_dict() for item in bins],
        "coverage": coverage_placeholder(),
    }


def _validate_inputs(y_true: list[int], p_pred: list[float]) -> None:
    if len(y_true) != len(p_pred):
        raise ValueError("y_true and p_pred must have the same length")


def _clamp(value: float, minimum: float, maximum: float) -> float:
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value
