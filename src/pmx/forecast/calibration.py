from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any

from pmx.backtest.metrics import calibration_bins

EPS = 1e-6


@dataclass(frozen=True, slots=True)
class IsotonicCalibrator:
    upper_bounds: tuple[float, ...]
    values: tuple[float, ...]

    def predict(self, probability: float) -> float:
        p = _clamp(probability, 0.0, 1.0)
        for upper, value in zip(self.upper_bounds, self.values, strict=True):
            if p <= upper:
                return _clamp(value, 0.0, 1.0)
        return _clamp(self.values[-1] if self.values else p, 0.0, 1.0)

    def as_dict(self) -> dict[str, object]:
        return {
            "kind": "isotonic",
            "upper_bounds": [round(item, 8) for item in self.upper_bounds],
            "values": [round(item, 8) for item in self.values],
        }


@dataclass(frozen=True, slots=True)
class PlattCalibrator:
    slope: float
    intercept: float

    def predict(self, probability: float) -> float:
        p = _clamp(probability, EPS, 1.0 - EPS)
        logit = math.log(p / (1.0 - p))
        return _sigmoid(self.slope * logit + self.intercept)

    def as_dict(self) -> dict[str, object]:
        return {
            "kind": "platt",
            "slope": round(self.slope, 8),
            "intercept": round(self.intercept, 8),
        }


Calibrator = IsotonicCalibrator | PlattCalibrator


def fit_calibrator(
    probabilities: list[float],
    labels: list[int],
    *,
    min_isotonic_samples: int = 30,
) -> Calibrator:
    _validate_inputs(probabilities, labels)
    if not probabilities:
        return PlattCalibrator(slope=1.0, intercept=0.0)
    if len(probabilities) >= min_isotonic_samples:
        return fit_isotonic(probabilities, labels)
    return fit_platt(probabilities, labels)


def fit_isotonic(probabilities: list[float], labels: list[int]) -> IsotonicCalibrator:
    _validate_inputs(probabilities, labels)
    if not probabilities:
        return IsotonicCalibrator(upper_bounds=(1.0,), values=(0.5,))

    ranked = sorted(
        enumerate(zip(probabilities, labels, strict=True)),
        key=lambda item: (float(item[1][0]), item[0]),
    )

    blocks: list[_IsoBlock] = []
    for _, (probability, label) in ranked:
        block = _IsoBlock(
            lower=float(probability),
            upper=float(probability),
            weight=1,
            positive=float(label),
        )
        blocks.append(block)
        while len(blocks) >= 2:
            prev = blocks[-2]
            curr = blocks[-1]
            if prev.mean <= curr.mean:
                break
            merged = _IsoBlock(
                lower=prev.lower,
                upper=curr.upper,
                weight=prev.weight + curr.weight,
                positive=prev.positive + curr.positive,
            )
            blocks[-2:] = [merged]

    upper_bounds = tuple(_clamp(block.upper, 0.0, 1.0) for block in blocks)
    values = tuple(_clamp(block.mean, 0.0, 1.0) for block in blocks)
    return IsotonicCalibrator(upper_bounds=upper_bounds, values=values)


def fit_platt(
    probabilities: list[float],
    labels: list[int],
    *,
    iterations: int = 80,
    learning_rate: float = 0.2,
    l2: float = 1e-3,
) -> PlattCalibrator:
    _validate_inputs(probabilities, labels)
    if not probabilities:
        return PlattCalibrator(slope=1.0, intercept=0.0)

    xs = [_logit(_clamp(item, EPS, 1.0 - EPS)) for item in probabilities]
    ys = [float(item) for item in labels]

    slope = 1.0
    intercept = 0.0
    n = float(len(xs))
    for _ in range(iterations):
        grad_slope = 0.0
        grad_intercept = 0.0
        for x_value, y_value in zip(xs, ys, strict=True):
            pred = _sigmoid(slope * x_value + intercept)
            error = pred - y_value
            grad_slope += error * x_value
            grad_intercept += error
        grad_slope = grad_slope / n + l2 * slope
        grad_intercept = grad_intercept / n
        slope -= learning_rate * grad_slope
        intercept -= learning_rate * grad_intercept
    return PlattCalibrator(slope=slope, intercept=intercept)


def calibrate_probabilities(calibrator: Calibrator, probabilities: list[float]) -> list[float]:
    return [calibrator.predict(probability) for probability in probabilities]


def calibration_report(
    *,
    labels: list[int],
    raw_probabilities: list[float],
    calibrated_probabilities: list[float],
    n_bins: int = 10,
) -> dict[str, Any]:
    raw_report = _single_calibration_report(
        labels=labels,
        probabilities=raw_probabilities,
        n_bins=n_bins,
    )
    calibrated_report = _single_calibration_report(
        labels=labels,
        probabilities=calibrated_probabilities,
        n_bins=n_bins,
    )
    payload: dict[str, Any] = {
        "n_bins": n_bins,
        "raw": raw_report,
        "calibrated": calibrated_report,
    }
    payload["report_hash"] = _stable_hash(payload)
    return payload


def calibrator_hash(calibrator: Calibrator) -> str:
    serialized = json.dumps(calibrator.as_dict(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class _IsoBlock:
    lower: float
    upper: float
    weight: int
    positive: float

    @property
    def mean(self) -> float:
        if self.weight <= 0:
            return 0.5
        return self.positive / self.weight


def _validate_inputs(probabilities: list[float], labels: list[int]) -> None:
    if len(probabilities) != len(labels):
        raise ValueError("probabilities and labels must have same length")


def _single_calibration_report(
    *,
    labels: list[int],
    probabilities: list[float],
    n_bins: int,
) -> dict[str, Any]:
    _validate_inputs(probabilities, labels)
    bins = calibration_bins(labels, probabilities, n_bins=n_bins)
    total = len(labels)

    brier_sum = 0.0
    nll_sum = 0.0
    for y_value, p_value in zip(labels, probabilities, strict=True):
        probability = _clamp(float(p_value), 0.0, 1.0)
        y_float = float(y_value)
        brier_sum += (probability - y_float) ** 2
        nll_sum += -(
            y_float * math.log(_clamp(probability, EPS, 1.0 - EPS))
            + (1.0 - y_float) * math.log(_clamp(1.0 - probability, EPS, 1.0 - EPS))
        )

    bin_payload: list[dict[str, float | int]] = []
    ece = 0.0
    mce = 0.0
    for bucket in bins:
        diff = abs(bucket.mean_pred - bucket.mean_true)
        if total > 0:
            ece += diff * (bucket.count / total)
        if diff > mce:
            mce = diff
        bin_payload.append(
            {
                "index": int(bucket.index),
                "lower": round(float(bucket.lower), 8),
                "upper": round(float(bucket.upper), 8),
                "count": int(bucket.count),
                "avg_pred": round(float(bucket.mean_pred), 8),
                "emp_freq": round(float(bucket.mean_true), 8),
            }
        )

    metrics = {
        "n_eval": total,
        "ece": ece,
        "mce": mce,
        "brier": (brier_sum / total) if total > 0 else 0.0,
        "nll": (nll_sum / total) if total > 0 else 0.0,
    }
    return {
        "bins": bin_payload,
        "metrics": metrics,
    }


def _stable_hash(payload: object) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _logit(value: float) -> float:
    return math.log(value / (1.0 - value))


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
