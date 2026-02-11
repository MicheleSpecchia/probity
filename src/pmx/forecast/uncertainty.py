from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from pmx.forecast.canonical import canonical_hash


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
    calibration_count = max(min_calibration, math.ceil(n * calibration_fraction))
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
    return canonical_hash(model.as_dict())


def uncertainty_report_hash(report: Mapping[str, Any]) -> str:
    return canonical_hash(report)


def uncertainty_coverage_report(
    preds: Sequence[float],
    labels: Sequence[int],
    intervals_50: Sequence[tuple[float, float]],
    intervals_90: Sequence[tuple[float, float]],
    *,
    min_n: int = 50,
    target_50: float = 0.50,
    target_90: float = 0.90,
    tol: float = 0.05,
    degenerate_tol: float = 1e-12,
    degenerate_rate_threshold: float = 0.25,
) -> tuple[dict[str, Any], tuple[str, ...], tuple[dict[str, str], ...]]:
    aligned_n = min(len(preds), len(labels), len(intervals_50), len(intervals_90))
    aligned_labels = [int(labels[idx]) for idx in range(aligned_n)]
    aligned_intervals_50 = [intervals_50[idx] for idx in range(aligned_n)]
    aligned_intervals_90 = [intervals_90[idx] for idx in range(aligned_n)]

    sanity_checks: list[dict[str, Any]] = []
    if (
        len(preds) != len(labels)
        or len(preds) != len(intervals_50)
        or len(preds) != len(intervals_90)
    ):
        _append_sanity_check(
            sanity_checks,
            code="length_mismatch_truncated",
            level="all",
            count=max(len(preds), len(labels), len(intervals_50), len(intervals_90)) - aligned_n,
        )

    level_50 = _analyze_level(
        intervals=aligned_intervals_50,
        labels=aligned_labels,
        degenerate_tol=degenerate_tol,
    )
    level_90 = _analyze_level(
        intervals=aligned_intervals_90,
        labels=aligned_labels,
        degenerate_tol=degenerate_tol,
    )

    if level_50["invalid_count"] > 0:
        _append_sanity_check(
            sanity_checks,
            code="invalid_interval",
            level="0.5",
            count=level_50["invalid_count"],
            example_ids=level_50["invalid_example_ids"],
        )
    if level_90["invalid_count"] > 0:
        _append_sanity_check(
            sanity_checks,
            code="invalid_interval",
            level="0.9",
            count=level_90["invalid_count"],
            example_ids=level_90["invalid_example_ids"],
        )
    if level_50["degenerate_count"] > 0:
        _append_sanity_check(
            sanity_checks,
            code="degenerate_interval",
            level="0.5",
            count=level_50["degenerate_count"],
            example_ids=level_50["degenerate_example_ids"],
        )
    if level_90["degenerate_count"] > 0:
        _append_sanity_check(
            sanity_checks,
            code="degenerate_interval",
            level="0.9",
            count=level_90["degenerate_count"],
            example_ids=level_90["degenerate_example_ids"],
        )

    monotonic_violations = _monotonic_violations(
        widths_50=level_50["widths_by_index"],
        widths_90=level_90["widths_by_index"],
        tol=degenerate_tol,
    )
    if monotonic_violations["count"] > 0:
        _append_sanity_check(
            sanity_checks,
            code="monotonic_width_violation",
            level="all",
            count=monotonic_violations["count"],
            example_ids=monotonic_violations["example_ids"],
        )

    if aligned_n < min_n:
        _append_sanity_check(
            sanity_checks,
            code="insufficient_uncertainty_data",
            level="all",
            count=aligned_n,
        )

    coverage_50 = level_50["coverage"]
    coverage_90 = level_90["coverage"]
    report: dict[str, Any] = {
        "version": "uncertainty_report.v1",
        "levels": [0.5, 0.9],
        "n_total": aligned_n,
        "n_by_level": {
            "0.5": aligned_n,
            "0.9": aligned_n,
        },
        "coverage_by_level": {
            "0.5": _round_metric(coverage_50),
            "0.9": _round_metric(coverage_90),
        },
        "avg_width_by_level": {
            "0.5": _round_metric(level_50["avg_width"]),
            "0.9": _round_metric(level_90["avg_width"]),
        },
        "median_width_by_level": {
            "0.5": _round_metric(level_50["median_width"]),
            "0.9": _round_metric(level_90["median_width"]),
        },
        "p_outside_rate_by_level": {
            "0.5": _round_metric(1.0 - coverage_50),
            "0.9": _round_metric(1.0 - coverage_90),
        },
        "degenerate_interval_rate_by_level": {
            "0.5": _round_metric(level_50["degenerate_rate"]),
            "0.9": _round_metric(level_90["degenerate_rate"]),
        },
        "invalid_interval_count": int(level_50["invalid_count"] + level_90["invalid_count"]),
        "sanity_checks": sanity_checks,
    }

    flags: list[str] = []
    warnings: list[dict[str, str]] = []
    if aligned_n < min_n:
        flags.append("insufficient_uncertainty_data")
        warnings.append(
            {
                "code": "insufficient_uncertainty_data",
                "message": "Uncertainty report has insufficient sample size.",
            }
        )
    if report["invalid_interval_count"] > 0:
        flags.append("conformal_invalid_intervals")
        warnings.append(
            {
                "code": "conformal_invalid_intervals",
                "message": "Conformal intervals include invalid entries.",
            }
        )
    degenerate_rate_max = max(level_50["degenerate_rate"], level_90["degenerate_rate"])
    if degenerate_rate_max > degenerate_rate_threshold:
        flags.append("conformal_degenerate_intervals")
        warnings.append(
            {
                "code": "conformal_degenerate_intervals",
                "message": "Conformal intervals are frequently degenerate.",
            }
        )
    if coverage_90 < (target_90 - tol):
        flags.append("coverage_below_target_90")
        warnings.append(
            {
                "code": "coverage_below_target_90",
                "message": "Observed 90% interval coverage below target.",
            }
        )
    if coverage_50 < (target_50 - tol):
        flags.append("coverage_below_target_50")
        warnings.append(
            {
                "code": "coverage_below_target_50",
                "message": "Observed 50% interval coverage below target.",
            }
        )
    if monotonic_violations["count"] > 0:
        warnings.append(
            {
                "code": "monotonic_width_violation",
                "message": "Found intervals with 90% width below 50% width.",
            }
        )

    return report, tuple(flags), tuple(warnings)


def _empirical_quantile(values: list[float], *, level: float) -> float:
    if not values:
        return 0.25 if level >= 0.9 else 0.10
    rank = math.ceil(level * len(values)) - 1
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


def _analyze_level(
    *,
    intervals: Sequence[tuple[float, float]],
    labels: Sequence[int],
    degenerate_tol: float,
) -> dict[str, Any]:
    n_total = min(len(intervals), len(labels))
    covered = 0
    invalid_count = 0
    degenerate_count = 0
    invalid_example_ids: list[int] = []
    degenerate_example_ids: list[int] = []
    widths_by_index: list[float | None] = [None for _ in range(n_total)]
    widths_for_median: list[float] = []

    for idx in range(n_total):
        raw_interval = intervals[idx]
        raw_lo, raw_hi = raw_interval
        lo = _finite_float(raw_lo)
        hi = _finite_float(raw_hi)
        if lo is None or hi is None or lo > hi:
            invalid_count += 1
            invalid_example_ids.append(idx)
            continue

        if lo < 0.0 or hi > 1.0:
            invalid_count += 1
            invalid_example_ids.append(idx)

        clipped_lo = _clamp(lo, 0.0, 1.0)
        clipped_hi = _clamp(hi, 0.0, 1.0)
        width = max(0.0, clipped_hi - clipped_lo)
        widths_by_index[idx] = width
        widths_for_median.append(width)

        label = float(labels[idx])
        if clipped_lo <= label <= clipped_hi:
            covered += 1
        if width <= degenerate_tol:
            degenerate_count += 1
            degenerate_example_ids.append(idx)

    if n_total <= 0:
        return {
            "coverage": 0.0,
            "avg_width": 0.0,
            "median_width": 0.0,
            "degenerate_rate": 0.0,
            "invalid_count": 0,
            "degenerate_count": 0,
            "invalid_example_ids": [],
            "degenerate_example_ids": [],
            "widths_by_index": [],
        }

    avg_width = sum(width for width in widths_by_index if width is not None) / n_total
    median_width = _median(widths_for_median)
    return {
        "coverage": covered / n_total,
        "avg_width": avg_width,
        "median_width": median_width,
        "degenerate_rate": degenerate_count / n_total,
        "invalid_count": invalid_count,
        "degenerate_count": degenerate_count,
        "invalid_example_ids": invalid_example_ids[:5],
        "degenerate_example_ids": degenerate_example_ids[:5],
        "widths_by_index": widths_by_index,
    }


def _monotonic_violations(
    *,
    widths_50: Sequence[float | None],
    widths_90: Sequence[float | None],
    tol: float,
) -> dict[str, Any]:
    n_total = min(len(widths_50), len(widths_90))
    examples: list[int] = []
    count = 0
    for idx in range(n_total):
        width_50 = widths_50[idx]
        width_90 = widths_90[idx]
        if width_50 is None or width_90 is None:
            continue
        if width_90 + tol < width_50:
            count += 1
            examples.append(idx)
    return {"count": count, "example_ids": examples[:5]}


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        return sorted_values[mid]
    return (sorted_values[mid - 1] + sorted_values[mid]) / 2.0


def _finite_float(raw: Any) -> float | None:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def _append_sanity_check(
    checks: list[dict[str, Any]],
    *,
    code: str,
    level: str,
    count: int,
    example_ids: Sequence[int] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "code": code,
        "level": level,
        "count": int(count),
    }
    if example_ids:
        payload["example_ids"] = [int(item) for item in sorted(example_ids)[:5]]
    checks.append(payload)


def _round_metric(value: float) -> float:
    return round(float(value), 6)
