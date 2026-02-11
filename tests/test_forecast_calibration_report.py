from __future__ import annotations

import math

import pytest

from pmx.forecast.calibration import calibration_report
from pmx.forecast.pipeline import evaluate_calibration_quality_gates


def test_calibration_report_metrics_are_deterministic_and_expected() -> None:
    labels = [0, 1, 1, 0]
    raw = [0.1, 0.9, 0.8, 0.2]
    calibrated = [0.1, 0.9, 0.8, 0.2]

    report = calibration_report(
        labels=labels,
        raw_probabilities=raw,
        calibrated_probabilities=calibrated,
        n_bins=2,
    )

    assert report["n_bins"] == 2
    calibrated_report = report["calibrated"]
    metrics = calibrated_report["metrics"]
    assert metrics["n_eval"] == 4
    assert metrics["ece"] == pytest.approx(0.15, abs=1e-12)
    assert metrics["mce"] == pytest.approx(0.15, abs=1e-12)
    assert metrics["brier"] == pytest.approx(0.025, abs=1e-12)
    expected_nll = -(
        math.log(0.9) + math.log(0.9) + math.log(0.8) + math.log(0.8)
    ) / 4.0
    assert metrics["nll"] == pytest.approx(expected_nll, abs=1e-12)

    bins = calibrated_report["bins"]
    assert len(bins) == 2
    assert bins[0]["count"] == 2
    assert bins[0]["avg_pred"] == pytest.approx(0.15, abs=1e-12)
    assert bins[0]["emp_freq"] == pytest.approx(0.0, abs=1e-12)
    assert bins[1]["count"] == 2
    assert bins[1]["avg_pred"] == pytest.approx(0.85, abs=1e-12)
    assert bins[1]["emp_freq"] == pytest.approx(1.0, abs=1e-12)


def test_calibration_report_hash_is_stable() -> None:
    labels = [0, 1, 0, 1, 1, 0]
    raw = [0.2, 0.8, 0.3, 0.7, 0.75, 0.25]
    calibrated = [0.15, 0.85, 0.35, 0.65, 0.8, 0.2]

    report_a = calibration_report(
        labels=labels,
        raw_probabilities=raw,
        calibrated_probabilities=calibrated,
        n_bins=3,
    )
    report_b = calibration_report(
        labels=labels,
        raw_probabilities=raw,
        calibrated_probabilities=calibrated,
        n_bins=3,
    )

    assert report_a == report_b
    assert report_a["report_hash"] == report_b["report_hash"]


def test_quality_gates_soft_flags() -> None:
    flags = evaluate_calibration_quality_gates(
        n_eval=12,
        calibrated_ece=0.12,
        min_eval=40,
        ece_threshold=0.08,
    )
    assert flags == ("insufficient_calibration_data", "poor_calibration")

    ok_flags = evaluate_calibration_quality_gates(
        n_eval=80,
        calibrated_ece=0.03,
        min_eval=40,
        ece_threshold=0.08,
    )
    assert ok_flags == ()
