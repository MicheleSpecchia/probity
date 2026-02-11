from __future__ import annotations

import pytest

from pmx.backtest.metrics import (
    aggregate_metrics,
    brier_score,
    calibration_bins,
    ece_score,
    sharpness_score,
)


def test_metrics_are_deterministic_and_correct() -> None:
    y_true = [0, 1, 1, 0]
    p_pred = [0.1, 0.9, 0.8, 0.2]

    assert brier_score(y_true, p_pred) == pytest.approx(0.025, abs=1e-9)
    assert ece_score(y_true, p_pred, n_bins=2) == pytest.approx(0.15, abs=1e-9)
    assert sharpness_score(p_pred) == pytest.approx(0.125, abs=1e-9)

    bins = calibration_bins(y_true, p_pred, n_bins=2)
    assert len(bins) == 2
    assert bins[0].count == 2
    assert bins[1].count == 2


def test_aggregate_metrics_shape() -> None:
    y_true = [1, 0, 1]
    p_pred = [0.6, 0.4, 0.7]
    metrics = aggregate_metrics(y_true, p_pred)

    assert metrics["n_examples"] == 3
    assert "brier" in metrics
    assert "ece" in metrics
    assert "sharpness" in metrics
    assert isinstance(metrics["calibration_bins"], list)
    assert isinstance(metrics["coverage"], dict)
