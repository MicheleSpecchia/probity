"""Backtesting helpers."""

from typing import Any

__all__ = [
    "AsofDataset",
    "CalibrationBin",
    "DbOutcomeProvider",
    "Example",
    "FixtureOutcomeProvider",
    "OutcomeRecord",
    "aggregate_metrics",
    "brier_score",
    "build_asof_dataset",
    "build_asof_examples",
    "calibration_bins",
    "coverage_placeholder",
    "ece_score",
    "sharpness_score",
]


def __getattr__(name: str) -> Any:
    if name in {
        "AsofDataset",
        "DbOutcomeProvider",
        "Example",
        "FixtureOutcomeProvider",
        "OutcomeRecord",
        "build_asof_dataset",
        "build_asof_examples",
    }:
        from pmx.backtest import asof_dataset

        return getattr(asof_dataset, name)
    if name in {
        "CalibrationBin",
        "aggregate_metrics",
        "brier_score",
        "calibration_bins",
        "coverage_placeholder",
        "ece_score",
        "sharpness_score",
    }:
        from pmx.backtest import metrics

        return getattr(metrics, name)
    raise AttributeError(name)
