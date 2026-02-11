from __future__ import annotations

import pytest

from pmx.forecast.calibration import (
    IsotonicCalibrator,
    PlattCalibrator,
    calibrate_probabilities,
    fit_calibrator,
)


def test_fit_calibrator_uses_platt_below_threshold() -> None:
    probabilities = [0.1, 0.2, 0.3, 0.7, 0.8, 0.9]
    labels = [0, 0, 0, 1, 1, 1]

    calibrator_a = fit_calibrator(probabilities, labels, min_isotonic_samples=10)
    calibrator_b = fit_calibrator(probabilities, labels, min_isotonic_samples=10)

    assert isinstance(calibrator_a, PlattCalibrator)
    assert isinstance(calibrator_b, PlattCalibrator)
    assert calibrator_a.slope == pytest.approx(calibrator_b.slope, abs=1e-12)
    assert calibrator_a.intercept == pytest.approx(calibrator_b.intercept, abs=1e-12)

    calibrated = calibrate_probabilities(calibrator_a, probabilities)
    assert len(calibrated) == len(probabilities)
    assert all(0.0 <= value <= 1.0 for value in calibrated)


def test_fit_calibrator_uses_isotonic_at_threshold() -> None:
    probabilities = [i / 40.0 for i in range(1, 41)]
    labels = [0 if i < 20 else 1 for i in range(40)]

    calibrator = fit_calibrator(probabilities, labels, min_isotonic_samples=30)

    assert isinstance(calibrator, IsotonicCalibrator)
    assert len(calibrator.values) > 0
    predicted = [calibrator.predict(value) for value in probabilities]
    assert predicted == sorted(predicted)
