from __future__ import annotations

from pmx.forecast.uncertainty import build_intervals, fit_split_conformal, interval_quality_report


def test_split_conformal_intervals_are_deterministic() -> None:
    labels = [0, 1] * 20
    probabilities = [0.2, 0.8] * 20

    model_a = fit_split_conformal(labels, probabilities, min_calibration=10)
    model_b = fit_split_conformal(labels, probabilities, min_calibration=10)

    assert model_a == model_b
    assert model_a.q90 >= model_a.q50

    intervals_a = build_intervals(model_a, 0.63)
    intervals_b = build_intervals(model_b, 0.63)
    assert intervals_a == intervals_b
    lo50, hi50 = intervals_a["interval_50"]
    lo90, hi90 = intervals_a["interval_90"]
    assert lo90 <= lo50 <= hi50 <= hi90


def test_interval_quality_report_is_bounded() -> None:
    labels = [0, 0, 1, 1, 0, 1, 1, 0]
    probabilities = [0.15, 0.25, 0.8, 0.9, 0.35, 0.75, 0.7, 0.2]
    model = fit_split_conformal(labels, probabilities, min_calibration=4)

    report = interval_quality_report(
        labels=labels,
        calibrated_probabilities=probabilities,
        model=model,
    )

    assert 0.0 <= report["coverage_50"] <= 1.0
    assert 0.0 <= report["coverage_90"] <= 1.0
    assert report["sharpness_50"] >= 0.0
    assert report["sharpness_90"] >= report["sharpness_50"]
    assert report["coverage_90"] >= report["coverage_50"]
