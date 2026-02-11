from __future__ import annotations

from pmx.forecast.uncertainty import uncertainty_coverage_report, uncertainty_report_hash


def test_uncertainty_report_deterministic_hash() -> None:
    preds = [0.2, 0.8, 0.65, 0.35]
    labels = [0, 1, 1, 0]
    intervals_50 = [(0.1, 0.4), (0.7, 0.9), (0.5, 0.8), (0.2, 0.45)]
    intervals_90 = [(0.0, 0.6), (0.6, 1.0), (0.4, 0.95), (0.0, 0.6)]

    report_a, flags_a, warnings_a = uncertainty_coverage_report(
        preds,
        labels,
        intervals_50,
        intervals_90,
    )
    report_b, flags_b, warnings_b = uncertainty_coverage_report(
        preds,
        labels,
        intervals_50,
        intervals_90,
    )

    assert report_a == report_b
    assert flags_a == flags_b
    assert warnings_a == warnings_b
    assert uncertainty_report_hash(report_a) == uncertainty_report_hash(report_b)


def test_uncertainty_report_perfect_coverage() -> None:
    preds = [0.2, 0.9, 0.7, 0.1]
    labels = [0, 1, 1, 0]
    intervals_50 = [(0.0, 0.4), (0.6, 1.0), (0.6, 1.0), (0.0, 0.3)]
    intervals_90 = [(0.0, 0.8), (0.2, 1.0), (0.2, 1.0), (0.0, 0.8)]

    report, flags, _warnings = uncertainty_coverage_report(
        preds,
        labels,
        intervals_50,
        intervals_90,
        min_n=1,
    )

    assert report["coverage_by_level"]["0.5"] == 1.0
    assert report["coverage_by_level"]["0.9"] == 1.0
    assert report["p_outside_rate_by_level"]["0.5"] == 0.0
    assert report["p_outside_rate_by_level"]["0.9"] == 0.0
    assert "coverage_below_target_50" not in flags
    assert "coverage_below_target_90" not in flags


def test_uncertainty_report_invalid_intervals_flag() -> None:
    preds = [0.2, 0.8]
    labels = [0, 1]
    intervals_50 = [(0.1, 0.5), (0.7, 0.9)]
    intervals_90 = [(0.9, 0.1), (0.0, 1.2)]

    report, flags, warnings = uncertainty_coverage_report(
        preds,
        labels,
        intervals_50,
        intervals_90,
        min_n=1,
    )

    assert report["invalid_interval_count"] > 0
    assert "conformal_invalid_intervals" in flags
    assert any("invalid" in warning["code"] for warning in warnings)


def test_uncertainty_report_monotonic_violation_warning() -> None:
    preds = [0.5, 0.6]
    labels = [0, 1]
    intervals_50 = [(0.2, 0.8), (0.3, 0.9)]
    intervals_90 = [(0.4, 0.6), (0.4, 0.7)]

    report, _flags, warnings = uncertainty_coverage_report(
        preds,
        labels,
        intervals_50,
        intervals_90,
        min_n=1,
    )

    sanity_codes = [str(item["code"]) for item in report["sanity_checks"]]
    assert "monotonic_width_violation" in sanity_codes
    assert any(
        warning["code"] == "monotonic_width_violation"
        and "90% width below 50% width" in warning.get("message", "")
        for warning in warnings
    )


def test_uncertainty_report_insufficient_data_flag() -> None:
    report, flags, warnings = uncertainty_coverage_report(
        preds=[0.2, 0.8],
        labels=[0, 1],
        intervals_50=[(0.1, 0.4), (0.6, 0.9)],
        intervals_90=[(0.0, 0.8), (0.2, 1.0)],
        min_n=10,
    )

    assert report["n_total"] == 2
    assert "insufficient_uncertainty_data" in flags
    assert any(warning["code"] == "insufficient_uncertainty_data" for warning in warnings)


def test_uncertainty_report_degenerate_interval_flag() -> None:
    report, flags, _warnings = uncertainty_coverage_report(
        preds=[0.2, 0.8, 0.3, 0.7],
        labels=[0, 1, 0, 1],
        intervals_50=[(0.2, 0.2), (0.8, 0.8), (0.3, 0.3), (0.7, 0.7)],
        intervals_90=[(0.1, 0.2), (0.7, 0.9), (0.2, 0.4), (0.6, 0.8)],
        min_n=1,
        degenerate_rate_threshold=0.20,
    )

    assert report["degenerate_interval_rate_by_level"]["0.5"] == 1.0
    assert "conformal_degenerate_intervals" in flags
