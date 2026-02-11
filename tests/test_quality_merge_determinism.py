from __future__ import annotations

from pmx.forecast.quality import merge_quality_flags, merge_quality_warnings


def test_quality_flags_merge_is_unique_sorted_and_deterministic() -> None:
    merged_a = merge_quality_flags(
        ["poor_calibration", "insufficient_uncertainty_data"],
        ["poor_calibration", "coverage_below_target_90"],
    )
    merged_b = merge_quality_flags(
        ["coverage_below_target_90", "poor_calibration"],
        ["insufficient_uncertainty_data", "poor_calibration"],
    )

    assert merged_a == merged_b
    assert merged_a == (
        "coverage_below_target_90",
        "insufficient_uncertainty_data",
        "poor_calibration",
    )


def test_quality_warnings_merge_is_sorted_and_deterministic() -> None:
    warnings_a = (
        {"code": "poor_calibration", "message": "ece too high"},
        {"code": "insufficient_uncertainty_data", "message": "n too low"},
    )
    warnings_b = (
        {"message": "n too low", "code": "insufficient_uncertainty_data"},
        {"code": "poor_calibration", "detail": "ece too high"},
    )

    merged_a = merge_quality_warnings(warnings_a, warnings_b)
    merged_b = merge_quality_warnings(warnings_b, warnings_a)
    assert merged_a == merged_b
    assert merged_a == (
        {"code": "insufficient_uncertainty_data", "message": "n too low"},
        {"code": "poor_calibration", "message": "ece too high"},
    )
