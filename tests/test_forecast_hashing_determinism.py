from __future__ import annotations

from pmx.forecast.calibration import calibration_report_hash
from pmx.forecast.uncertainty import uncertainty_report_hash


def test_report_hash_ignores_dict_order_and_rounds_consistently() -> None:
    calibration_a = {
        "n_bins": 10,
        "raw": {"metrics": {"ece": 0.123456789, "mce": 0.1}},
        "calibrated": {"metrics": {"ece": 0.02, "mce": 0.04}},
    }
    calibration_b = {
        "calibrated": {"metrics": {"mce": 0.04, "ece": 0.02}},
        "raw": {"metrics": {"mce": 0.1, "ece": 0.123456789}},
        "n_bins": 10,
    }
    assert calibration_report_hash(calibration_a) == calibration_report_hash(calibration_b)

    uncertainty_a = {
        "version": "uncertainty_report.v1",
        "levels": [0.5, 0.9],
        "coverage_by_level": {"0.5": 0.501234567, "0.9": 0.901234567},
        "sanity_checks": [{"code": "none", "count": 0, "level": "all"}],
    }
    uncertainty_b = {
        "sanity_checks": [{"level": "all", "count": 0, "code": "none"}],
        "coverage_by_level": {"0.9": 0.901234567, "0.5": 0.501234567},
        "levels": [0.5, 0.9],
        "version": "uncertainty_report.v1",
    }
    assert uncertainty_report_hash(uncertainty_a) == uncertainty_report_hash(uncertainty_b)
