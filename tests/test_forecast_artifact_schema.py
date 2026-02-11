from __future__ import annotations

from typing import Any

from pmx.forecast.validate_artifact import validate_forecast_artifact


def _valid_payload() -> dict[str, Any]:
    return {
        "artifact_schema_version": "forecast_artifact.v1",
        "run_id": "run-123",
        "code_version": "abc123",
        "config_hash": "0" * 64,
        "dataset_hash": "1" * 64,
        "model_hash": "2" * 64,
        "calibration_hash": "3" * 64,
        "uncertainty_hash": "4" * 64,
        "forecast_payload_hash": "5" * 64,
        "calibration_report": {"n_bins": 10},
        "calibration_report_hash": "6" * 64,
        "uncertainty_report": {"version": "uncertainty_report.v1"},
        "uncertainty_report_hash": "7" * 64,
        "quality_flags": ["poor_calibration"],
        "quality_warnings": [{"code": "poor_calibration", "message": "warning"}],
    }


def test_forecast_artifact_schema_valid_payload() -> None:
    payload = _valid_payload()
    errors = validate_forecast_artifact(payload)
    assert errors == []


def test_forecast_artifact_schema_missing_field_has_deterministic_error() -> None:
    payload = _valid_payload()
    payload.pop("model_hash")

    errors = validate_forecast_artifact(payload)
    assert len(errors) >= 1
    first = errors[0]
    assert first["code"] == "schema:required"
    assert first["path"] == "$"
    assert "model_hash" in str(first["reason"])
