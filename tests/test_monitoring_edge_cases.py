from __future__ import annotations

from pmx.monitoring.policy import MonitoringPolicyConfig, evaluate_monitoring_health

_PIPELINE_HASH = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


def test_monitoring_health_ok_when_no_quality_signals() -> None:
    result = evaluate_monitoring_health(
        pipeline_artifact={
            "run_id": "pipeline-run",
            "pipeline_payload_hash": _PIPELINE_HASH,
        },
        config=MonitoringPolicyConfig(),
    )
    assert result.health_status == "OK"
    assert result.quality_flags == ()
    assert result.quality_warnings == ()


def test_monitoring_health_warn_on_noncritical_quality_flags() -> None:
    result = evaluate_monitoring_health(
        pipeline_artifact={
            "run_id": "pipeline-run",
            "pipeline_payload_hash": _PIPELINE_HASH,
            "quality_flags": ["stale"],
            "quality_warnings": [{"code": "stale", "message": "Book stale."}],
        },
        config=MonitoringPolicyConfig(),
    )
    assert result.health_status == "WARN"
    assert "stale" in result.quality_flags


def test_monitoring_health_can_disable_warn_mode() -> None:
    result = evaluate_monitoring_health(
        pipeline_artifact={
            "run_id": "pipeline-run",
            "pipeline_payload_hash": _PIPELINE_HASH,
            "quality_flags": ["stale"],
        },
        config=MonitoringPolicyConfig(warn_on_any_quality_signal=False),
    )
    assert result.health_status == "OK"
