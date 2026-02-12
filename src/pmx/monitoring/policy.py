from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypeGuard

HealthStatus = Literal["OK", "WARN", "FAIL"]


@dataclass(frozen=True, slots=True)
class MonitoringPolicyConfig:
    fail_on_critical_block: bool = True
    warn_on_any_quality_signal: bool = True

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "fail_on_critical_block": self.fail_on_critical_block,
            "warn_on_any_quality_signal": self.warn_on_any_quality_signal,
        }


@dataclass(frozen=True, slots=True)
class MonitoringResult:
    health_status: HealthStatus
    health_summary: dict[str, Any]
    quality_flags: tuple[str, ...]
    quality_warnings: tuple[dict[str, str], ...]


def evaluate_monitoring_health(
    *,
    pipeline_artifact: Mapping[str, Any],
    config: MonitoringPolicyConfig,
    forecast_artifact: Mapping[str, Any] | None = None,
    performance_artifact: Mapping[str, Any] | None = None,
    risk_artifact: Mapping[str, Any] | None = None,
) -> MonitoringResult:
    flags: set[str] = set()
    warning_map: dict[tuple[str, str, str], dict[str, str]] = {}
    source_counts = {
        "forecast": {"flags": 0, "warnings": 0},
        "pipeline": {"flags": 0, "warnings": 0},
        "performance": {"flags": 0, "warnings": 0},
        "risk": {"flags": 0, "warnings": 0},
    }
    fail_codes: set[str] = set()

    _collect_quality_signals(
        pipeline_artifact,
        source="pipeline",
        flags=flags,
        warning_map=warning_map,
        source_counts=source_counts,
    )
    if forecast_artifact is not None:
        _collect_quality_signals(
            forecast_artifact,
            source="forecast",
            flags=flags,
            warning_map=warning_map,
            source_counts=source_counts,
        )
    if performance_artifact is not None:
        _collect_quality_signals(
            performance_artifact,
            source="performance",
            flags=flags,
            warning_map=warning_map,
            source_counts=source_counts,
        )
    if risk_artifact is not None:
        _collect_quality_signals(
            risk_artifact,
            source="risk",
            flags=flags,
            warning_map=warning_map,
            source_counts=source_counts,
        )
        fail_codes.update(_critical_block_reasons(risk_artifact))

    for flag in flags:
        if flag.startswith("critical_"):
            fail_codes.add(f"critical_flag:{flag}")

    status: HealthStatus
    if config.fail_on_critical_block and fail_codes:
        status = "FAIL"
    elif config.warn_on_any_quality_signal and (flags or warning_map):
        status = "WARN"
    else:
        status = "OK"

    quality_flags = tuple(sorted(flags))
    warning_keys = sorted(warning_map.keys(), key=lambda key: key)
    quality_warnings = tuple(warning_map[key] for key in warning_keys)
    health_summary = {
        "n_flags": len(quality_flags),
        "n_warnings": len(quality_warnings),
        "n_fail_codes": len(fail_codes),
        "fail_codes": sorted(fail_codes),
        "sources": {
            key: {
                "flags": int(value["flags"]),
                "warnings": int(value["warnings"]),
            }
            for key, value in sorted(source_counts.items(), key=lambda item: item[0])
        },
    }
    return MonitoringResult(
        health_status=status,
        health_summary=health_summary,
        quality_flags=quality_flags,
        quality_warnings=quality_warnings,
    )


def _collect_quality_signals(
    artifact: Mapping[str, Any],
    *,
    source: str,
    flags: set[str],
    warning_map: dict[tuple[str, str, str], dict[str, str]],
    source_counts: dict[str, dict[str, int]],
) -> None:
    raw_flags = artifact.get("quality_flags")
    if _is_sequence(raw_flags):
        for raw_flag in raw_flags:
            text = _optional_text(raw_flag)
            if text is None:
                continue
            flags.add(text)
            source_counts[source]["flags"] += 1

    raw_warnings = artifact.get("quality_warnings")
    if _is_sequence(raw_warnings):
        for raw_warning in raw_warnings:
            if not isinstance(raw_warning, Mapping):
                continue
            code = _optional_text(raw_warning.get("code")) or "unknown_warning"
            message = _optional_text(raw_warning.get("message")) or _optional_text(
                raw_warning.get("detail")
            )
            key = (code, message or "", source)
            payload: dict[str, str] = {"code": code, "source": source}
            if message is not None:
                payload["message"] = message
            warning_map[key] = payload
            source_counts[source]["warnings"] += 1


def _critical_block_reasons(risk_artifact: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    raw_items = risk_artifact.get("items")
    if not _is_sequence(raw_items):
        return out
    for raw_item in raw_items:
        if not isinstance(raw_item, Mapping):
            continue
        verdict = _optional_text(raw_item.get("verdict"))
        if verdict != "BLOCK":
            continue
        reasons = raw_item.get("reason_codes")
        if not _is_sequence(reasons):
            continue
        for reason_raw in reasons:
            reason = _optional_text(reason_raw)
            if reason is None:
                continue
            if "critical_" in reason:
                out.add(f"critical_block_reason:{reason}")
    return out


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _is_sequence(raw: Any) -> TypeGuard[Sequence[Any]]:
    return isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray))
