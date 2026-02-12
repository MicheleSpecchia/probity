from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.performance.artifact import (
    PERFORMANCE_POLICY_VERSION,
    build_performance_report_artifact,
)
from pmx.performance.canonical import canonical_hash
from pmx.performance.metrics import (
    DEFAULT_MIN_INPUTS_WARNING,
    DEFAULT_NEGATIVE_PNL_BPS_THRESHOLD,
    DEFAULT_NEGATIVE_PNL_USD_THRESHOLD,
    DEFAULT_TOP1_SHARE_THRESHOLD,
    DEFAULT_TOP3_SHARE_THRESHOLD,
    compute_performance_metrics,
)
from pmx.performance.validate_artifact import validate_performance_report_artifact
from pmx.portfolio.validate_artifact import validate_portfolio_artifact

JOB_NAME = "performance_from_portfolio"


@dataclass(frozen=True, slots=True)
class PerformanceFromPortfolioConfig:
    artifacts_root: str
    window_from: datetime | None
    window_to: datetime | None
    min_inputs_warning: int = DEFAULT_MIN_INPUTS_WARNING
    top1_share_threshold: float = DEFAULT_TOP1_SHARE_THRESHOLD
    top3_share_threshold: float = DEFAULT_TOP3_SHARE_THRESHOLD
    negative_pnl_usd_threshold: float = DEFAULT_NEGATIVE_PNL_USD_THRESHOLD
    negative_pnl_bps_threshold: float = DEFAULT_NEGATIVE_PNL_BPS_THRESHOLD

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "artifacts_root": self.artifacts_root,
            "min_inputs_warning": self.min_inputs_warning,
            "negative_pnl_bps_threshold": self.negative_pnl_bps_threshold,
            "negative_pnl_usd_threshold": self.negative_pnl_usd_threshold,
            "top1_share_threshold": self.top1_share_threshold,
            "top3_share_threshold": self.top3_share_threshold,
            "window_from": self.window_from.isoformat() if self.window_from is not None else None,
            "window_to": self.window_to.isoformat() if self.window_to is not None else None,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_performance_from_portfolio_config(
        artifacts_root=args.artifacts_root,
        window_from=args.window_from,
        window_to=args.window_to,
    )
    run_performance_from_portfolio(
        portfolio_artifact_paths=_parse_portfolio_artifact_paths(args.portfolio_artifacts),
        config=config,
        nonce=args.nonce,
    )
    return 0


def run_performance_from_portfolio(
    *,
    portfolio_artifact_paths: list[Path],
    config: PerformanceFromPortfolioConfig,
    nonce: str | None = None,
) -> dict[str, Any]:
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    loaded_inputs = _load_portfolio_inputs(
        portfolio_artifact_paths,
        window_from=config.window_from,
        window_to=config.window_to,
    )
    if not loaded_inputs:
        raise ValueError("No portfolio artifacts matched the selected window.")

    input_paths = [path for path, _ in loaded_inputs]
    artifacts = [artifact for _, artifact in loaded_inputs]
    input_hashes = [_resolve_payload_hash(artifact) for artifact in artifacts]
    started_at = _resolve_started_at(artifacts)

    deterministic_nonce = nonce or canonical_hash(
        {
            "input_portfolio_hashes": input_hashes,
            "input_portfolio_paths": input_paths,
            "params": config.as_hash_dict(),
            "policy_version": PERFORMANCE_POLICY_VERSION,
        }
    )
    run_context = build_run_context(
        JOB_NAME,
        {
            "input_portfolio_hashes": input_hashes,
            "input_portfolio_paths": input_paths,
            "params": config.as_hash_dict(),
            "policy_version": PERFORMANCE_POLICY_VERSION,
        },
        started_at=started_at,
        nonce=deterministic_nonce,
    )
    _log(
        logger,
        logging.INFO,
        "performance_from_portfolio_started",
        run_context,
        n_inputs=len(artifacts),
        window_from=config.window_from.isoformat() if config.window_from is not None else None,
        window_to=config.window_to.isoformat() if config.window_to is not None else None,
    )

    per_run_metrics, aggregate_metrics, flags, warnings = compute_performance_metrics(
        artifacts,
        min_inputs_warning=config.min_inputs_warning,
        top1_share_threshold=config.top1_share_threshold,
        top3_share_threshold=config.top3_share_threshold,
        negative_pnl_usd_threshold=config.negative_pnl_usd_threshold,
        negative_pnl_bps_threshold=config.negative_pnl_bps_threshold,
    )
    input_refs = [
        {
            "portfolio_run_id": _require_text(artifact.get("run_id"), "run_id"),
            "portfolio_payload_hash": _resolve_payload_hash(artifact),
        }
        for artifact in artifacts
    ]

    artifact = build_performance_report_artifact(
        run_context=run_context,
        params=config.as_hash_dict(),
        inputs=input_refs,
        per_run_metrics=per_run_metrics,
        aggregate_metrics=aggregate_metrics,
        quality_flags=flags,
        quality_warnings=warnings,
        policy_version=PERFORMANCE_POLICY_VERSION,
    )
    errors = validate_performance_report_artifact(artifact)
    if errors:
        raise ValueError(f"Invalid performance-report artifact: {errors[0]}")

    artifact_path = _write_artifact(
        artifacts_root=config.artifacts_root,
        run_id=run_context.run_id,
        artifact=artifact,
    )
    _log(
        logger,
        logging.INFO,
        "performance_from_portfolio_completed",
        run_context,
        artifact_path=str(artifact_path),
        n_inputs=len(artifacts),
        n_per_run_metrics=len(per_run_metrics),
        n_quality_flags=len(flags),
        performance_policy_hash=str(artifact["performance_policy_hash"]),
        performance_inputs_hash=str(artifact["performance_inputs_hash"]),
        performance_payload_hash=str(artifact["performance_payload_hash"]),
    )
    artifact["artifact_path"] = str(artifact_path)
    return artifact


def load_performance_from_portfolio_config(
    *,
    artifacts_root: str | None,
    window_from: str | None,
    window_to: str | None,
) -> PerformanceFromPortfolioConfig:
    resolved_root = artifacts_root or os.getenv("PERFORMANCE_ARTIFACTS_ROOT") or "artifacts"
    parsed_from = _parse_optional_datetime(window_from)
    parsed_to = _parse_optional_datetime(window_to)
    if parsed_from is not None and parsed_to is not None and parsed_from > parsed_to:
        raise ValueError("window_from must be <= window_to")
    return PerformanceFromPortfolioConfig(
        artifacts_root=resolved_root,
        window_from=parsed_from,
        window_to=parsed_to,
    )


def _parse_portfolio_artifact_paths(raw_values: list[str]) -> list[Path]:
    parts: list[str] = []
    for raw in raw_values:
        for split_value in raw.split(","):
            value = split_value.strip()
            if value:
                parts.append(value)
    if not parts:
        raise ValueError("At least one portfolio artifact path is required")
    return [Path(value) for value in parts]


def _load_portfolio_inputs(
    paths: list[Path],
    *,
    window_from: datetime | None,
    window_to: datetime | None,
) -> list[tuple[str, dict[str, Any]]]:
    loaded: list[tuple[str, dict[str, Any]]] = []
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"Portfolio artifact not found: {path}")
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        if not isinstance(raw, dict):
            raise ValueError(f"Portfolio artifact root must be an object: {path}")
        errors = validate_portfolio_artifact(raw)
        if errors:
            raise ValueError(f"Invalid portfolio artifact {path}: {errors[0]}")

        generated_at = _parse_optional_datetime(_optional_text(raw.get("generated_at_utc")))
        if window_from is not None and generated_at is not None and generated_at < window_from:
            continue
        if window_to is not None and generated_at is not None and generated_at > window_to:
            continue

        loaded.append((str(path.resolve()), raw))
    loaded.sort(key=lambda item: (item[0], _optional_text(item[1].get("run_id")) or ""))
    return loaded


def _resolve_payload_hash(artifact: Mapping[str, Any]) -> str:
    value = _optional_text(artifact.get("portfolio_payload_hash"))
    if value is not None and _is_sha256(value):
        return value
    return canonical_hash(artifact)


def _resolve_started_at(artifacts: list[dict[str, Any]]) -> datetime:
    candidates: list[datetime] = []
    for artifact in artifacts:
        parsed = _parse_optional_datetime(_optional_text(artifact.get("generated_at_utc")))
        if parsed is not None:
            candidates.append(parsed)
    if candidates:
        return min(candidates)
    return datetime(1970, 1, 1, tzinfo=UTC)


def _write_artifact(*, artifacts_root: str, run_id: str, artifact: dict[str, Any]) -> Path:
    root = Path(artifacts_root) / "performance"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic performance report from portfolio artifacts."
    )
    parser.add_argument(
        "--portfolio-artifacts",
        action="append",
        required=True,
        help=(
            "Portfolio artifact path(s). Repeat the flag or pass comma-separated "
            "values in a single flag."
        ),
    )
    parser.add_argument(
        "--artifacts-root",
        default="artifacts",
        help="Artifacts root directory. Output is written under <root>/performance.",
    )
    parser.add_argument(
        "--window-from",
        default=None,
        help="Optional UTC lower bound filter for portfolio generated_at_utc (inclusive).",
    )
    parser.add_argument(
        "--window-to",
        default=None,
        help="Optional UTC upper bound filter for portfolio generated_at_utc (inclusive).",
    )
    parser.add_argument(
        "--nonce",
        default=None,
        help="Optional nonce override for deterministic run_id generation.",
    )
    return parser.parse_args(argv)


def _parse_optional_datetime(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    normalized = raw.strip().replace("Z", "+00:00")
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO datetime: {raw!r}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _require_text(raw: Any, path: str) -> str:
    value = _optional_text(raw)
    if value is None:
        raise ValueError(f"{path} must be a non-empty string")
    return value


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _log(
    logger: logging.Logger,
    level: int,
    message: str,
    run_context: RunContext,
    **extra_fields: Any,
) -> None:
    payload: dict[str, Any] = dict(run_context.as_log_context())
    payload["extra_fields"] = extra_fields
    logger.log(level, message, extra=payload)


if __name__ == "__main__":
    raise SystemExit(main())
