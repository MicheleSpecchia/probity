from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.selector.evaluate import DEFAULT_SELECTOR_VERSIONS, evaluate_selector_runs

JOB_NAME = "eval_selector"


@dataclass(frozen=True, slots=True)
class EvalSelectorConfig:
    feature_set: str
    ingest_epsilon_seconds: int
    window_hours: int
    artifacts_root: str
    selector_versions: tuple[str, ...]

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "feature_set": self.feature_set,
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
            "window_hours": self.window_hours,
            "artifacts_root": self.artifacts_root,
            "selector_versions": list(self.selector_versions),
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    decision_ts = _parse_required_datetime_arg(args.decision_ts)
    config = load_eval_selector_config(
        feature_set=args.feature_set,
        epsilon_seconds=args.epsilon_seconds,
        window_hours=args.window_hours,
        artifacts_root=args.artifacts_root,
    )
    run_eval_selector(
        decision_ts=decision_ts,
        config=config,
    )
    return 0


def run_eval_selector(
    *,
    decision_ts: datetime,
    config: EvalSelectorConfig,
) -> dict[str, Any]:
    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("Missing DB URL: set DATABASE_URL or APP_DATABASE_URL")

    decision = _as_utc_datetime(decision_ts)
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    run_context = build_run_context(
        JOB_NAME,
        {
            **config.as_hash_dict(),
            "decision_ts": decision.isoformat(),
        },
        started_at=decision,
    )
    run_uuid = UUID(run_context.run_id)

    _log(
        logger,
        logging.INFO,
        "eval_selector_started",
        run_context,
        decision_ts=decision.isoformat(),
        window_hours=config.window_hours,
    )

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        _insert_run(
            connection,
            run_id=run_uuid,
            decision_ts=decision,
            ingest_epsilon_seconds=config.ingest_epsilon_seconds,
            code_version=run_context.code_version,
            config_hash=run_context.config_hash,
        )
        report = evaluate_selector_runs(
            connection,
            decision_ts=decision,
            epsilon_s=config.ingest_epsilon_seconds,
            window_hours=config.window_hours,
            feature_set=config.feature_set,
            selector_versions=config.selector_versions,
            selection_run_ids=None,
        )

    artifact = {
        "run_id": run_context.run_id,
        "job_name": JOB_NAME,
        "decision_ts": decision.isoformat(),
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "report": report,
    }
    artifact_path = _write_artifact(config.artifacts_root, run_context.run_id, artifact)

    selector_summary = {
        version: payload.get("counts", {}).get("examples", 0)
        for version, payload in sorted(report["selectors"].items())
    }
    _log(
        logger,
        logging.INFO,
        "eval_selector_completed",
        run_context,
        artifact_path=str(artifact_path),
        selector_example_counts=selector_summary,
    )
    return artifact


def load_eval_selector_config(
    *,
    feature_set: str | None,
    epsilon_seconds: int | None,
    window_hours: int | None,
    artifacts_root: str | None,
) -> EvalSelectorConfig:
    resolved_feature_set = feature_set or os.getenv("BACKTEST_FEATURE_SET") or "micro_v1"
    resolved_epsilon = (
        epsilon_seconds
        if epsilon_seconds is not None
        else _load_positive_int("INGEST_EPSILON_SECONDS", 300)
    )
    resolved_window = (
        window_hours
        if window_hours is not None
        else _load_positive_int("SELECTOR_EVAL_WINDOW_HOURS", 72)
    )
    resolved_artifacts = (
        artifacts_root or os.getenv("SELECTOR_EVAL_ARTIFACTS_ROOT") or "artifacts/selector_eval"
    )
    return EvalSelectorConfig(
        feature_set=resolved_feature_set,
        ingest_epsilon_seconds=resolved_epsilon,
        window_hours=resolved_window,
        artifacts_root=resolved_artifacts,
        selector_versions=DEFAULT_SELECTOR_VERSIONS,
    )


def _insert_run(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    decision_ts: datetime,
    ingest_epsilon_seconds: int,
    code_version: str,
    config_hash: str,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO runs (
                run_id,
                run_type,
                decision_ts,
                ingest_epsilon_seconds,
                code_version,
                config_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                JOB_NAME,
                _as_utc_datetime(decision_ts),
                ingest_epsilon_seconds,
                code_version,
                config_hash,
            ),
        )


def _write_artifact(artifacts_root: str, run_id: str, payload: dict[str, Any]) -> Path:
    root = Path(artifacts_root)
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate selector vs baselines on backtest metrics."
    )
    parser.add_argument(
        "--decision-ts", required=True, help="Evaluation anchor decision timestamp."
    )
    parser.add_argument(
        "--epsilon-seconds",
        type=int,
        default=None,
        help="As-of ingest epsilon seconds.",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=72,
        help="Backtest lookback window in hours.",
    )
    parser.add_argument("--feature-set", default="micro_v1", help="Feature set id.")
    parser.add_argument(
        "--artifacts-root",
        default=None,
        help="Selector evaluation artifact directory (default artifacts/selector_eval).",
    )
    return parser.parse_args(argv)


def _parse_required_datetime_arg(raw: str) -> datetime:
    text = raw.strip()
    if not text:
        raise ValueError("--decision-ts cannot be empty")
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid --decision-ts value: {raw!r}") from exc
    return _as_utc_datetime(parsed)


def _load_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be > 0, got {parsed}")
    return parsed


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


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
