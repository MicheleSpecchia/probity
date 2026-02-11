from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.backtest.asof_dataset import Example, build_asof_dataset
from pmx.backtest.metrics import aggregate_metrics
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.models.baselines import baseline_a_price, baseline_b_micro

JOB_NAME = "backtest_baselines"


@dataclass(frozen=True, slots=True)
class BacktestBaselinesConfig:
    feature_set: str
    ingest_epsilon_seconds: int
    artifacts_root: str

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "feature_set": self.feature_set,
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
            "artifacts_root": self.artifacts_root,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_backtest_baselines_config(
        feature_set=args.feature_set,
        epsilon_seconds=args.epsilon_seconds,
        artifacts_root=args.artifacts_root,
    )
    from_ts = _parse_required_datetime_arg(args.from_ts, "--from")
    to_ts = _parse_required_datetime_arg(args.to_ts, "--to")
    if to_ts < from_ts:
        raise ValueError("--to must be >= --from")
    if args.step_hours <= 0:
        raise ValueError("--step-hours must be > 0")

    run_backtest_baselines(
        config=config,
        token_ids=_parse_token_ids_arg(args.token_ids),
        max_tokens=args.max_tokens,
        from_ts=from_ts,
        to_ts=to_ts,
        step_hours=args.step_hours,
    )
    return 0


def run_backtest_baselines(
    *,
    config: BacktestBaselinesConfig,
    token_ids: list[str] | None,
    max_tokens: int | None,
    from_ts: datetime,
    to_ts: datetime,
    step_hours: int,
) -> dict[str, Any]:
    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("Missing DB URL: set DATABASE_URL or APP_DATABASE_URL")
    if max_tokens is not None and max_tokens <= 0:
        raise ValueError("--max-tokens must be > 0")

    from_utc = _as_utc_datetime(from_ts)
    to_utc = _as_utc_datetime(to_ts)
    decision_ts_list = _build_decision_grid(from_utc, to_utc, step_hours=step_hours)

    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    run_context = build_run_context(
        JOB_NAME,
        {
            **config.as_hash_dict(),
            "token_ids": token_ids or [],
            "max_tokens": max_tokens,
            "from_ts": from_utc.isoformat(),
            "to_ts": to_utc.isoformat(),
            "step_hours": step_hours,
        },
        started_at=from_utc,
    )
    run_uuid = UUID(run_context.run_id)
    _log(
        logger,
        logging.INFO,
        "backtest_baselines_started",
        run_context,
        from_ts=from_utc.isoformat(),
        to_ts=to_utc.isoformat(),
        step_hours=step_hours,
    )

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        _insert_run(
            connection,
            run_id=run_uuid,
            run_type="backtest_baselines",
            decision_ts=from_utc,
            ingest_epsilon_seconds=config.ingest_epsilon_seconds,
            code_version=run_context.code_version,
            config_hash=run_context.config_hash,
        )

        selected_tokens = _resolve_tokens(
            connection,
            explicit_token_ids=token_ids,
            max_tokens=max_tokens,
        )
        dataset = build_asof_dataset(
            connection,
            token_ids=selected_tokens,
            decision_ts_list=decision_ts_list,
            epsilon_s=config.ingest_epsilon_seconds,
            feature_set=config.feature_set,
            outcome_provider=None,
        )

    dataset_hash = _dataset_hash(dataset.examples)
    aggregate, per_token = _evaluate_dataset(dataset.examples)
    artifact = {
        "run_id": run_context.run_id,
        "job_name": JOB_NAME,
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "feature_set": config.feature_set,
        "dataset_hash": dataset_hash,
        "from_ts": from_utc.isoformat(),
        "to_ts": to_utc.isoformat(),
        "step_hours": step_hours,
        "counts": {
            "examples": len(dataset.examples),
            "skipped_no_outcome": dataset.skipped_no_outcome,
            "skipped_missing_features": dataset.skipped_missing_features,
            "skipped_missing_price": dataset.skipped_missing_price,
            "tokens_selected": len(selected_tokens),
        },
        "aggregate_metrics": aggregate,
        "per_token_metrics": per_token,
    }
    artifact_path = _write_artifact(config.artifacts_root, run_context.run_id, artifact)
    _log(
        logger,
        logging.INFO,
        "backtest_baselines_completed",
        run_context,
        artifact_path=str(artifact_path),
        examples=len(dataset.examples),
    )
    return artifact


def load_backtest_baselines_config(
    *,
    feature_set: str | None,
    epsilon_seconds: int | None,
    artifacts_root: str | None,
) -> BacktestBaselinesConfig:
    resolved_feature_set = feature_set or os.getenv("BACKTEST_FEATURE_SET") or "micro_v1"
    if epsilon_seconds is not None:
        if epsilon_seconds <= 0:
            raise ValueError("--epsilon-seconds must be > 0")
        resolved_epsilon = epsilon_seconds
    else:
        resolved_epsilon = _load_positive_int("INGEST_EPSILON_SECONDS", 300)
    resolved_artifacts = (
        artifacts_root or os.getenv("BACKTEST_ARTIFACTS_ROOT") or "artifacts/backtests"
    )
    return BacktestBaselinesConfig(
        feature_set=resolved_feature_set,
        ingest_epsilon_seconds=resolved_epsilon,
        artifacts_root=resolved_artifacts,
    )


def _evaluate_dataset(examples: tuple[Example, ...]) -> tuple[dict[str, Any], dict[str, Any]]:
    y_all: list[int] = []
    pred_a_all: list[float] = []
    pred_b_all: list[float] = []

    per_token_rows: dict[str, _TokenPredictions] = {}
    for example in examples:
        pred_a = baseline_a_price(example.price_prob)
        pred_b = baseline_b_micro(example.features_json)
        y_all.append(example.outcome_y)
        pred_a_all.append(pred_a)
        pred_b_all.append(pred_b)

        bucket = per_token_rows.setdefault(example.token_id, _TokenPredictions())
        bucket.y.append(example.outcome_y)
        bucket.a.append(pred_a)
        bucket.b.append(pred_b)

    aggregate = {
        "baseline_a": aggregate_metrics(y_all, pred_a_all),
        "baseline_b": aggregate_metrics(y_all, pred_b_all),
    }

    per_token: dict[str, Any] = {}
    for token_id in sorted(per_token_rows.keys()):
        row = per_token_rows[token_id]
        per_token[token_id] = {
            "baseline_a": aggregate_metrics(row.y, row.a),
            "baseline_b": aggregate_metrics(row.y, row.b),
        }

    return aggregate, per_token


def _dataset_hash(examples: tuple[Example, ...]) -> str:
    payload = [
        {
            "token_id": example.token_id,
            "market_id": example.market_id,
            "decision_ts": example.decision_ts.isoformat(),
            "price_prob": round(example.price_prob, 8),
            "outcome_y": example.outcome_y,
            "features_json": example.features_json,
        }
        for example in examples
    ]
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class _TokenPredictions:
    y: list[int]
    a: list[float]
    b: list[float]

    def __init__(self) -> None:
        self.y = []
        self.a = []
        self.b = []


def _write_artifact(artifacts_root: str, run_id: str, artifact: dict[str, Any]) -> Path:
    root = Path(artifacts_root)
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _resolve_tokens(
    conn: psycopg.Connection,
    *,
    explicit_token_ids: list[str] | None,
    max_tokens: int | None,
) -> list[str]:
    if explicit_token_ids:
        return sorted({token for token in explicit_token_ids if token})

    if max_tokens is None:
        query = "SELECT token_id FROM market_tokens ORDER BY token_id ASC"
        params: tuple[object, ...] = ()
    else:
        query = "SELECT token_id FROM market_tokens ORDER BY token_id ASC LIMIT %s"
        params = (max_tokens,)

    with conn.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [str(row[0]) for row in rows]


def _insert_run(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    run_type: str,
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
                run_type,
                _as_utc_datetime(decision_ts),
                ingest_epsilon_seconds,
                code_version,
                config_hash,
            ),
        )


def _build_decision_grid(from_ts: datetime, to_ts: datetime, *, step_hours: int) -> list[datetime]:
    out: list[datetime] = []
    current = from_ts
    step = timedelta(hours=step_hours)
    while current <= to_ts:
        out.append(current)
        current = current + step
    return out


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run as-of backtest for baseline models A/B.")
    parser.add_argument("--token-ids", default=None, help="Comma-separated token ids.")
    parser.add_argument(
        "--max-tokens", type=int, default=None, help="Optional max selected tokens."
    )
    parser.add_argument(
        "--from", dest="from_ts", required=True, help="Backtest start ISO timestamp."
    )
    parser.add_argument("--to", dest="to_ts", required=True, help="Backtest end ISO timestamp.")
    parser.add_argument("--step-hours", type=int, default=4, help="Walk-forward step in hours.")
    parser.add_argument(
        "--epsilon-seconds", type=int, default=None, help="As-of ingest epsilon seconds."
    )
    parser.add_argument("--feature-set", default="micro_v1", help="Feature set id.")
    parser.add_argument(
        "--artifacts-root",
        default=None,
        help="Backtest artifact directory (default artifacts/backtests).",
    )
    return parser.parse_args(argv)


def _parse_token_ids_arg(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    values = [item.strip() for item in raw.split(",")]
    deduped = sorted({item for item in values if item})
    return deduped or None


def _parse_required_datetime_arg(raw: str, flag: str) -> datetime:
    text = raw.strip()
    if not text:
        raise ValueError(f"{flag} cannot be empty")
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid {flag} value: {raw!r}") from exc
    return _as_utc_datetime(parsed)


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


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
