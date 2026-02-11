from __future__ import annotations

import argparse
import json
import logging
import os
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.db.selector_repository import SelectorRepository
from pmx.selector.baselines import deterministic_seed, select_random_stratified, select_top_volume
from pmx.selector.compute import CandidateScore, build_candidate_set, compute_scores
from pmx.selector.constraints import DEFAULT_TARGET_BUCKET_MIX, enforce_constraints
from pmx.selector.spec import SelectorConfig

JOB_NAME = "select_markets"
SELECTOR_VERSION = "selector_v1"
BASELINE_TOP_VOLUME_VERSION = "baseline_top_volume"
BASELINE_RANDOM_STRATIFIED_VERSION = "baseline_random_stratified"


@dataclass(frozen=True, slots=True)
class SelectMarketsConfig:
    ingest_epsilon_seconds: int
    max_candidates: int
    k_deep: int
    feature_set: str
    max_per_category: int
    max_per_group: int
    target_bucket_mix: dict[str, float]
    artifacts_root: str
    selector_config: SelectorConfig

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
            "max_candidates": self.max_candidates,
            "k_deep": self.k_deep,
            "feature_set": self.feature_set,
            "max_per_category": self.max_per_category,
            "max_per_group": self.max_per_group,
            "target_bucket_mix": self.target_bucket_mix,
            "selector_config": {
                "lq_threshold": self.selector_config.lq_threshold,
                "max_spread_bps_hard": self.selector_config.max_spread_bps_hard,
                "min_top_depth_hard": self.selector_config.min_top_depth_hard,
                "stale_seconds_hard": self.selector_config.stale_seconds_hard,
            },
            "artifacts_root": self.artifacts_root,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    decision_ts = _parse_optional_datetime_arg(args.decision_ts) or datetime.now(tz=UTC)
    config = load_select_markets_config(
        epsilon_seconds=args.epsilon_seconds,
        max_candidates=args.max_candidates,
        k_deep=args.k_deep,
    )
    run_select_markets(
        decision_ts=decision_ts,
        config=config,
    )
    return 0


def run_select_markets(
    *,
    decision_ts: datetime,
    config: SelectMarketsConfig,
) -> dict[str, Any]:
    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("Missing DB URL: set DATABASE_URL or APP_DATABASE_URL")

    decision = _as_utc_datetime(decision_ts)
    run_context = build_run_context(JOB_NAME, config.as_hash_dict(), started_at=decision)
    run_uuid = UUID(run_context.run_id)
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")

    _log(
        logger,
        logging.INFO,
        "select_markets_started",
        run_context,
        decision_ts=decision.isoformat(),
        max_candidates=config.max_candidates,
        k_deep=config.k_deep,
    )

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        repository = SelectorRepository(connection)
        repository.insert_run(
            run_id=run_uuid,
            run_type="select_markets",
            decision_ts=decision,
            ingest_epsilon_seconds=config.ingest_epsilon_seconds,
            code_version=run_context.code_version,
            config_hash=run_context.config_hash,
        )

        candidates = build_candidate_set(
            connection,
            decision,
            config.ingest_epsilon_seconds,
            max_candidates=config.max_candidates,
            feature_set=config.feature_set,
            selector_config=config.selector_config,
        )
        scored = compute_scores(
            connection,
            candidates,
            decision,
            config.ingest_epsilon_seconds,
            feature_set=config.feature_set,
            selector_config=config.selector_config,
        )

        constrained = enforce_constraints(
            scored,
            k=config.k_deep,
            target_bucket_mix=config.target_bucket_mix,
            max_per_category=config.max_per_category,
            max_per_group=config.max_per_group,
        )
        selected_main = list(constrained.selected)

        selected_top_volume = select_top_volume(scored, k=config.k_deep)
        seed = deterministic_seed(decision, run_context.config_hash)
        selected_random = select_random_stratified(scored, k=config.k_deep, seed=seed)

        run_main = repository.create_selection_run(
            run_id=run_uuid,
            stage="deep_dive",
            universe_size=len(scored),
            selected_size=len(selected_main),
            selector_version=SELECTOR_VERSION,
            params={
                "decision_ts": decision.isoformat(),
                "feature_set": config.feature_set,
                "target_bucket_mix": config.target_bucket_mix,
                "max_per_category": config.max_per_category,
                "max_per_group": config.max_per_group,
            },
        )
        for candidate in scored:
            repository.upsert_candidate_scores(
                selection_run_id=run_main,
                market_id=candidate.market_id,
                token_id=candidate.token_id,
                screen_score=candidate.screen_score,
                components_json=candidate.components,
                flags_json={
                    "flags": list(candidate.flags),
                    "penalties": candidate.penalties,
                    "include_reasons": list(candidate.include_reasons),
                },
                ttr_bucket=candidate.ttr_bucket,
                category=candidate.category,
                group_id=candidate.group_id,
            )
        _persist_selected(
            repository,
            selection_run_id=run_main,
            selector_version=SELECTOR_VERSION,
            selected=selected_main,
        )

        run_top = repository.create_selection_run(
            run_id=run_uuid,
            stage="deep_dive_baseline",
            universe_size=len(scored),
            selected_size=len(selected_top_volume),
            selector_version=BASELINE_TOP_VOLUME_VERSION,
            params={"decision_ts": decision.isoformat(), "baseline": BASELINE_TOP_VOLUME_VERSION},
        )
        _persist_selected(
            repository,
            selection_run_id=run_top,
            selector_version=BASELINE_TOP_VOLUME_VERSION,
            selected=selected_top_volume,
        )

        run_random = repository.create_selection_run(
            run_id=run_uuid,
            stage="deep_dive_baseline",
            universe_size=len(scored),
            selected_size=len(selected_random),
            selector_version=BASELINE_RANDOM_STRATIFIED_VERSION,
            params={
                "decision_ts": decision.isoformat(),
                "baseline": BASELINE_RANDOM_STRATIFIED_VERSION,
                "seed": seed,
            },
        )
        _persist_selected(
            repository,
            selection_run_id=run_random,
            selector_version=BASELINE_RANDOM_STRATIFIED_VERSION,
            selected=selected_random,
        )

        fallback_path = repository.write_candidate_fallback_artifact(
            run_id=run_context.run_id,
            artifacts_root=os.path.join(config.artifacts_root, "selector"),
        )

        summary = {
            "run_id": run_context.run_id,
            "decision_ts": decision.isoformat(),
            "selector_version": SELECTOR_VERSION,
            "counts": {
                "candidates": len(candidates),
                "scored": len(scored),
                "selected_main": len(selected_main),
                "selected_top_volume": len(selected_top_volume),
                "selected_random": len(selected_random),
            },
            "input_hashes": {
                "candidates_hash": _hash_records([_candidate_record(item) for item in candidates]),
                "scored_hash": _hash_records([item.as_dict() for item in scored]),
                "selected_selector_v1_hash": _hash_records(
                    [item.as_dict() for item in selected_main]
                ),
                "selected_baseline_top_volume_hash": _hash_records(
                    [item.as_dict() for item in selected_top_volume]
                ),
                "selected_baseline_random_hash": _hash_records(
                    [item.as_dict() for item in selected_random]
                ),
            },
            "bucket_counts": dict(
                sorted(Counter(item.ttr_bucket for item in selected_main).items())
            ),
            "category_counts": dict(
                sorted(Counter(item.category for item in selected_main).items())
            ),
            "flag_counts": dict(
                sorted(
                    Counter(flag for item in scored for flag in item.flags).items(),
                )
            ),
            "selection_runs": {
                "selector_v1": run_main,
                "baseline_top_volume": run_top,
                "baseline_random_stratified": run_random,
            },
            "candidate_fallback_artifact": str(fallback_path) if fallback_path else None,
            "config_hash": run_context.config_hash,
            "code_version": run_context.code_version,
        }

    coerced_fields = _coerce_mapping(summary["counts"])
    _log(
        logger,
        logging.INFO,
        "select_markets_completed",
        run_context,
        **coerced_fields,
        bucket_counts=summary["bucket_counts"],
        category_counts=summary["category_counts"],
        flag_counts=summary["flag_counts"],
    )
    _write_job_artifact(config.artifacts_root, run_context.run_id, summary)
    return summary


def load_select_markets_config(
    *,
    epsilon_seconds: int | None,
    max_candidates: int | None,
    k_deep: int | None,
) -> SelectMarketsConfig:
    epsilon = (
        epsilon_seconds
        if epsilon_seconds is not None
        else _load_positive_int("INGEST_EPSILON_SECONDS", 300)
    )
    max_candidates_value = (
        max_candidates
        if max_candidates is not None
        else _load_positive_int("SELECTOR_MAX_CANDIDATES", 1500)
    )
    k_value = k_deep if k_deep is not None else _load_positive_int("SELECTOR_K_DEEP", 200)
    if max_candidates_value <= 0 or k_value <= 0:
        raise ValueError("max_candidates and k_deep must be > 0")

    return SelectMarketsConfig(
        ingest_epsilon_seconds=epsilon,
        max_candidates=max_candidates_value,
        k_deep=k_value,
        feature_set=os.getenv("SELECTOR_FEATURE_SET", "micro_v1"),
        max_per_category=_load_positive_int("SELECTOR_MAX_PER_CATEGORY", 45),
        max_per_group=_load_positive_int("SELECTOR_MAX_PER_GROUP", 20),
        target_bucket_mix=dict(DEFAULT_TARGET_BUCKET_MIX),
        artifacts_root=os.getenv("SELECTOR_ARTIFACTS_ROOT", "artifacts/selector"),
        selector_config=SelectorConfig(
            lq_threshold=_load_positive_float("SELECTOR_LQ_THRESHOLD", 0.35),
            max_spread_bps_hard=_load_positive_float("SELECTOR_MAX_SPREAD_BPS_HARD", 1500.0),
            min_top_depth_hard=_load_positive_float("SELECTOR_MIN_TOP_DEPTH_HARD", 1.0),
            stale_seconds_hard=_load_positive_float("SELECTOR_STALE_SECONDS_HARD", 14_400.0),
        ),
    )


def _persist_selected(
    repository: SelectorRepository,
    *,
    selection_run_id: int,
    selector_version: str,
    selected: list[CandidateScore],
) -> None:
    for rank, item in enumerate(selected, start=1):
        repository.insert_selected(
            selection_run_id=selection_run_id,
            selector_version=selector_version,
            rank=rank,
            market_id=item.market_id,
            token_id=item.token_id,
            score=item.screen_score,
            reason_json={
                "ttr_bucket": item.ttr_bucket,
                "category": item.category,
                "group_id": item.group_id,
                "components": item.components,
                "flags": list(item.flags),
                "include_reasons": list(item.include_reasons),
            },
        )


def _write_job_artifact(artifacts_root: str, run_id: str, payload: dict[str, Any]) -> Path:
    root = Path(artifacts_root)
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _hash_records(records: list[dict[str, Any]]) -> str:
    serialized = json.dumps(records, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    import hashlib

    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _candidate_record(item: Any) -> dict[str, Any]:
    return {
        "market_id": item.market_id,
        "token_id": item.token_id,
        "category": item.category,
        "group_id": item.group_id,
        "ttr_bucket": item.ttr_bucket,
        "volume_24h": item.volume_24h,
        "include_reasons": list(item.include_reasons),
    }


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run selector v1 market deep-dive selection.")
    parser.add_argument(
        "--decision-ts", default=None, help="ISO decision timestamp (default now UTC)."
    )
    parser.add_argument(
        "--epsilon-seconds", type=int, default=None, help="As-of ingest epsilon seconds."
    )
    parser.add_argument(
        "--max-candidates", type=int, default=None, help="Maximum candidate universe size."
    )
    parser.add_argument("--k-deep", type=int, default=None, help="Deep-dive selected market count.")
    return parser.parse_args(argv)


def _parse_optional_datetime_arg(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid --decision-ts value: {raw!r}") from exc
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


def _load_positive_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a float, got {raw!r}") from exc
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


def _coerce_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return {}


if __name__ == "__main__":
    raise SystemExit(main())
