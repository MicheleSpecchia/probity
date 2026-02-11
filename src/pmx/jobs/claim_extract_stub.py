from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.claims.audit import build_audit_bundle, compute_prompt_hash, write_audit_bundle
from pmx.claims.extractor import (
    build_prompt,
    run_extract_stub,
    validate_and_normalize,
)
from pmx.claims.schemas import CLAIM_EXTRACT_SCHEMA_VERSION, EVIDENCE_CHECKLIST_SCHEMA_VERSION

JOB_NAME = "claim_extract_stub"


@dataclass(frozen=True, slots=True)
class ClaimExtractStubConfig:
    ingest_epsilon_seconds: int
    artifacts_root: str
    claim_schema_version: str
    evidence_schema_version: str

    def as_hash_dict(self) -> dict[str, int | str]:
        return {
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
            "artifacts_root": self.artifacts_root,
            "claim_schema_version": self.claim_schema_version,
            "evidence_schema_version": self.evidence_schema_version,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_claim_extract_stub_config(artifacts_root=args.artifacts_root)
    decision_ts = _parse_optional_datetime_arg(args.decision_ts) or datetime.now(tz=UTC)
    articles = _load_articles(args.articles_json)

    run_claim_extract_stub(
        config=config,
        market_id=args.market_id,
        articles=articles,
        decision_ts=decision_ts,
        stub_output_path=args.stub_output_json,
    )
    return 0


def run_claim_extract_stub(
    *,
    config: ClaimExtractStubConfig,
    market_id: str,
    articles: Sequence[Mapping[str, Any]],
    decision_ts: datetime,
    stub_output_path: str | None = None,
    nonce: str | None = None,
) -> dict[str, Any]:
    started_at = _as_utc_datetime(decision_ts)
    run_context = build_run_context(
        JOB_NAME,
        {
            **config.as_hash_dict(),
            "market_id": market_id,
            "article_ids": _sorted_article_ids(articles),
            "stub_output_path": stub_output_path or "",
        },
        started_at=started_at,
        nonce=nonce,
    )
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    _log(
        logger,
        logging.INFO,
        "claim_extract_stub_started",
        run_context,
        market_id=market_id,
        article_count=len(articles),
    )

    prompt = build_prompt(
        market_id=market_id,
        articles=articles,
        schema_version=config.claim_schema_version,
    )
    prompt_hash = compute_prompt_hash(prompt)
    raw_payload = run_extract_stub(
        market_id=market_id,
        articles=articles,
        generated_at=started_at,
        schema_version=config.claim_schema_version,
        fixture_path=stub_output_path,
    )
    outcome = validate_and_normalize(
        raw_payload,
        market_id=market_id,
        schema_version=config.claim_schema_version,
    )
    bundle = build_audit_bundle(
        run_context=run_context,
        market_id=market_id,
        decision_ts=started_at,
        ingest_epsilon_seconds=config.ingest_epsilon_seconds,
        claim_schema_version=config.claim_schema_version,
        evidence_schema_version=config.evidence_schema_version,
        prompt_hash=prompt_hash,
        articles=articles,
        payload=outcome.payload,
        validator_errors=outcome.validator_errors,
        no_trade_flags=outcome.no_trade_flags,
    )
    artifact_path = write_audit_bundle(
        bundle=bundle,
        artifacts_root=config.artifacts_root,
        run_id=run_context.run_id,
        market_id=market_id,
    )

    _log(
        logger,
        logging.INFO,
        "claim_extract_stub_completed",
        run_context,
        market_id=market_id,
        artifact_path=str(artifact_path),
        used_fallback=outcome.used_fallback,
        validator_error_count=len(outcome.validator_errors),
    )
    return {
        "artifact_path": str(artifact_path),
        "run_id": run_context.run_id,
        "used_fallback": int(outcome.used_fallback),
        "validator_error_count": len(outcome.validator_errors),
        "claim_count": _claim_count(outcome.payload),
    }


def load_claim_extract_stub_config(*, artifacts_root: str | None = None) -> ClaimExtractStubConfig:
    root: str = (
        artifacts_root or os.getenv("CLAIM_EXTRACT_ARTIFACTS_ROOT") or "artifacts/claim_extract"
    )
    return ClaimExtractStubConfig(
        ingest_epsilon_seconds=_load_positive_int("INGEST_EPSILON_SECONDS", 300),
        artifacts_root=root,
        claim_schema_version=CLAIM_EXTRACT_SCHEMA_VERSION,
        evidence_schema_version=EVIDENCE_CHECKLIST_SCHEMA_VERSION,
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic stub claim extraction.")
    parser.add_argument("--market-id", required=True, help="Target market identifier.")
    parser.add_argument(
        "--articles-json",
        required=True,
        help="Path to JSON file containing article objects.",
    )
    parser.add_argument(
        "--stub-output-json",
        default=None,
        help="Optional path to a stub LLM output fixture (JSON object).",
    )
    parser.add_argument(
        "--decision-ts",
        default=None,
        help="Optional ISO datetime for deterministic run timestamp.",
    )
    parser.add_argument(
        "--artifacts-root",
        default=None,
        help="Artifact root directory (default: artifacts).",
    )
    return parser.parse_args(argv)


def _load_articles(path: str) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as handle:
        loaded = json.load(handle)
    if not isinstance(loaded, list):
        raise ValueError("--articles-json must contain a JSON array")
    articles: list[dict[str, Any]] = []
    for item in loaded:
        if isinstance(item, Mapping):
            articles.append({str(key): value for key, value in item.items()})
    return articles


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


def _sorted_article_ids(articles: Sequence[Mapping[str, Any]]) -> list[int]:
    ids: list[int] = []
    for article in articles:
        raw_id = article.get("article_id")
        if raw_id is None:
            continue
        if isinstance(raw_id, bool):
            continue
        if isinstance(raw_id, int):
            ids.append(raw_id)
            continue
        if not isinstance(raw_id, str):
            continue
        text = raw_id.strip()
        if not text:
            continue
        try:
            parsed = int(text)
        except ValueError:
            continue
        ids.append(parsed)
    return sorted(ids)


def _claim_count(payload: Mapping[str, Any]) -> int:
    claims = payload.get("claims")
    return len(claims) if isinstance(claims, list) else 0


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
