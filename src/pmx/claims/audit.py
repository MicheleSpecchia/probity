from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from pmx.audit.run_context import RunContext
from pmx.claims.extractor import normalize_articles_for_prompt, validation_issue_to_dict
from pmx.claims.validate import ValidationIssue
from pmx.news.normalize import canonicalize_json

_SAFE_COMPONENT_RE = re.compile(r"[^A-Za-z0-9._-]+")


def compute_prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()


def build_audit_bundle(
    *,
    run_context: RunContext,
    market_id: str,
    decision_ts: datetime,
    ingest_epsilon_seconds: int,
    claim_schema_version: str,
    evidence_schema_version: str,
    prompt_hash: str,
    articles: Sequence[Mapping[str, Any]],
    payload: Mapping[str, Any],
    validator_errors: Sequence[ValidationIssue],
    no_trade_flags: Sequence[str],
) -> dict[str, Any]:
    normalized_articles = normalize_articles_for_prompt(articles)
    article_ids = [int(row["article_id"]) for row in normalized_articles]
    canonical_urls = [str(row["canonical_url"]) for row in normalized_articles]
    errors_payload = [validation_issue_to_dict(issue) for issue in validator_errors]

    bundle = canonicalize_json(
        {
            "artifact_type": "claim_extract_audit_bundle.v1",
            "run_id": run_context.run_id,
            "job_name": run_context.job_name,
            "code_version": run_context.code_version,
            "config_hash": run_context.config_hash,
            "decision_ts": _as_utc_datetime(decision_ts).isoformat(),
            "ingest_epsilon_seconds": int(ingest_epsilon_seconds),
            "schema_versions": {
                "claim_extract": claim_schema_version,
                "evidence_checklist": evidence_schema_version,
            },
            "prompt_hash": prompt_hash,
            "input_article_ids": article_ids,
            "input_canonical_urls": canonical_urls,
            "validator_errors": errors_payload,
            "no_trade_flags": sorted({str(flag) for flag in no_trade_flags}),
            "payload": payload,
        }
    )
    if not isinstance(bundle, dict):
        raise ValueError("Audit bundle canonicalization failed")
    return cast(dict[str, Any], bundle)


def write_audit_bundle(
    *,
    bundle: Mapping[str, Any],
    artifacts_root: str | Path,
    run_id: str,
    market_id: str,
) -> Path:
    root = Path(artifacts_root)
    output_dir = root / "claim_extract"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{_safe_filename(run_id)}_{_safe_filename(market_id)}.json"

    encoded = json.dumps(
        canonicalize_json(bundle),
        separators=(",", ":"),
        sort_keys=True,
        ensure_ascii=True,
    )
    output_path.write_text(encoded, encoding="utf-8")
    return output_path


def _safe_filename(value: str) -> str:
    cleaned = _SAFE_COMPONENT_RE.sub("_", value.strip())
    return cleaned or "unknown"


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
