from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from pmx.claims.schemas import CLAIM_EXTRACT_SCHEMA_VERSION
from pmx.claims.validate import PayloadValidationError, ValidationIssue, validate_claim_extract
from pmx.news.normalize import canonicalize_json

INVALID_OUTPUT_FLAG = "llm_invalid_output"
_FALLBACK_GENERATED_AT = "1970-01-01T00:00:00+00:00"


@dataclass(frozen=True, slots=True)
class ExtractionOutcome:
    payload: dict[str, Any]
    validator_errors: tuple[ValidationIssue, ...]
    no_trade_flags: tuple[str, ...]
    used_fallback: bool


def build_prompt(
    market_id: str,
    articles: Sequence[Mapping[str, Any]],
    schema_version: str = CLAIM_EXTRACT_SCHEMA_VERSION,
) -> str:
    normalized_articles = normalize_articles_for_prompt(articles)
    prompt_payload = canonicalize_json(
        {
            "task": "claim_extract",
            "schema_version": schema_version,
            "market_id": market_id,
            "articles": normalized_articles,
        }
    )
    prompt_json = json.dumps(
        prompt_payload,
        separators=(",", ":"),
        sort_keys=True,
        ensure_ascii=True,
    )
    return (
        "PMX_CLAIM_EXTRACT_STUB\n"
        "Return JSON compatible with the provided schema_version.\n"
        f"input={prompt_json}"
    )


def normalize_articles_for_prompt(articles: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for raw in articles:
        article_id = _optional_int(raw.get("article_id"))
        if article_id is None:
            continue
        canonical_url = _as_text(raw.get("canonical_url")) or _as_text(raw.get("url")) or ""
        published_at = _optional_datetime(raw.get("published_at"))
        title = _as_text(raw.get("title")) or ""
        body = _as_text(raw.get("body")) or _as_text(raw.get("summary")) or ""
        normalized.append(
            {
                "article_id": article_id,
                "canonical_url": canonical_url,
                "published_at": published_at.isoformat() if published_at is not None else "",
                "title": title,
                "body": body[:1000],
            }
        )

    normalized.sort(
        key=lambda row: (
            str(row["published_at"]),
            str(row["canonical_url"]),
            int(row["article_id"]),
        )
    )
    return normalized


def run_extract_stub(
    *,
    market_id: str,
    articles: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    schema_version: str = CLAIM_EXTRACT_SCHEMA_VERSION,
    fixture_path: str | Path | None = None,
) -> dict[str, Any]:
    if fixture_path is not None:
        with Path(fixture_path).open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        if not isinstance(loaded, Mapping):
            raise ValueError("Stub fixture output must be a JSON object")
        payload: dict[str, Any] = dict(loaded)
        payload.setdefault("schema_version", schema_version)
        payload.setdefault("market_id", market_id)
        payload.setdefault("generated_at", _as_utc_datetime(generated_at).isoformat())
    else:
        payload = _build_deterministic_stub_payload(
            market_id=market_id,
            articles=articles,
            generated_at=generated_at,
            schema_version=schema_version,
        )

    canonical_payload = canonicalize_json(payload)
    if not isinstance(canonical_payload, dict):
        raise ValueError("Stub payload canonicalization failed")
    return cast(dict[str, Any], canonical_payload)


def validate_and_normalize(
    payload: Mapping[str, Any],
    *,
    market_id: str,
    schema_version: str = CLAIM_EXTRACT_SCHEMA_VERSION,
) -> ExtractionOutcome:
    try:
        validated = validate_claim_extract(payload)
    except PayloadValidationError as exc:
        error_payload = [validation_issue_to_dict(issue) for issue in exc.issues]
        fallback = canonicalize_json(
            {
                "schema_version": schema_version,
                "market_id": market_id,
                "generated_at": _extract_generated_at(payload),
                "claims": [],
                "claims_raw": [],
                "errors": error_payload,
                "no_trade_flags": [INVALID_OUTPUT_FLAG],
            }
        )
        if not isinstance(fallback, dict):
            raise ValueError("Fallback payload canonicalization failed") from exc
        return ExtractionOutcome(
            payload=cast(dict[str, Any], fallback),
            validator_errors=tuple(exc.issues),
            no_trade_flags=(INVALID_OUTPUT_FLAG,),
            used_fallback=True,
        )

    return ExtractionOutcome(
        payload=validated.payload,
        validator_errors=(),
        no_trade_flags=(),
        used_fallback=False,
    )


def validation_issue_to_dict(issue: ValidationIssue) -> dict[str, str]:
    return {
        "code": issue.code,
        "path": issue.path,
        "reason": issue.reason,
    }


def _build_deterministic_stub_payload(
    *,
    market_id: str,
    articles: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    schema_version: str,
) -> dict[str, Any]:
    normalized = normalize_articles_for_prompt(articles)
    if normalized:
        lead = normalized[0]
        source_url = str(lead["canonical_url"]).strip()
        if not source_url:
            source_url = f"https://example.invalid/articles/{lead['article_id']}"
        claims = [
            {
                "claim_text": (
                    "Article "
                    f"{lead['article_id']} provides material context for market {market_id}."
                ),
                "stance": "unknown",
                "sources": [
                    {
                        "url": source_url,
                        "title": lead["title"],
                        "published_at": lead["published_at"] or _FALLBACK_GENERATED_AT,
                    }
                ],
            }
        ]
    else:
        claims = []

    return {
        "schema_version": schema_version,
        "market_id": market_id,
        "generated_at": _as_utc_datetime(generated_at).isoformat(),
        "claims": claims,
    }


def _extract_generated_at(payload: Mapping[str, Any]) -> str:
    raw = payload.get("generated_at")
    parsed = _optional_datetime(raw)
    if parsed is None:
        return _FALLBACK_GENERATED_AT
    return parsed.isoformat()


def _optional_int(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _optional_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc_datetime(raw)
    text = _as_text(raw)
    if text is None:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return _as_utc_datetime(parsed)


def _as_text(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
