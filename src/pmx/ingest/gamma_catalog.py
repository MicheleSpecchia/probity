from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class MarketRecord:
    market_id: str
    slug: str | None
    title: str
    description: str | None
    category: str | None
    status: str
    created_ts: datetime | None
    updated_ts: datetime | None
    resolution_ts: datetime | None
    rule_text: str | None
    rule_parse_json: dict[str, Any]
    rule_parse_ok: bool


@dataclass(frozen=True, slots=True)
class MarketTokenRecord:
    market_id: str
    outcome: str
    token_id: str


def parse_rule_text(rule_text: str | None) -> tuple[bool, dict[str, Any]]:
    text = (rule_text or "").strip()
    lower = text.lower()

    keywords = ("if", "then", "before", "after", "by", "resolve", "yes", "no")
    detected = [keyword for keyword in keywords if keyword in lower]

    signals = {
        "has_text": bool(text),
        "has_date_hint": any(token in lower for token in ("before", "after", " by ")),
        "has_boolean_outcome_hint": any(token in lower for token in ("yes", "no")),
        "detected_keywords": sorted(detected),
    }

    payload = {
        "version": "stub_v1",
        "signals": signals,
        "notes": "Rule parser stub only. Semantic parsing is deferred to a later milestone.",
    }
    return False, payload


def normalize_market_payload(
    payload: Mapping[str, Any],
    *,
    ingested_at: datetime,
    gamma_etag: str | None = None,
) -> tuple[MarketRecord | None, list[MarketTokenRecord]]:
    market_id = _as_text(
        payload.get("market_id")
        or payload.get("marketId")
        or payload.get("id")
        or payload.get("condition_id")
        or payload.get("conditionId")
    )
    if not market_id:
        return None, []

    rule_text = _as_text(
        payload.get("rule_text")
        or payload.get("rules")
        or payload.get("resolution_criteria")
        or payload.get("resolutionCriteria")
    )
    rule_parse_ok, rule_parse_json = parse_rule_text(rule_text)

    audit_payload: dict[str, Any] = {
        "gamma_raw": _canonicalize_json(dict(payload)),
        "ingested_at": _as_utc_datetime(ingested_at).isoformat(),
    }
    if gamma_etag is not None and gamma_etag.strip():
        audit_payload["gamma_etag"] = gamma_etag.strip()

    enriched_rule_parse_json = {
        "version": str(rule_parse_json.get("version", "stub_v1")),
        "signals": _canonicalize_json(rule_parse_json.get("signals", {})),
        "notes": str(
            rule_parse_json.get(
                "notes",
                "Rule parser stub only. Semantic parsing is deferred to a later milestone.",
            )
        ),
        "audit": audit_payload,
    }

    record = MarketRecord(
        market_id=market_id,
        slug=_as_text(payload.get("slug")),
        title=_as_text(payload.get("title") or payload.get("question")) or market_id,
        description=_as_text(payload.get("description")),
        category=_as_text(payload.get("category")),
        status=_normalize_status(payload),
        created_ts=_parse_optional_datetime(
            payload.get("created_ts")
            or payload.get("createdAt")
            or payload.get("created_at")
            or payload.get("created")
        ),
        updated_ts=_parse_optional_datetime(
            payload.get("updated_ts")
            or payload.get("updatedAt")
            or payload.get("updated_at")
            or payload.get("updated")
        ),
        resolution_ts=_parse_optional_datetime(
            payload.get("resolution_ts")
            or payload.get("resolvedAt")
            or payload.get("resolveDate")
            or payload.get("resolutionDate")
        ),
        rule_text=rule_text,
        rule_parse_json=enriched_rule_parse_json,
        rule_parse_ok=rule_parse_ok,
    )

    return record, extract_market_tokens(payload, market_id=market_id)


def extract_market_tokens(payload: Mapping[str, Any], *, market_id: str) -> list[MarketTokenRecord]:
    out: list[MarketTokenRecord] = []

    tokens = payload.get("tokens")
    if isinstance(tokens, list):
        for token in tokens:
            if not isinstance(token, Mapping):
                continue
            token_id = _as_text(token.get("token_id") or token.get("tokenId") or token.get("id"))
            outcome = _as_text(token.get("outcome") or token.get("name") or token.get("label"))
            if token_id and outcome:
                out.append(
                    MarketTokenRecord(
                        market_id=market_id,
                        outcome=outcome,
                        token_id=token_id,
                    )
                )

    if not out:
        clob_token_ids = payload.get("clobTokenIds")
        outcomes = payload.get("outcomes")
        if isinstance(clob_token_ids, list) and isinstance(outcomes, list):
            for outcome_value, token_id_value in zip(outcomes, clob_token_ids, strict=False):
                token_id = _as_text(token_id_value)
                outcome = _as_text(outcome_value)
                if token_id and outcome:
                    out.append(
                        MarketTokenRecord(
                            market_id=market_id,
                            outcome=outcome,
                            token_id=token_id,
                        )
                    )

    if not out:
        mapping = payload.get("tokenIdsByOutcome")
        if isinstance(mapping, Mapping):
            for outcome_key, token_id_value in mapping.items():
                token_id = _as_text(token_id_value)
                outcome = _as_text(outcome_key)
                if token_id and outcome:
                    out.append(
                        MarketTokenRecord(
                            market_id=market_id,
                            outcome=outcome,
                            token_id=token_id,
                        )
                    )

    deduped: dict[tuple[str, str], MarketTokenRecord] = {}
    for token in out:
        deduped[(token.market_id, token.outcome)] = token
    return list(deduped.values())


def market_sort_key(payload: Mapping[str, Any]) -> tuple[str, str]:
    updated = _parse_optional_datetime(
        payload.get("updated_ts")
        or payload.get("updatedAt")
        or payload.get("updated_at")
        or payload.get("updated")
    )
    updated_key = updated.isoformat() if updated else ""
    market_id = _as_text(
        payload.get("market_id")
        or payload.get("marketId")
        or payload.get("id")
        or payload.get("condition_id")
        or payload.get("conditionId")
    )
    return updated_key, market_id or ""


def _normalize_status(payload: Mapping[str, Any]) -> str:
    status = _as_text(payload.get("status"))
    if status:
        return status.lower()

    if payload.get("resolved") is True:
        return "resolved"
    if payload.get("closed") is True:
        return "closed"
    if payload.get("active") is True:
        return "active"

    return "unknown"


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_optional_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc_datetime(raw)
    if isinstance(raw, int | float):
        return datetime.fromtimestamp(float(raw), tz=UTC)
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return None
        normalized = stripped.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return _as_utc_datetime(parsed)
    return None


def _canonicalize_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        items = sorted(value.items(), key=lambda item: str(item[0]))
        return {str(key): _canonicalize_json(item_value) for key, item_value in items}
    if isinstance(value, list):
        return [_canonicalize_json(item) for item in value]
    if isinstance(value, tuple):
        return [_canonicalize_json(item) for item in value]
    return value
