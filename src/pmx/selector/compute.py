from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg

from pmx.selector.spec import (
    SelectorConfig,
    compute_deep_score,
    compute_screen_score,
    liquidity_quality_from_features,
)
from pmx.selector.ttr import estimate_ttr_bucket

RECENTLY_UPDATED_HOURS = 48
TOP_VOLUME_RATIO = 0.80


@dataclass(frozen=True, slots=True)
class Candidate:
    market_id: str
    token_id: str
    category: str
    group_id: str
    ttr_bucket: str
    market_payload: dict[str, Any]
    volume_24h: float
    include_reasons: tuple[str, ...]
    screen_score: float = 0.0
    deep_score: float = 0.0


@dataclass(frozen=True, slots=True)
class CandidateScore:
    market_id: str
    token_id: str
    category: str
    group_id: str
    ttr_bucket: str
    screen_score: float
    lq: float
    volume_24h: float
    price_prob: float | None
    components: dict[str, float]
    flags: tuple[str, ...]
    penalties: dict[str, float]
    include_reasons: tuple[str, ...]
    deep_score: float = 0.0
    deep_components: dict[str, float] = field(default_factory=dict)
    deep_flags: tuple[str, ...] = ()
    deep_penalties: dict[str, float] = field(default_factory=dict)
    deep_reason_hash: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "token_id": self.token_id,
            "category": self.category,
            "group_id": self.group_id,
            "ttr_bucket": self.ttr_bucket,
            "screen_score": self.screen_score,
            "lq": self.lq,
            "volume_24h": self.volume_24h,
            "price_prob": self.price_prob,
            "components": self.components,
            "flags": list(self.flags),
            "penalties": self.penalties,
            "include_reasons": list(self.include_reasons),
            "deep_score": self.deep_score,
            "deep_components": self.deep_components,
            "deep_flags": list(self.deep_flags),
            "deep_penalties": self.deep_penalties,
            "deep_reason_hash": self.deep_reason_hash,
        }


def build_candidate_set(
    conn: psycopg.Connection,
    decision_ts: datetime,
    epsilon_s: int,
    *,
    max_candidates: int = 1500,
    feature_set: str = "micro_v1",
    selector_config: SelectorConfig | None = None,
) -> list[Candidate]:
    if max_candidates <= 0:
        raise ValueError("max_candidates must be > 0")
    decision = _as_utc_datetime(decision_ts)
    ingest_bound = decision + timedelta(seconds=epsilon_s)
    cfg = selector_config or SelectorConfig()

    rows = _fetch_market_token_rows(conn)
    if not rows:
        return []

    token_ids = [row.token_id for row in rows]
    volume_map = _fetch_volume_map(conn, token_ids, decision, ingest_bound)
    feature_map = _fetch_feature_map(conn, rows, decision, ingest_bound, feature_set)

    top_volume_n = max(1, int(max_candidates * TOP_VOLUME_RATIO))
    by_volume = sorted(
        rows,
        key=lambda row: (-volume_map.get(row.token_id, 0.0), row.market_id, row.token_id),
    )[:top_volume_n]
    by_volume_ids = {row.token_id for row in by_volume}

    lq_candidates: set[str] = set()
    recent_candidates: set[str] = set()
    recent_cutoff = decision - timedelta(hours=RECENTLY_UPDATED_HOURS)
    for row in rows:
        features = feature_map.get(row.token_id)
        lq = liquidity_quality_from_features(features)
        if lq >= cfg.lq_threshold:
            lq_candidates.add(row.token_id)
        if (
            row.updated_ts is not None
            and row.updated_ts <= decision
            and row.updated_ts >= recent_cutoff
        ):
            recent_candidates.add(row.token_id)

    selected_tokens = by_volume_ids | lq_candidates | recent_candidates
    candidates: list[Candidate] = []
    for row in rows:
        if row.token_id not in selected_tokens:
            continue
        reasons: list[str] = []
        if row.token_id in by_volume_ids:
            reasons.append("by_volume")
        if row.token_id in lq_candidates:
            reasons.append("by_lq_threshold")
        if row.token_id in recent_candidates:
            reasons.append("recently_updated")

        market_payload = row.market_payload
        candidates.append(
            Candidate(
                market_id=row.market_id,
                token_id=row.token_id,
                category=row.category,
                group_id=row.group_id,
                ttr_bucket=estimate_ttr_bucket(market_payload, decision),
                market_payload=market_payload,
                volume_24h=round(volume_map.get(row.token_id, 0.0), 8),
                include_reasons=tuple(sorted(reasons)),
                screen_score=0.0,
                deep_score=0.0,
            )
        )

    candidates.sort(
        key=lambda item: (
            -item.volume_24h,
            item.market_id,
            item.token_id,
        )
    )
    return candidates[:max_candidates]


def compute_scores(
    conn: psycopg.Connection,
    candidates: Sequence[Candidate],
    decision_ts: datetime,
    epsilon_s: int,
    *,
    feature_set: str = "micro_v1",
    selector_config: SelectorConfig | None = None,
) -> list[CandidateScore]:
    decision = _as_utc_datetime(decision_ts)
    ingest_bound = decision + timedelta(seconds=epsilon_s)
    cfg = selector_config or SelectorConfig()
    token_ids = [candidate.token_id for candidate in candidates]

    feature_map = _fetch_feature_map_for_candidates(
        conn,
        candidates,
        decision,
        ingest_bound,
        feature_set,
    )
    price_map = _fetch_price_prob_map(conn, token_ids, decision, ingest_bound)

    scored: list[CandidateScore] = []
    for candidate in sorted(candidates, key=lambda item: (item.token_id, item.market_id)):
        features = feature_map.get(candidate.token_id)
        price_prob = price_map.get(candidate.token_id)
        result = compute_screen_score(
            features=features,
            price_prob=price_prob,
            market_payload=candidate.market_payload,
            config=cfg,
        )
        deep_result = compute_deep_score(
            score_result=result,
            ttr_bucket=candidate.ttr_bucket,
            price_prob=price_prob,
        )
        scored.append(
            CandidateScore(
                market_id=candidate.market_id,
                token_id=candidate.token_id,
                category=candidate.category,
                group_id=candidate.group_id,
                ttr_bucket=candidate.ttr_bucket,
                screen_score=result.screen_score,
                lq=result.components.get("lq", 0.0),
                volume_24h=candidate.volume_24h,
                price_prob=price_prob,
                components=result.components,
                flags=result.flags,
                penalties=result.penalties,
                include_reasons=candidate.include_reasons,
                deep_score=deep_result.deep_score,
                deep_components=deep_result.components,
                deep_flags=deep_result.flags,
                deep_penalties=deep_result.penalties,
                deep_reason_hash=deep_result.reason_hash,
            )
        )
    return sorted(
        scored,
        key=lambda item: (
            -item.screen_score,
            -item.lq,
            -item.volume_24h,
            item.market_id,
        ),
    )


def compute_deep_scores(scored: Sequence[CandidateScore]) -> list[CandidateScore]:
    return sorted(
        scored,
        key=lambda item: (
            -item.deep_score,
            -item.screen_score,
            -item.volume_24h,
            item.market_id,
        ),
    )


@dataclass(frozen=True, slots=True)
class _MarketTokenRow:
    market_id: str
    token_id: str
    category: str
    group_id: str
    updated_ts: datetime | None
    market_payload: dict[str, Any]


def _fetch_market_token_rows(conn: psycopg.Connection) -> list[_MarketTokenRow]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                mt.market_id,
                mt.token_id,
                COALESCE(m.category, 'unknown') AS category,
                COALESCE(m.slug, m.market_id) AS group_id,
                m.updated_ts,
                m.title,
                m.description,
                m.rule_text,
                m.rule_parse_json,
                m.rule_parse_ok,
                m.status,
                m.resolution_ts
            FROM market_tokens AS mt
            JOIN markets AS m
              ON m.market_id = mt.market_id
            ORDER BY mt.token_id ASC
            """
        )
        rows = cursor.fetchall()

    output: list[_MarketTokenRow] = []
    for row in rows:
        market_id = str(row[0])
        token_id = str(row[1])
        category = str(row[2]) if row[2] is not None else "unknown"
        group_id = str(row[3]) if row[3] is not None else market_id
        updated_ts = _as_optional_utc_datetime(row[4])
        market_payload = {
            "market_id": market_id,
            "token_id": token_id,
            "title": row[5],
            "description": row[6],
            "rule_text": row[7],
            "rule_parse_json": row[8] if isinstance(row[8], dict) else {},
            "rule_parse_ok": bool(row[9]),
            "status": row[10],
            "resolution_ts": row[11].isoformat() if isinstance(row[11], datetime) else None,
            "updated_ts": updated_ts.isoformat() if updated_ts is not None else None,
            "category": category,
            "group_id": group_id,
        }
        output.append(
            _MarketTokenRow(
                market_id=market_id,
                token_id=token_id,
                category=category,
                group_id=group_id,
                updated_ts=updated_ts,
                market_payload=market_payload,
            )
        )
    return output


def _fetch_volume_map(
    conn: psycopg.Connection,
    token_ids: Sequence[str],
    decision_ts: datetime,
    ingest_bound: datetime,
) -> dict[str, float]:
    if not token_ids:
        return {}
    lower = decision_ts - timedelta(hours=24)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT token_id, SUM(price * size) AS volume_24h
            FROM trades
            WHERE token_id = ANY(%s)
              AND event_ts >= %s
              AND event_ts <= %s
              AND ingested_at <= %s
            GROUP BY token_id
            """,
            (list(token_ids), lower, decision_ts, ingest_bound),
        )
        rows = cursor.fetchall()
    return {str(row[0]): _numeric_to_float(row[1]) for row in rows}


def _fetch_feature_map(
    conn: psycopg.Connection,
    rows: Sequence[_MarketTokenRow],
    decision_ts: datetime,
    ingest_bound: datetime,
    feature_set: str,
) -> dict[str, dict[str, Any]]:
    candidates = [
        Candidate(
            market_id=row.market_id,
            token_id=row.token_id,
            category=row.category,
            group_id=row.group_id,
            ttr_bucket="unknown",
            market_payload=row.market_payload,
            volume_24h=0.0,
            include_reasons=(),
            screen_score=0.0,
            deep_score=0.0,
        )
        for row in rows
    ]
    return _fetch_feature_map_for_candidates(
        conn, candidates, decision_ts, ingest_bound, feature_set
    )


def _fetch_feature_map_for_candidates(
    conn: psycopg.Connection,
    candidates: Sequence[Candidate],
    decision_ts: datetime,
    ingest_bound: datetime,
    feature_set: str,
) -> dict[str, dict[str, Any]]:
    if not candidates:
        return {}
    output: dict[str, dict[str, Any]] = {}
    with conn.cursor() as cursor:
        for candidate in candidates:
            cursor.execute(
                """
                SELECT features
                FROM feature_snapshots
                WHERE market_id = %s
                  AND asof_ts <= %s
                  AND feature_set_version = ANY(%s)
                ORDER BY asof_ts DESC, feature_snapshot_id DESC
                LIMIT 1
                """,
                (
                    candidate.market_id,
                    decision_ts,
                    [f"{feature_set}:token:{candidate.token_id}", feature_set],
                ),
            )
            row = cursor.fetchone()
            if row is None:
                continue
            payload = row[0]
            if isinstance(payload, dict):
                normalized = {str(key): value for key, value in payload.items()}
                if not _feature_payload_is_asof_safe(normalized, ingest_bound):
                    continue
                output[candidate.token_id] = normalized
    return output


def _feature_payload_is_asof_safe(payload: dict[str, Any], ingest_bound: datetime) -> bool:
    raw_bound = payload.get("ingest_bound_ts")
    parsed_bound = _as_optional_utc_datetime(raw_bound)
    if parsed_bound is None:
        return True
    return parsed_bound <= ingest_bound


def _fetch_price_prob_map(
    conn: psycopg.Connection,
    token_ids: Sequence[str],
    decision_ts: datetime,
    ingest_bound: datetime,
) -> dict[str, float]:
    if not token_ids:
        return {}
    out: dict[str, float] = {}
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT ON (token_id) token_id, mid, bids, asks
            FROM orderbook_snapshots
            WHERE token_id = ANY(%s)
              AND event_ts <= %s
              AND ingested_at <= %s
            ORDER BY token_id, event_ts DESC, ingested_at DESC, snapshot_id DESC
            """,
            (list(token_ids), decision_ts, ingest_bound),
        )
        for row in cursor.fetchall():
            token_id = str(row[0])
            prob = _prob_from_mid_or_book(row[1], row[2], row[3])
            if prob is not None:
                out[token_id] = prob

        missing = [token for token in token_ids if token not in out]
        if missing:
            cursor.execute(
                """
                SELECT DISTINCT ON (token_id) token_id, price
                FROM trades
                WHERE token_id = ANY(%s)
                  AND event_ts <= %s
                  AND ingested_at <= %s
                ORDER BY token_id, event_ts DESC, ingested_at DESC, trade_id DESC
                """,
                (missing, decision_ts, ingest_bound),
            )
            for row in cursor.fetchall():
                token_id = str(row[0])
                prob = _prob_from_raw(row[1])
                if prob is not None:
                    out[token_id] = prob
    return out


def _prob_from_mid_or_book(mid_raw: Any, bids_raw: Any, asks_raw: Any) -> float | None:
    mid = _prob_from_raw(mid_raw)
    if mid is not None:
        return mid
    best_bid = _best_price(bids_raw, descending=True)
    best_ask = _best_price(asks_raw, descending=False)
    if best_bid is None or best_ask is None:
        return None
    if best_bid <= 0 or best_ask <= 0:
        return None
    return _clamp((best_bid + best_ask) / 2.0, 0.0, 1.0)


def _best_price(raw: Any, *, descending: bool) -> float | None:
    if not isinstance(raw, list):
        return None
    prices: list[float] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        price = _prob_from_raw(item.get("price"))
        if price is None:
            continue
        prices.append(price)
    if not prices:
        return None
    return max(prices) if descending else min(prices)


def _prob_from_raw(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        value = float(Decimal(str(raw)))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if value < 0 or value > 1:
        return None
    return value


def _numeric_to_float(raw: Any) -> float:
    if raw is None:
        return 0.0
    try:
        return float(Decimal(str(raw)))
    except (InvalidOperation, ValueError, TypeError):
        return 0.0


def _as_optional_utc_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc_datetime(raw)
    text = str(raw).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return _as_utc_datetime(parsed)


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value
