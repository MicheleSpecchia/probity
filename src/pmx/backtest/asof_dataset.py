from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

import psycopg

from pmx.db.feature_repository import build_token_feature_set_version


@dataclass(frozen=True, slots=True)
class Example:
    token_id: str
    market_id: str
    decision_ts: datetime
    features_json: dict[str, Any]
    price_prob: float
    outcome_y: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "token_id": self.token_id,
            "market_id": self.market_id,
            "decision_ts": self.decision_ts.isoformat(),
            "features_json": self.features_json,
            "price_prob": self.price_prob,
            "outcome_y": self.outcome_y,
        }


@dataclass(frozen=True, slots=True)
class AsofDataset:
    examples: tuple[Example, ...]
    skipped_no_outcome: int
    skipped_missing_features: int
    skipped_missing_price: int


@dataclass(frozen=True, slots=True)
class OutcomeRecord:
    market_id: str
    resolved_outcome: str
    resolved_ts: datetime


class OutcomeProvider(Protocol):
    def get_outcome(self, conn: psycopg.Connection, market_id: str) -> OutcomeRecord | None: ...


class DbOutcomeProvider:
    def __init__(self) -> None:
        self._cache: dict[str, OutcomeRecord | None] = {}

    def get_outcome(self, conn: psycopg.Connection, market_id: str) -> OutcomeRecord | None:
        if market_id in self._cache:
            return self._cache[market_id]

        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT resolved, outcome, resolved_ts
                    FROM market_outcomes
                    WHERE market_id = %s
                    LIMIT 1
                    """,
                    (market_id,),
                )
                row = cursor.fetchone()
        except psycopg.Error:
            self._cache[market_id] = None
            return None

        if row is None:
            self._cache[market_id] = None
            return None

        resolved = bool(row[0])
        resolved_outcome = _as_text(row[1])
        resolved_ts_raw = row[2]
        if not resolved or resolved_outcome is None or not isinstance(resolved_ts_raw, datetime):
            self._cache[market_id] = None
            return None

        record = OutcomeRecord(
            market_id=market_id,
            resolved_outcome=resolved_outcome,
            resolved_ts=_as_utc_datetime(resolved_ts_raw),
        )
        self._cache[market_id] = record
        return record


class FixtureOutcomeProvider:
    def __init__(self, outcomes: Mapping[str, OutcomeRecord]) -> None:
        self._outcomes = dict(outcomes)

    @classmethod
    def from_json_path(cls, path: str) -> FixtureOutcomeProvider:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("Fixture outcome payload must be a list")
        outcomes: dict[str, OutcomeRecord] = {}
        for item in payload:
            if not isinstance(item, Mapping):
                continue
            market_id = _as_text(item.get("market_id"))
            outcome = _as_text(item.get("outcome"))
            resolved_ts_raw = _as_text(item.get("resolved_ts"))
            if market_id is None or outcome is None or resolved_ts_raw is None:
                continue
            resolved_ts = _parse_datetime(resolved_ts_raw)
            if resolved_ts is None:
                continue
            outcomes[market_id] = OutcomeRecord(
                market_id=market_id,
                resolved_outcome=outcome,
                resolved_ts=resolved_ts,
            )
        return cls(outcomes)

    def get_outcome(self, conn: psycopg.Connection, market_id: str) -> OutcomeRecord | None:
        del conn
        return self._outcomes.get(market_id)


def build_asof_examples(
    conn: psycopg.Connection,
    token_ids: Sequence[str],
    decision_ts_list: Sequence[datetime],
    epsilon_s: int,
    feature_set: str = "micro_v1",
) -> list[Example]:
    dataset = build_asof_dataset(
        conn,
        token_ids=token_ids,
        decision_ts_list=decision_ts_list,
        epsilon_s=epsilon_s,
        feature_set=feature_set,
        outcome_provider=None,
    )
    return list(dataset.examples)


def build_asof_dataset(
    conn: psycopg.Connection,
    *,
    token_ids: Sequence[str],
    decision_ts_list: Sequence[datetime],
    epsilon_s: int,
    feature_set: str = "micro_v1",
    outcome_provider: OutcomeProvider | None = None,
) -> AsofDataset:
    if epsilon_s <= 0:
        raise ValueError("epsilon_s must be > 0")

    normalized_tokens = sorted({token.strip() for token in token_ids if token.strip()})
    normalized_decisions = sorted({_as_utc_datetime(ts) for ts in decision_ts_list})
    if not normalized_tokens or not normalized_decisions:
        return AsofDataset(
            examples=(),
            skipped_no_outcome=0,
            skipped_missing_features=0,
            skipped_missing_price=0,
        )

    provider = outcome_provider or DbOutcomeProvider()
    pairs = _list_token_market_pairs(conn, normalized_tokens)

    examples: list[Example] = []
    skipped_no_outcome = 0
    skipped_missing_features = 0
    skipped_missing_price = 0

    for pair in pairs:
        outcome = provider.get_outcome(conn, pair.market_id)
        if outcome is None:
            skipped_no_outcome += len(normalized_decisions)
            continue
        for decision_ts in normalized_decisions:
            if decision_ts >= outcome.resolved_ts:
                skipped_no_outcome += 1
                continue

            outcome_y = (
                1
                if _normalize_label(pair.token_outcome)
                == _normalize_label(outcome.resolved_outcome)
                else 0
            )
            features_json = _fetch_features_snapshot(
                conn,
                market_id=pair.market_id,
                token_id=pair.token_id,
                decision_ts=decision_ts,
                feature_set=feature_set,
            )
            if features_json is None:
                skipped_missing_features += 1
                continue

            price_prob = _fetch_price_prob(
                conn,
                token_id=pair.token_id,
                decision_ts=decision_ts,
                epsilon_s=epsilon_s,
            )
            if price_prob is None:
                skipped_missing_price += 1
                continue

            examples.append(
                Example(
                    token_id=pair.token_id,
                    market_id=pair.market_id,
                    decision_ts=decision_ts,
                    features_json=features_json,
                    price_prob=price_prob,
                    outcome_y=outcome_y,
                )
            )

    examples.sort(key=lambda item: (item.token_id, item.decision_ts))
    return AsofDataset(
        examples=tuple(examples),
        skipped_no_outcome=skipped_no_outcome,
        skipped_missing_features=skipped_missing_features,
        skipped_missing_price=skipped_missing_price,
    )


@dataclass(frozen=True, slots=True)
class _TokenMarketOutcome:
    token_id: str
    market_id: str
    token_outcome: str


def _list_token_market_pairs(
    conn: psycopg.Connection,
    token_ids: list[str],
) -> list[_TokenMarketOutcome]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT token_id, market_id, outcome
            FROM market_tokens
            WHERE token_id = ANY(%s)
            ORDER BY token_id ASC
            """,
            (token_ids,),
        )
        rows = cursor.fetchall()

    output: list[_TokenMarketOutcome] = []
    for row in rows:
        token = _as_text(row[0])
        market_id = _as_text(row[1])
        outcome = _as_text(row[2])
        if token is None or market_id is None or outcome is None:
            continue
        output.append(
            _TokenMarketOutcome(
                token_id=token,
                market_id=market_id,
                token_outcome=outcome,
            )
        )
    return output


def _fetch_features_snapshot(
    conn: psycopg.Connection,
    *,
    market_id: str,
    token_id: str,
    decision_ts: datetime,
    feature_set: str,
) -> dict[str, Any] | None:
    token_scoped = build_token_feature_set_version(feature_set, token_id)
    with conn.cursor() as cursor:
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
            (market_id, decision_ts, [token_scoped, feature_set]),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    payload = row[0]
    if not isinstance(payload, Mapping):
        return None
    return {str(key): value for key, value in payload.items()}


def _fetch_price_prob(
    conn: psycopg.Connection,
    *,
    token_id: str,
    decision_ts: datetime,
    epsilon_s: int,
) -> float | None:
    ingest_bound = decision_ts + timedelta(seconds=epsilon_s)

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT mid, bids, asks
            FROM orderbook_snapshots
            WHERE token_id = %s
              AND event_ts <= %s
              AND ingested_at <= %s
            ORDER BY event_ts DESC, ingested_at DESC, snapshot_id DESC
            LIMIT 1
            """,
            (token_id, decision_ts, ingest_bound),
        )
        book_row = cursor.fetchone()

    if book_row is not None:
        mid = _prob_from_mid_or_book(
            mid_raw=book_row[0],
            bids_raw=book_row[1],
            asks_raw=book_row[2],
        )
        if mid is not None:
            return mid

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT price
            FROM trades
            WHERE token_id = %s
              AND event_ts <= %s
              AND ingested_at <= %s
            ORDER BY event_ts DESC, ingested_at DESC, trade_id DESC
            LIMIT 1
            """,
            (token_id, decision_ts, ingest_bound),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return _prob_from_raw(row[0])


def _prob_from_mid_or_book(*, mid_raw: Any, bids_raw: Any, asks_raw: Any) -> float | None:
    mid_prob = _prob_from_raw(mid_raw)
    if mid_prob is not None:
        return mid_prob

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
        if not isinstance(item, Mapping):
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


def _normalize_label(label: str) -> str:
    return " ".join(label.strip().lower().split())


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _parse_datetime(raw: str) -> datetime | None:
    text = raw.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
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
