from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

import psycopg


@dataclass(frozen=True, slots=True)
class MarketOutcomeRecord:
    market_id: str
    resolved: bool
    outcome: str | None
    resolved_ts: datetime | None
    resolver_source: str
    ingested_at: datetime


class OutcomesRepository:
    def __init__(self, connection: psycopg.Connection) -> None:
        self.connection = connection

    def insert_run(
        self,
        *,
        run_id: UUID,
        run_type: str,
        decision_ts: datetime,
        ingest_epsilon_seconds: int,
        code_version: str,
        config_hash: str,
    ) -> None:
        decision_ts_utc = _as_utc_datetime(decision_ts)
        with self.connection.cursor() as cursor:
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
                    decision_ts_utc,
                    ingest_epsilon_seconds,
                    code_version,
                    config_hash,
                ),
            )

    def upsert_market_outcome(self, record: MarketOutcomeRecord) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO market_outcomes (
                    market_id,
                    resolved,
                    outcome,
                    resolved_ts,
                    resolver_source,
                    ingested_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_id) DO UPDATE
                SET resolved = EXCLUDED.resolved,
                    outcome = EXCLUDED.outcome,
                    resolved_ts = EXCLUDED.resolved_ts,
                    resolver_source = EXCLUDED.resolver_source,
                    ingested_at = EXCLUDED.ingested_at
                """,
                (
                    record.market_id,
                    record.resolved,
                    record.outcome,
                    _as_utc_datetime(record.resolved_ts)
                    if record.resolved_ts is not None
                    else None,
                    record.resolver_source,
                    _as_utc_datetime(record.ingested_at),
                ),
            )


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
