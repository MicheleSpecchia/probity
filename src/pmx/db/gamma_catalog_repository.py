from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pmx.ingest.gamma_catalog import MarketRecord, MarketTokenRecord


@dataclass(frozen=True, slots=True)
class TokenConflict:
    token_id: str
    existing_market_id: str
    existing_outcome: str
    action: str = "kept_existing"


class GammaCatalogRepository:
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

    def upsert_market(self, market: MarketRecord) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (
                    market_id,
                    slug,
                    title,
                    description,
                    category,
                    status,
                    created_ts,
                    updated_ts,
                    resolution_ts,
                    rule_text,
                    rule_parse_json,
                    rule_parse_ok
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s::jsonb, %s
                )
                ON CONFLICT (market_id) DO UPDATE
                SET slug = EXCLUDED.slug,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    category = EXCLUDED.category,
                    status = EXCLUDED.status,
                    created_ts = EXCLUDED.created_ts,
                    updated_ts = EXCLUDED.updated_ts,
                    resolution_ts = EXCLUDED.resolution_ts,
                    rule_text = EXCLUDED.rule_text,
                    rule_parse_json = EXCLUDED.rule_parse_json,
                    rule_parse_ok = EXCLUDED.rule_parse_ok
                """,
                (
                    market.market_id,
                    market.slug,
                    market.title,
                    market.description,
                    market.category,
                    market.status,
                    market.created_ts,
                    market.updated_ts,
                    market.resolution_ts,
                    market.rule_text,
                    Jsonb(market.rule_parse_json),
                    market.rule_parse_ok,
                ),
            )

    def upsert_market_token(self, token: MarketTokenRecord) -> TokenConflict | None:
        try:
            # `connection.transaction()` creates a savepoint when nested inside
            # an outer transaction, allowing deterministic rollback on unique conflicts.
            with self.connection.transaction():
                with self.connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO market_tokens (market_id, outcome, token_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (market_id, outcome) DO UPDATE
                        SET token_id = EXCLUDED.token_id
                        """,
                        (token.market_id, token.outcome, token.token_id),
                    )
        except psycopg.IntegrityError as exc:
            if not _is_token_unique_conflict(exc):
                raise

            existing = self._find_token_owner(token.token_id)
            if existing is None:
                raise

            return TokenConflict(
                token_id=token.token_id,
                existing_market_id=str(existing["market_id"]),
                existing_outcome=str(existing["outcome"]),
            )
        return None

    def _find_token_owner(self, token_id: str) -> dict[str, Any] | None:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT market_id, outcome
                FROM market_tokens
                WHERE token_id = %s
                """,
                (token_id,),
            )
            row = cursor.fetchone()
        return row


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _is_token_unique_conflict(exc: psycopg.IntegrityError) -> bool:
    if not isinstance(exc, psycopg.errors.UniqueViolation):
        return False

    diag = getattr(exc, "diag", None)
    constraint_name = getattr(diag, "constraint_name", None)
    return constraint_name == "market_tokens_token_id_uk"
