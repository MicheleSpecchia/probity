from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb


@dataclass(frozen=True, slots=True)
class TokenMarketRef:
    token_id: str
    market_id: str


class FeatureRepository:
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
                    _as_utc_datetime(decision_ts),
                    ingest_epsilon_seconds,
                    code_version,
                    config_hash,
                ),
            )

    def list_token_markets(
        self,
        *,
        token_ids: list[str] | None,
        max_tokens: int | None,
    ) -> list[TokenMarketRef]:
        if token_ids:
            normalized = sorted({token.strip() for token in token_ids if token.strip()})
            if not normalized:
                return []
            query = """
                SELECT token_id, market_id
                FROM market_tokens
                WHERE token_id = ANY(%s)
                ORDER BY token_id ASC
            """
            params: tuple[object, ...] = (normalized,)
        elif max_tokens is None:
            query = """
                SELECT token_id, market_id
                FROM market_tokens
                ORDER BY token_id ASC
            """
            params = ()
        else:
            query = """
                SELECT token_id, market_id
                FROM market_tokens
                ORDER BY token_id ASC
                LIMIT %s
            """
            params = (max_tokens,)

        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        return [TokenMarketRef(token_id=str(row[0]), market_id=str(row[1])) for row in rows]

    def upsert_feature_snapshot(
        self,
        *,
        run_id: UUID,
        market_id: str,
        token_id: str,
        decision_ts: datetime,
        feature_set: str,
        features_json: dict[str, Any],
        computed_at: datetime,
    ) -> None:
        feature_set_version = build_token_feature_set_version(feature_set, token_id)
        payload = dict(features_json)
        payload["_meta"] = {
            "computed_at": _as_utc_datetime(computed_at).isoformat(),
            "feature_set": feature_set,
            "token_id": token_id,
        }
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO feature_snapshots (
                    run_id,
                    market_id,
                    asof_ts,
                    feature_set_version,
                    features
                )
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT ON CONSTRAINT feature_snapshots_run_market_asof_version_uk
                DO UPDATE SET features = EXCLUDED.features
                """,
                (
                    run_id,
                    market_id,
                    _as_utc_datetime(decision_ts),
                    feature_set_version,
                    Jsonb(payload),
                ),
            )


def build_token_feature_set_version(feature_set: str, token_id: str) -> str:
    return f"{feature_set}:token:{token_id}"


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
