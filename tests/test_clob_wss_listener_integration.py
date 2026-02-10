from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest

from pmx.audit.run_context import RunContext
from pmx.db.clob_repository import ClobRepository
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.ingest.clob_client import TradeRecord
from pmx.ingest.clob_wss_client import ClobStreamEvent
from pmx.ingest.reconciler import ClobReconciler, ReconcileStrategyConfig, StreamTokenState
from pmx.jobs.clob_wss_listener import _handle_stream_event
from tests.db_helpers import alembic_upgrade_head


class _FakeRestClient:
    def __init__(self, trades: list[TradeRecord]) -> None:
        self._trades = list(trades)

    def get_trades(self, token_id: str, *, since_ts: datetime | None) -> list[TradeRecord]:
        _ = token_id, since_ts
        return list(self._trades)

    def get_orderbook(self, token_id: str, *, fallback_event_ts: datetime | None = None) -> None:
        _ = token_id, fallback_event_ts
        return None


def test_wss_stream_plus_repair_is_idempotent() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    market_id = f"m-{uuid4().hex[:12]}"
    token_id = f"t-{uuid4().hex[:12]}"
    ingested_at = datetime(2026, 2, 10, 12, 0, tzinfo=UTC)

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        repository = ClobRepository(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (market_id, title, status, rule_parse_ok)
                VALUES (%s, %s, %s, %s)
                """,
                (market_id, "CLOB WSS Integration Market", "active", False),
            )
            cursor.execute(
                """
                INSERT INTO market_tokens (market_id, outcome, token_id)
                VALUES (%s, %s, %s)
                """,
                (market_id, "YES", token_id),
            )

        state = StreamTokenState()
        stream_events = [
            ClobStreamEvent(
                token_id=token_id,
                channel="trade",
                event_ts=datetime(2026, 2, 10, 12, 0, 10, tzinfo=UTC),
                seq=10,
                payload={
                    "token_id": token_id,
                    "price": "0.50000000",
                    "size": "1.00000000",
                    "side": "buy",
                    "seq": 10,
                    "hash": "h10",
                },
            ),
            ClobStreamEvent(
                token_id=token_id,
                channel="trade",
                event_ts=datetime(2026, 2, 10, 12, 0, 12, tzinfo=UTC),
                seq=12,
                payload={
                    "token_id": token_id,
                    "price": "0.51000000",
                    "size": "1.00000000",
                    "side": "buy",
                    "seq": 12,
                    "hash": "h12",
                },
            ),
        ]

        for event in stream_events:
            handled = _handle_stream_event(
                event=event,
                token_state=state,
                repository=repository,
                ingested_at=ingested_at,
                orderbook_depth=None,
                seq_mode_enabled=True,
            )
            assert handled == "trade"

        assert state.saw_sequence_gap is True

        rest_trades = [
            TradeRecord(
                token_id=token_id,
                event_ts=datetime(2026, 2, 10, 12, 0, 11, tzinfo=UTC),
                price=Decimal("0.50500000"),
                size=Decimal("1.00000000"),
                side="buy",
                trade_hash="h11",
                seq=11,
            ),
            TradeRecord(
                token_id=token_id,
                event_ts=datetime(2026, 2, 10, 12, 0, 12, tzinfo=UTC),
                price=Decimal("0.51000000"),
                size=Decimal("1.00000000"),
                side="buy",
                trade_hash="h12",
                seq=12,
            ),
        ]
        reconciler = ClobReconciler(
            rest_client=_FakeRestClient(rest_trades),
            repository=repository,
            logger=logging.getLogger("tests.wss_integration"),
            run_context=RunContext(
                run_id="integration-run",
                job_name="clob_wss_listener",
                code_version="test",
                config_hash="cfg",
                started_at=ingested_at.isoformat(),
            ),
            since_ts=None,
            strategy=ReconcileStrategyConfig(
                seq_mode_enabled=True,
                gap_seconds=60,
                mismatch_bps=10,
            ),
        )

        first_result = reconciler.reconcile_token(
            token_id=token_id,
            state=state,
            ingested_at=ingested_at,
        )
        second_result = reconciler.reconcile_token(
            token_id=token_id,
            state=state,
            ingested_at=ingested_at,
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM trades WHERE token_id = %s", (token_id,))
            trade_count = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT event_ts, seq_norm, trade_hash_norm
                FROM trades
                WHERE token_id = %s
                ORDER BY event_ts ASC, seq_norm ASC, trade_hash_norm ASC
                """,
                (token_id,),
            )
            rows = cursor.fetchall()

        assert first_result.gap_detected is True
        assert first_result.action_taken == "rest_refetch_upsert"
        assert first_result.rows_upserted == 2
        assert first_result.rest_calls == 2
        assert second_result.action_taken == "none"
        assert trade_count == 3
        event_ts_values = [row[0] for row in rows]
        id_keys = [(row[0], row[1], row[2]) for row in rows]
        assert event_ts_values == sorted(event_ts_values)
        assert len(id_keys) == len(set(id_keys))
