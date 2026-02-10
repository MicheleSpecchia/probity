from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from pmx.db.gamma_catalog_repository import TokenConflict
from pmx.jobs.gamma_catalog_refresh import GammaCatalogRefreshConfig, run_gamma_catalog_refresh


class _FakeConnectionContext:
    def __enter__(self) -> _FakeConnectionContext:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:
        return False


class _FakeRepository:
    instances: list[_FakeRepository] = []

    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.run_inserted = False
        self.upserted_market_ids: list[str] = []
        self.token_records: list[tuple[str, str, str]] = []
        _FakeRepository.instances.append(self)

    def insert_run(self, **_: Any) -> None:
        self.run_inserted = True

    def upsert_market(self, market: Any) -> None:
        self.upserted_market_ids.append(market.market_id)

    def upsert_market_token(self, token: Any) -> TokenConflict | None:
        self.token_records.append((token.market_id, token.outcome, token.token_id))
        if token.market_id == "m-b":
            return TokenConflict(
                token_id=token.token_id,
                existing_market_id="m-a",
                existing_outcome="YES",
            )
        return None


class _FakeGammaClient:
    def __init__(self, config: Any) -> None:
        self.config = config

    def iter_markets(self, **_: Any) -> list[dict[str, Any]]:
        # Intentionally unsorted input to verify deterministic processing order.
        return [
            {
                "id": "m-b",
                "title": "Market B",
                "status": "active",
                "updatedAt": "2026-01-01T00:00:00Z",
                "tokens": [{"outcome": "YES", "tokenId": "token-shared"}],
            },
            {
                "id": "m-a",
                "title": "Market A",
                "status": "active",
                "updatedAt": "2026-01-01T00:00:00Z",
                "tokens": [{"outcome": "YES", "tokenId": "token-shared"}],
            },
        ]


def test_run_gamma_catalog_refresh_is_deterministic_and_logs_conflicts(
    monkeypatch: Any,
    caplog: Any,
) -> None:
    _FakeRepository.instances.clear()

    monkeypatch.setattr(
        "pmx.jobs.gamma_catalog_refresh.get_database_url",
        lambda: "postgresql://fake",
    )
    monkeypatch.setattr(
        "pmx.jobs.gamma_catalog_refresh.psycopg.connect",
        lambda *args, **kwargs: _FakeConnectionContext(),
    )
    monkeypatch.setattr("pmx.jobs.gamma_catalog_refresh.GammaCatalogRepository", _FakeRepository)
    monkeypatch.setattr("pmx.jobs.gamma_catalog_refresh.GammaClient", _FakeGammaClient)

    caplog.set_level(logging.INFO)

    stats = run_gamma_catalog_refresh(
        config=GammaCatalogRefreshConfig(
            gamma_base_url="https://gamma-api.polymarket.com",
            gamma_timeout_seconds=20,
            gamma_page_size=200,
            ingest_epsilon_seconds=300,
        ),
        since_updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        max_markets=None,
    )

    assert stats["markets_upserted"] == 2
    assert stats["token_conflicts"] == 1
    assert stats["errors"] == 1
    assert stats["tokens_upserted"] == 1

    repository = _FakeRepository.instances[0]
    assert repository.run_inserted is True
    assert repository.upserted_market_ids == ["m-a", "m-b"]

    conflict_records = [record for record in caplog.records if record.msg == "token_conflict"]
    assert len(conflict_records) == 1
    extra_fields = conflict_records[0].extra_fields
    assert extra_fields["event"] == "token_conflict"
    assert extra_fields["action"] == "kept_existing"
    assert extra_fields["existing_market_id"] == "m-a"
    assert extra_fields["new_market_id"] == "m-b"
