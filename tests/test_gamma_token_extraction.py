from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar

from pmx.ingest.gamma_catalog import extract_market_tokens
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


class _TokenCaptureRepository:
    instances: ClassVar[list[_TokenCaptureRepository]] = []

    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.token_records: list[tuple[str, str, str]] = []
        _TokenCaptureRepository.instances.append(self)

    def insert_run(self, **_: Any) -> None:
        return None

    def upsert_market(self, market: Any) -> None:
        return None

    def upsert_market_token(self, token: Any) -> None:
        self.token_records.append((token.market_id, token.outcome, token.token_id))
        return None


class _StringFieldsGammaClient:
    def __init__(self, config: Any) -> None:
        self.config = config

    def iter_markets(self, **_: Any) -> list[dict[str, Any]]:
        return [
            {
                "id": "market-json-refresh",
                "title": "Gamma refresh string fields",
                "status": "active",
                "updatedAt": "2026-01-01T00:00:00Z",
                "outcomes": '["Yes","No"]',
                "clobTokenIds": '["tok-yes","tok-no"]',
                "outcomePrices": '["0","0"]',
            }
        ]


def test_extract_market_tokens_parses_json_string_fields_deterministically() -> None:
    payload = {
        "id": "market-json",
        "title": "Gamma JSON string payload",
        "outcomes": '["Yes","No"]',
        "clobTokenIds": '["tok-yes","tok-no"]',
        "outcomePrices": '["0","0"]',
    }

    first = extract_market_tokens(payload, market_id="market-json")
    second = extract_market_tokens(payload, market_id="market-json")

    assert first == second
    assert [(token.outcome, token.token_id) for token in first] == [
        ("No", "tok-no"),
        ("Yes", "tok-yes"),
    ]


def test_extract_market_tokens_keeps_real_list_compatibility() -> None:
    payload = {
        "id": "market-list",
        "title": "Gamma list payload",
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["tok-yes", "tok-no"],
    }

    tokens = extract_market_tokens(payload, market_id="market-list")

    assert [(token.outcome, token.token_id) for token in tokens] == [
        ("No", "tok-no"),
        ("Yes", "tok-yes"),
    ]


def test_gamma_catalog_refresh_upserts_tokens_from_json_string_fields(monkeypatch: Any) -> None:
    _TokenCaptureRepository.instances.clear()

    monkeypatch.setattr(
        "pmx.jobs.gamma_catalog_refresh.get_database_url",
        lambda: "postgresql://fake",
    )
    monkeypatch.setattr(
        "pmx.jobs.gamma_catalog_refresh.psycopg.connect",
        lambda *args, **kwargs: _FakeConnectionContext(),
    )
    monkeypatch.setattr(
        "pmx.jobs.gamma_catalog_refresh.GammaCatalogRepository",
        _TokenCaptureRepository,
    )
    monkeypatch.setattr(
        "pmx.jobs.gamma_catalog_refresh.GammaClient",
        _StringFieldsGammaClient,
    )

    stats = run_gamma_catalog_refresh(
        config=GammaCatalogRefreshConfig(
            gamma_base_url="https://gamma-api.polymarket.com",
            gamma_timeout_seconds=20,
            gamma_page_size=200,
            ingest_epsilon_seconds=300,
        ),
        since_updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        max_markets=10,
    )

    assert stats["tokens_upserted"] == 2
    assert stats["errors"] == 0
    repository = _TokenCaptureRepository.instances[0]
    assert repository.token_records == [
        ("market-json-refresh", "No", "tok-no"),
        ("market-json-refresh", "Yes", "tok-yes"),
    ]
