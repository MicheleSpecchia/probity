from __future__ import annotations

from typing import Any, ClassVar

from pmx.jobs.gamma_outcomes_refresh import (
    GammaOutcomesRefreshConfig,
    run_gamma_outcomes_refresh,
)


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


class _FakeOutcomesRepository:
    instances: ClassVar[list[_FakeOutcomesRepository]] = []

    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.run_inserted = False
        self.outcomes: list[tuple[str, bool, str | None, str]] = []
        _FakeOutcomesRepository.instances.append(self)

    def insert_run(self, **_: Any) -> None:
        self.run_inserted = True

    def upsert_market_outcome(self, record: Any) -> None:
        self.outcomes.append(
            (record.market_id, bool(record.resolved), record.outcome, record.resolver_source)
        )


class _FakeGammaClient:
    def __init__(self, config: Any) -> None:
        self.config = config

    def iter_markets(self, **_: Any) -> list[dict[str, Any]]:
        # Intentionally unsorted to verify deterministic ordering in the job.
        return [
            {
                "id": "m-c",
                "title": "Market C",
                "outcomes": '["Yes","No"]',
                "outcomePrices": '["0.50","0.50"]',
                "updatedAt": "2026-02-10T10:00:00Z",
            },
            {
                "id": "m-a",
                "title": "Market A",
                "winningOutcome": "Yes",
                "resolvedAt": "2026-02-10T09:00:00Z",
            },
            {
                "id": "m-b",
                "title": "Market B",
                "outcomes": '["Yes","No"]',
                "outcomePrices": '["1.00","0.00"]',
                "updatedAt": "2026-02-10T09:30:00Z",
            },
        ]


def _build_config() -> GammaOutcomesRefreshConfig:
    return GammaOutcomesRefreshConfig(
        gamma_base_url="https://gamma-api.polymarket.com",
        gamma_timeout_seconds=20,
        gamma_page_size=200,
        ingest_epsilon_seconds=300,
    )


def test_run_gamma_outcomes_refresh_is_deterministic_without_filter(monkeypatch: Any) -> None:
    _FakeOutcomesRepository.instances.clear()

    monkeypatch.setattr(
        "pmx.jobs.gamma_outcomes_refresh.get_database_url",
        lambda: "postgresql://fake",
    )
    monkeypatch.setattr(
        "pmx.jobs.gamma_outcomes_refresh.psycopg.connect",
        lambda *args, **kwargs: _FakeConnectionContext(),
    )
    monkeypatch.setattr(
        "pmx.jobs.gamma_outcomes_refresh.OutcomesRepository",
        _FakeOutcomesRepository,
    )
    monkeypatch.setattr("pmx.jobs.gamma_outcomes_refresh.GammaClient", _FakeGammaClient)

    stats = run_gamma_outcomes_refresh(
        config=_build_config(),
        max_markets=10,
        only_resolved=False,
    )

    assert stats["errors"] == 0
    assert stats["outcomes_upserted"] == 3
    assert stats["resolved_upserted"] == 2
    assert stats["unresolved_upserted"] == 1
    assert stats["unresolved_skipped"] == 0

    repository = _FakeOutcomesRepository.instances[0]
    assert repository.run_inserted is True
    assert repository.outcomes == [
        ("m-a", True, "Yes", "explicit:winningOutcome"),
        ("m-b", True, "Yes", "inferred:outcomePrices_unique_max_ge_0.99"),
        ("m-c", False, None, "unresolved"),
    ]


def test_run_gamma_outcomes_refresh_only_resolved(monkeypatch: Any) -> None:
    _FakeOutcomesRepository.instances.clear()

    monkeypatch.setattr(
        "pmx.jobs.gamma_outcomes_refresh.get_database_url",
        lambda: "postgresql://fake",
    )
    monkeypatch.setattr(
        "pmx.jobs.gamma_outcomes_refresh.psycopg.connect",
        lambda *args, **kwargs: _FakeConnectionContext(),
    )
    monkeypatch.setattr(
        "pmx.jobs.gamma_outcomes_refresh.OutcomesRepository",
        _FakeOutcomesRepository,
    )
    monkeypatch.setattr("pmx.jobs.gamma_outcomes_refresh.GammaClient", _FakeGammaClient)

    stats = run_gamma_outcomes_refresh(
        config=_build_config(),
        max_markets=10,
        only_resolved=True,
    )

    assert stats["errors"] == 0
    assert stats["outcomes_upserted"] == 2
    assert stats["resolved_upserted"] == 2
    assert stats["unresolved_upserted"] == 0
    assert stats["unresolved_skipped"] == 1

    repository = _FakeOutcomesRepository.instances[0]
    assert repository.outcomes == [
        ("m-a", True, "Yes", "explicit:winningOutcome"),
        ("m-b", True, "Yes", "inferred:outcomePrices_unique_max_ge_0.99"),
    ]
