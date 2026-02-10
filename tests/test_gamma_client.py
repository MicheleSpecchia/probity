from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pmx.ingest.gamma_client import GammaClient, GammaClientConfig, extract_market_page


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code: int,
        payload: Any,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict[str, Any], timeout: int) -> _FakeResponse:
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
        if not self.responses:
            raise RuntimeError("No fake responses left")
        return self.responses.pop(0)


def test_extract_market_page_handles_dict_payload_with_cursor() -> None:
    payload = {
        "markets": [{"id": "m1"}, {"id": "m2"}],
        "next_cursor": "cursor-2",
    }

    markets, next_params = extract_market_page(payload)

    assert [market["id"] for market in markets] == ["m1", "m2"]
    assert next_params == {"cursor": "cursor-2"}


def test_extract_market_page_handles_list_payload_without_cursor() -> None:
    payload = [{"id": "m1"}, {"id": "m2"}]

    markets, next_params = extract_market_page(payload)

    assert [market["id"] for market in markets] == ["m1", "m2"]
    assert next_params == {}


def test_extract_market_page_handles_pagination_next_page() -> None:
    payload = {
        "data": [{"id": "m1"}],
        "pagination": {"next_page": 3},
    }

    markets, next_params = extract_market_page(payload)

    assert [market["id"] for market in markets] == ["m1"]
    assert next_params == {"page": 3}


def test_iter_markets_applies_since_filter_and_cursor_pagination() -> None:
    responses = [
        _FakeResponse(
            status_code=200,
            payload={
                "markets": [
                    {"id": "m1", "updatedAt": "2026-01-01T00:00:00Z"},
                    {"id": "m2", "updatedAt": "2026-01-02T00:00:00Z"},
                ],
                "next_cursor": "next-1",
            },
        ),
        _FakeResponse(
            status_code=200,
            payload={
                "markets": [{"id": "m3", "updatedAt": "2026-01-03T00:00:00Z"}],
            },
        ),
    ]
    session = _FakeSession(responses)
    client = GammaClient(
        GammaClientConfig(base_url="https://example.gamma", page_size=2),
        session=session,
    )

    markets = client.iter_markets(
        since_updated_at=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert [market["id"] for market in markets] == ["m2", "m3"]
    assert session.calls[0]["params"]["limit"] == 2
    assert session.calls[1]["params"]["cursor"] == "next-1"


def test_iter_markets_retries_on_429_with_retry_after() -> None:
    responses = [
        _FakeResponse(status_code=429, payload={"error": "rate"}, headers={"Retry-After": "0"}),
        _FakeResponse(status_code=200, payload=[]),
    ]
    session = _FakeSession(responses)
    slept: list[float] = []

    client = GammaClient(
        GammaClientConfig(base_url="https://example.gamma", page_size=1),
        session=session,
        sleep_fn=slept.append,
    )

    markets = client.iter_markets()

    assert markets == []
    assert slept == [0.0]


def test_iter_markets_falls_back_to_offset_for_list_payloads() -> None:
    responses = [
        _FakeResponse(
            status_code=200,
            payload=[{"id": "m1"}, {"id": "m2"}],
        ),
        _FakeResponse(
            status_code=200,
            payload=[{"id": "m3"}],
        ),
    ]
    session = _FakeSession(responses)
    client = GammaClient(
        GammaClientConfig(base_url="https://example.gamma", page_size=2),
        session=session,
    )

    markets = client.iter_markets()

    assert [market["id"] for market in markets] == ["m1", "m2", "m3"]
    assert session.calls[0]["params"] == {"limit": 2}
    assert session.calls[1]["params"]["offset"] == 2
