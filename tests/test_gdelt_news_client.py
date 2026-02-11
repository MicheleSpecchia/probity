from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pmx.ingest.gdelt_client import GdeltClient, GdeltClientConfig, parse_gdelt_articles


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
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict[str, str], timeout: int) -> _FakeResponse:
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
        if not self.responses:
            raise RuntimeError("No fake response available")
        return self.responses.pop(0)


def test_parse_gdelt_articles_from_fixture() -> None:
    payload = json.loads(Path("tests/fixtures/news/gdelt_sample.json").read_text(encoding="utf-8"))
    articles = parse_gdelt_articles(payload)

    assert len(articles) == 2
    assert articles[0].domain == "reuters.com"
    assert articles[1].domain == "apnews.com"
    assert articles[0].published_at is not None
    assert articles[1].published_at is not None


def test_gdelt_client_retries_on_429_and_parses() -> None:
    payload = json.loads(Path("tests/fixtures/news/gdelt_sample.json").read_text(encoding="utf-8"))
    session = _FakeSession(
        [
            _FakeResponse(status_code=429, payload={"error": "rate"}, headers={"Retry-After": "0"}),
            _FakeResponse(status_code=200, payload=payload),
        ]
    )
    slept: list[float] = []
    client = GdeltClient(
        GdeltClientConfig(base_url="https://gdelt.example/api", timeout_seconds=20),
        session=session,
        sleep_fn=slept.append,
    )

    articles = client.fetch_articles(since_published=None, max_articles=10)

    assert len(articles) == 2
    assert slept == [0.0]
    assert session.calls[0]["url"] == "https://gdelt.example/api"
