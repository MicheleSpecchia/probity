from __future__ import annotations

from pathlib import Path

from pmx.ingest.whitelist_crawler import WhitelistCrawler, WhitelistCrawlerConfig, extract_article_fields


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code: int,
        text: str,
        headers: dict[str, str] | None = None,
        url: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self.url = url or "https://example.test/article"


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = list(responses)

    def get(self, url: str, timeout: tuple[int, int]) -> _FakeResponse:
        _ = url, timeout
        if not self.responses:
            raise RuntimeError("No fake response configured")
        return self.responses.pop(0)


def test_extract_article_fields_prefers_jsonld_and_og_title() -> None:
    html = Path("tests/fixtures/news/html_reuters_1.html").read_text(encoding="utf-8")
    extracted = extract_article_fields(html)

    assert extracted["title"] == "Reuters headline from og tag"
    assert extracted["published_at"] is not None
    assert extracted["body"] == "Reuters full body text extracted from JSON-LD."


def test_whitelist_crawler_retries_and_returns_raw_payload() -> None:
    html = Path("tests/fixtures/news/html_ap_1.html").read_text(encoding="utf-8")
    session = _FakeSession(
        [
            _FakeResponse(status_code=429, text="", headers={"Retry-After": "0"}),
            _FakeResponse(
                status_code=200,
                text=html,
                headers={"content-type": "text/html", "etag": "abc"},
                url="https://apnews.com/article/example-law-123",
            ),
        ]
    )
    slept: list[float] = []
    crawler = WhitelistCrawler(
        WhitelistCrawlerConfig(
            connect_timeout_seconds=5,
            read_timeout_seconds=15,
            max_retries=2,
            backoff_seconds=0.5,
            default_rps=100.0,
        ),
        session=session,
        sleep_fn=slept.append,
        clock_fn=lambda: 0.0,
    )

    result = crawler.crawl_article(
        url="https://apnews.com/article/example-law-123",
        domain="apnews.com",
        rps=100.0,
    )

    assert slept[0] == 0.0
    assert result.status_code == 200
    assert result.title == "AP fallback title tag"
    assert result.published_at is not None
    assert "crawler" not in result.raw
    assert result.raw["headers"]["content-type"] == "text/html"
