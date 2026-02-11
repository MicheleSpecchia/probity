from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.ingest.gdelt_client import GdeltArticle, parse_gdelt_articles
from pmx.ingest.whitelist_crawler import CrawlResult
from pmx.jobs.news_ingest import NewsIngestConfig, run_news_ingest
from pmx.news.primary_sources import load_primary_sources_config
from tests.db_helpers import alembic_upgrade_head


class _FakeGdeltClient:
    def __init__(self, articles: list[GdeltArticle]) -> None:
        self._articles = list(articles)

    def fetch_articles(
        self,
        *,
        since_published: datetime | None,
        max_articles: int | None,
    ) -> list[GdeltArticle]:
        _ = since_published
        output = list(self._articles)
        if max_articles is not None:
            output = output[:max_articles]
        return output


class _FakeCrawler:
    def __init__(self) -> None:
        self._html_by_domain = {
            "reuters.com": Path("tests/fixtures/news/html_reuters_1.html").read_text(encoding="utf-8"),
            "apnews.com": Path("tests/fixtures/news/html_ap_1.html").read_text(encoding="utf-8"),
        }

    def crawl_article(self, *, url: str, domain: str, rps: float | None = None) -> CrawlResult:
        _ = rps
        html_payload = self._html_by_domain.get(domain, "")
        if domain == "reuters.com":
            return CrawlResult(
                url=url,
                status_code=200,
                title="Reuters headline from og tag",
                published_at=datetime(2026, 1, 1, 12, 0, 5, tzinfo=UTC),
                body="Reuters full body text extracted from JSON-LD.",
                raw={
                    "status_code": 200,
                    "headers": {"content-type": "text/html"},
                    "final_url": url,
                    "extracted": {
                        "title": "Reuters headline from og tag",
                        "published_at": "2026-01-01T12:00:05+00:00",
                        "body": "Reuters full body text extracted from JSON-LD.",
                    },
                },
            )

        return CrawlResult(
            url=url,
            status_code=200,
            title="AP fallback title tag",
            published_at=datetime(2026, 1, 1, 12, 30, tzinfo=UTC),
            body="AP paragraph one. AP paragraph two.",
            raw={
                "status_code": 200,
                "headers": {"content-type": "text/html"},
                "final_url": url,
                "html_preview": html_payload[:50],
                "extracted": {
                    "title": "AP fallback title tag",
                    "published_at": "2026-01-01T12:30:00+00:00",
                    "body": "AP paragraph one. AP paragraph two.",
                },
            },
        )


class _NoopCrawler:
    def crawl_article(self, *, url: str, domain: str, rps: float | None = None) -> CrawlResult:
        _ = url, domain, rps
        raise AssertionError("Crawler should not be called in this test path")


def test_news_ingest_is_idempotent_with_fixtures() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    payload = json.loads(Path("tests/fixtures/news/gdelt_sample.json").read_text(encoding="utf-8"))
    parsed_articles = parse_gdelt_articles(payload)
    expected_urls = sorted(article.url for article in parsed_articles)
    fake_gdelt = _FakeGdeltClient(parsed_articles)
    fake_crawler = _FakeCrawler()
    primary_sources = load_primary_sources_config(Path("config") / "primary_sources.yaml")

    market_one = f"m-{uuid4().hex[:12]}"
    market_two = f"m-{uuid4().hex[:12]}"
    config = NewsIngestConfig(
        gdelt_base_url="https://gdelt.example/api",
        gdelt_timeout_seconds=20,
        gdelt_max_retries=4,
        gdelt_backoff_seconds=0.5,
        gdelt_max_records=100,
        crawler_connect_timeout_seconds=5,
        crawler_read_timeout_seconds=15,
        crawler_max_retries=3,
        crawler_backoff_seconds=0.5,
        primary_sources_config_path=str(Path("config") / "primary_sources.yaml"),
        ingest_epsilon_seconds=300,
    )

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (market_id, slug, title, status, rule_parse_ok)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (market_one, "us-senate-vote", "US Senate vote", "active", False),
            )
            cursor.execute(
                """
                INSERT INTO markets (market_id, slug, title, status, rule_parse_ok)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (market_two, "example-law", "Example law", "active", False),
            )

    first_stats = run_news_ingest(
        config=config,
        since_published=datetime(2026, 1, 1, tzinfo=UTC),
        max_articles=10,
        max_per_domain=5,
        crawl_primary=True,
        gdelt_client=fake_gdelt,
        crawler=fake_crawler,
        primary_sources=primary_sources,
    )
    second_stats = run_news_ingest(
        config=config,
        since_published=datetime(2026, 1, 1, tzinfo=UTC),
        max_articles=10,
        max_per_domain=5,
        crawl_primary=True,
        gdelt_client=fake_gdelt,
        crawler=fake_crawler,
        primary_sources=primary_sources,
    )

    with psycopg.connect(to_psycopg_dsn(database_url), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM articles
                WHERE url = ANY(%s)
                """,
                (expected_urls,),
            )
            article_count = int(cursor.fetchone()[0])
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM article_markets
                WHERE article_id IN (
                    SELECT article_id
                    FROM articles
                    WHERE url = ANY(%s)
                )
                """,
                (expected_urls,),
            )
            article_market_count = int(cursor.fetchone()[0])
            cursor.execute(
                """
                SELECT
                    raw ? 'gdelt',
                    raw ? 'crawler',
                    raw->'ingest'->>'published_at_source',
                    raw->'ingest'->>'unknown_published_at'
                FROM articles
                WHERE url = ANY(%s)
                ORDER BY article_id ASC
                """,
                (expected_urls,),
            )
            raw_flags = cursor.fetchall()

    assert first_stats["inserted"] == 2
    assert second_stats["inserted"] == 0
    assert second_stats["deduped_hard"] >= 2
    assert article_count == 2
    assert article_market_count >= 1
    assert raw_flags
    assert all(bool(row[0]) and bool(row[1]) for row in raw_flags)
    assert all(str(row[2]) in {"crawler_published_at", "gdelt_published_at"} for row in raw_flags)
    assert all(str(row[3]).lower() == "false" for row in raw_flags)


def test_news_ingest_soft_dedupes_same_content_across_different_urls() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    payload = json.loads(
        Path("tests/fixtures/news/gdelt_soft_content_sample.json").read_text(encoding="utf-8")
    )
    parsed_articles = parse_gdelt_articles(payload)
    fake_gdelt = _FakeGdeltClient(parsed_articles)
    noop_crawler = _NoopCrawler()
    primary_sources = load_primary_sources_config(Path("config") / "primary_sources.yaml")

    market_id = f"m-{uuid4().hex[:12]}"
    config = NewsIngestConfig(
        gdelt_base_url="https://gdelt.example/api",
        gdelt_timeout_seconds=20,
        gdelt_max_retries=4,
        gdelt_backoff_seconds=0.5,
        gdelt_max_records=100,
        crawler_connect_timeout_seconds=5,
        crawler_read_timeout_seconds=15,
        crawler_max_retries=3,
        crawler_backoff_seconds=0.5,
        primary_sources_config_path=str(Path("config") / "primary_sources.yaml"),
        ingest_epsilon_seconds=300,
    )

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO markets (market_id, slug, title, status, rule_parse_ok)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (market_id, "example-law-vote", "Example law vote", "active", False),
            )

    stats = run_news_ingest(
        config=config,
        since_published=datetime(2026, 1, 1, tzinfo=UTC),
        max_articles=10,
        max_per_domain=10,
        crawl_primary=False,
        gdelt_client=fake_gdelt,
        crawler=noop_crawler,
        primary_sources=primary_sources,
    )

    with psycopg.connect(to_psycopg_dsn(database_url), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM articles
                WHERE url = %s OR url = %s
                """,
                (parsed_articles[0].url, parsed_articles[1].url),
            )
            article_count = int(cursor.fetchone()[0])
            cursor.execute(
                """
                SELECT raw->'dedupe'->>'content_hash'
                FROM articles
                WHERE url = %s OR url = %s
                ORDER BY article_id ASC
                """,
                (parsed_articles[0].url, parsed_articles[1].url),
            )
            content_hashes = [row[0] for row in cursor.fetchall()]

    assert stats["inserted"] == 1
    assert stats["deduped_soft"] == 1
    assert article_count == 1
    assert len(content_hashes) == 1
