from __future__ import annotations

from datetime import UTC, datetime

from pmx.jobs.news_ingest import _merge_json_fill_missing, _select_article_published_at


def test_published_at_prefers_crawler_then_gdelt() -> None:
    run_ingested_at = datetime(2026, 1, 1, 16, 0, tzinfo=UTC)
    crawler_published_at = datetime(2026, 1, 1, 12, 5, tzinfo=UTC)
    gdelt_published_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    decision = _select_article_published_at(
        crawler_published_at=crawler_published_at,
        gdelt_published_at=gdelt_published_at,
        run_ingested_at=run_ingested_at,
    )

    assert decision.published_at == crawler_published_at
    assert decision.source == "crawler_published_at"
    assert decision.unknown_published_at is False


def test_published_at_falls_back_to_ingested_at_and_marks_unknown() -> None:
    run_ingested_at = datetime(2026, 1, 1, 16, 0, tzinfo=UTC)
    decision = _select_article_published_at(
        crawler_published_at=None,
        gdelt_published_at=None,
        run_ingested_at=run_ingested_at,
    )

    assert decision.published_at == run_ingested_at
    assert decision.source == "ingested_at_fallback"
    assert decision.unknown_published_at is True


def test_merge_json_fill_missing_preserves_existing_non_empty_values() -> None:
    existing = {
        "crawler": {"attempted": False},
        "dedupe": {"canonical_url": "https://example.com/a"},
        "ingest": {"unknown_published_at": True},
    }
    incoming = {
        "crawler": {"status_code": 200},
        "dedupe": {"canonical_url": "https://example.com/b", "content_hash": "x"},
        "ingest": {"published_at_source": "crawler_published_at"},
    }

    merged = _merge_json_fill_missing(existing, incoming)

    assert merged["dedupe"]["canonical_url"] == "https://example.com/a"
    assert merged["dedupe"]["content_hash"] == "x"
    assert merged["ingest"]["unknown_published_at"] is True
    assert merged["ingest"]["published_at_source"] == "crawler_published_at"
