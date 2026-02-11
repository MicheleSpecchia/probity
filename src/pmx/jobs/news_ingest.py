from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.db.news_repository import (
    ArticleWritePayload,
    NewsRepository,
)
from pmx.ingest.gdelt_client import (
    DEFAULT_GDELT_BASE_URL,
    DEFAULT_GDELT_MAX_RECORDS,
    DEFAULT_GDELT_TIMEOUT_SECONDS,
    GdeltArticle,
    GdeltClient,
    GdeltClientConfig,
)
from pmx.ingest.whitelist_crawler import CrawlResult, WhitelistCrawler, WhitelistCrawlerConfig
from pmx.news.dedupe import (
    DedupeHashes,
    SoftDedupeCandidate,
    build_dedupe_hashes,
    select_soft_dedupe_match,
)
from pmx.news.linking import (
    LinkedMarketScore,
    MarketLexiconEntry,
    build_market_lexicon,
    link_article_markets,
)
from pmx.news.normalize import canonicalize_json, canonicalize_url, extract_domain
from pmx.news.primary_sources import (
    PrimarySourceConfig,
    load_primary_sources_config,
    match_primary_source_policy,
)

JOB_NAME = "news_ingest"


@dataclass(frozen=True, slots=True)
class NewsIngestConfig:
    gdelt_base_url: str
    gdelt_timeout_seconds: int
    gdelt_max_retries: int
    gdelt_backoff_seconds: float
    gdelt_max_records: int
    crawler_connect_timeout_seconds: int
    crawler_read_timeout_seconds: int
    crawler_max_retries: int
    crawler_backoff_seconds: float
    primary_sources_config_path: str
    ingest_epsilon_seconds: int

    def as_hash_dict(self) -> dict[str, int | str | float]:
        return {
            "gdelt_base_url": self.gdelt_base_url,
            "gdelt_timeout_seconds": self.gdelt_timeout_seconds,
            "gdelt_max_retries": self.gdelt_max_retries,
            "gdelt_backoff_seconds": self.gdelt_backoff_seconds,
            "gdelt_max_records": self.gdelt_max_records,
            "crawler_connect_timeout_seconds": self.crawler_connect_timeout_seconds,
            "crawler_read_timeout_seconds": self.crawler_read_timeout_seconds,
            "crawler_max_retries": self.crawler_max_retries,
            "crawler_backoff_seconds": self.crawler_backoff_seconds,
            "primary_sources_config_path": self.primary_sources_config_path,
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
        }


@dataclass(frozen=True, slots=True)
class PublishedAtSelection:
    published_at: datetime
    source: str
    unknown_published_at: bool


@dataclass(frozen=True, slots=True)
class DedupeUpsertResult:
    article_id: int
    outcome: str
    dedupe_reason: str | None
    published_at_source: str
    unknown_published_at: bool


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    since_published = _parse_optional_datetime_arg(args.since_published)

    config = load_news_ingest_config()
    stats = run_news_ingest(
        config=config,
        since_published=since_published,
        max_articles=args.max_articles,
        max_per_domain=args.max_per_domain,
        crawl_primary=args.crawl_primary,
    )
    return 0 if stats["errors"] == 0 else 1


def run_news_ingest(
    *,
    config: NewsIngestConfig,
    since_published: datetime | None,
    max_articles: int | None,
    max_per_domain: int | None,
    crawl_primary: bool,
    gdelt_client: GdeltClient | None = None,
    crawler: WhitelistCrawler | None = None,
    primary_sources: PrimarySourceConfig | None = None,
) -> dict[str, int]:
    if max_articles is not None and max_articles <= 0:
        raise ValueError("--max-articles must be > 0")
    if max_per_domain is not None and max_per_domain <= 0:
        raise ValueError("--max-per-domain must be > 0")

    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("Missing DB URL: set DATABASE_URL or APP_DATABASE_URL")

    started_at = datetime.now(tz=UTC)
    run_context = build_run_context(
        JOB_NAME,
        {
            **config.as_hash_dict(),
            "since_published": since_published.isoformat() if since_published else None,
            "max_articles": max_articles,
            "max_per_domain": max_per_domain,
            "crawl_primary": crawl_primary,
        },
        started_at=started_at,
    )
    run_uuid = UUID(run_context.run_id)
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")

    _log(
        logger,
        logging.INFO,
        "news_ingest_started",
        run_context,
        since_published=since_published.isoformat() if since_published else None,
        max_articles=max_articles,
        max_per_domain=max_per_domain,
        crawl_primary=crawl_primary,
    )

    effective_primary_sources = primary_sources or load_primary_sources_config(
        config.primary_sources_config_path
    )
    effective_gdelt_client = gdelt_client or GdeltClient(
        GdeltClientConfig(
            base_url=config.gdelt_base_url,
            timeout_seconds=config.gdelt_timeout_seconds,
            max_retries=config.gdelt_max_retries,
            backoff_seconds=config.gdelt_backoff_seconds,
            max_records=config.gdelt_max_records,
        )
    )
    effective_crawler = crawler or WhitelistCrawler(
        WhitelistCrawlerConfig(
            connect_timeout_seconds=config.crawler_connect_timeout_seconds,
            read_timeout_seconds=config.crawler_read_timeout_seconds,
            max_retries=config.crawler_max_retries,
            backoff_seconds=config.crawler_backoff_seconds,
            default_rps=effective_primary_sources.defaults.rps,
        )
    )

    fetched_articles = effective_gdelt_client.fetch_articles(
        since_published=since_published,
        max_articles=max_articles,
    )
    filtered_articles = _apply_max_per_domain(
        fetched_articles,
        max_per_domain=max_per_domain,
    )

    stats = {
        "errors": 0,
        "fetched": len(fetched_articles),
        "inserted": 0,
        "updated": 0,
        "deduped_hard": 0,
        "deduped_soft": 0,
        "crawled_primary": 0,
        "linked": 0,
    }

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        repository = NewsRepository(connection)
        repository.insert_run(
            run_id=run_uuid,
            run_type="news_ingest",
            decision_ts=started_at,
            ingest_epsilon_seconds=config.ingest_epsilon_seconds,
            code_version=run_context.code_version,
            config_hash=run_context.config_hash,
        )

        source_ids_by_domain = _upsert_primary_sources(
            repository=repository,
            source_config=effective_primary_sources,
        )
        lexicon = build_market_lexicon(repository.list_markets_for_linking())

        for article in filtered_articles:
            try:
                article_domain = article.domain or extract_domain(article.url) or "unknown.local"
                source_policy = match_primary_source_policy(
                    article_domain,
                    effective_primary_sources.domains,
                )
                source_domain = source_policy.domain if source_policy else article_domain
                source_id = source_ids_by_domain.get(source_domain)
                if source_id is None:
                    source_id = repository.upsert_source(
                        domain=source_domain,
                        name=source_domain,
                        is_primary=False,
                        trust_score=50,
                    )
                    source_ids_by_domain[source_domain] = source_id

                crawl_result: CrawlResult | None = None
                if crawl_primary and source_policy is not None and source_policy.is_primary:
                    crawl_result = effective_crawler.crawl_article(
                        url=article.url,
                        domain=source_domain,
                        rps=source_policy.rps,
                    )
                    stats["crawled_primary"] += 1

                dedupe_result = _upsert_article_with_dedupe(
                    repository=repository,
                    run_ingested_at=started_at,
                    article=article,
                    crawl_result=crawl_result,
                    source_domain=source_domain,
                    source_id=source_id,
                )
                article_id = dedupe_result.article_id

                if dedupe_result.outcome == "inserted":
                    stats["inserted"] += 1
                else:
                    stats["updated"] += 1
                    if dedupe_result.dedupe_reason == "canonical_url":
                        stats["deduped_hard"] += 1
                    elif dedupe_result.dedupe_reason in {"content_hash", "title_window"}:
                        stats["deduped_soft"] += 1

                if dedupe_result.dedupe_reason is not None:
                    _log(
                        logger,
                        logging.INFO,
                        "dedupe_applied",
                        run_context,
                        article_id=article_id,
                        source_domain=source_domain,
                        url=article.url,
                        dedupe_reason=dedupe_result.dedupe_reason,
                        action_taken="update_existing",
                    )

                if dedupe_result.unknown_published_at:
                    _log(
                        logger,
                        logging.WARNING,
                        "article_missing_published_at",
                        run_context,
                        article_id=article_id,
                        source_domain=source_domain,
                        url=article.url,
                        action_taken="ingested_at_fallback",
                    )

                linked_scores = _build_links_for_article(
                    article=article,
                    crawl_result=crawl_result,
                    lexicon=lexicon,
                )
                linked_count = repository.replace_article_markets(
                    article_id,
                    [linked.market_id for linked in linked_scores],
                )
                stats["linked"] += linked_count

                if linked_scores:
                    _log(
                        logger,
                        logging.INFO,
                        "market_linking_applied",
                        run_context,
                        article_id=article_id,
                        links=[
                            {
                                "market_id": linked.market_id,
                                "score": linked.score,
                                "title_hits": linked.title_hits,
                                "body_hits": linked.body_hits,
                                "slug_hits": linked.slug_hits,
                            }
                            for linked in linked_scores
                        ],
                    )
            except Exception as exc:
                stats["errors"] += 1
                _log(
                    logger,
                    logging.ERROR,
                    "news_article_failed",
                    run_context,
                    url=article.url,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )

    _log(
        logger,
        logging.INFO,
        "news_ingest_completed",
        run_context,
        **stats,
    )
    return stats


def load_news_ingest_config() -> NewsIngestConfig:
    return NewsIngestConfig(
        gdelt_base_url=os.getenv("GDELT_BASE_URL", DEFAULT_GDELT_BASE_URL),
        gdelt_timeout_seconds=_load_positive_int(
            "GDELT_TIMEOUT_SECONDS", DEFAULT_GDELT_TIMEOUT_SECONDS
        ),
        gdelt_max_retries=_load_non_negative_int("GDELT_MAX_RETRIES", 4),
        gdelt_backoff_seconds=_load_positive_float("GDELT_BACKOFF_SECONDS", 0.5),
        gdelt_max_records=_load_positive_int("GDELT_MAX_RECORDS", DEFAULT_GDELT_MAX_RECORDS),
        crawler_connect_timeout_seconds=_load_positive_int(
            "NEWS_CRAWLER_CONNECT_TIMEOUT_SECONDS",
            5,
        ),
        crawler_read_timeout_seconds=_load_positive_int("NEWS_CRAWLER_READ_TIMEOUT_SECONDS", 15),
        crawler_max_retries=_load_non_negative_int("NEWS_CRAWLER_MAX_RETRIES", 3),
        crawler_backoff_seconds=_load_positive_float("NEWS_CRAWLER_BACKOFF_SECONDS", 0.5),
        primary_sources_config_path=os.getenv(
            "NEWS_PRIMARY_SOURCES_CONFIG",
            str(Path("config") / "primary_sources.yaml"),
        ),
        ingest_epsilon_seconds=_load_positive_int("INGEST_EPSILON_SECONDS", 300),
    )


def _upsert_primary_sources(
    *,
    repository: NewsRepository,
    source_config: PrimarySourceConfig,
) -> dict[str, int]:
    source_ids: dict[str, int] = {}
    for source in source_config.domains:
        source_id = repository.upsert_source(
            domain=source.domain,
            name=source.name,
            is_primary=source.is_primary,
            trust_score=source.trust_score,
        )
        source_ids[source.domain] = source_id
    return source_ids


def _upsert_article_with_dedupe(
    *,
    repository: NewsRepository,
    run_ingested_at: datetime,
    article: GdeltArticle,
    crawl_result: CrawlResult | None,
    source_domain: str,
    source_id: int,
) -> DedupeUpsertResult:
    final_url = _as_text(crawl_result.raw.get("final_url")) if crawl_result is not None else None
    canonical_url = canonicalize_url(final_url) or canonicalize_url(article.url) or article.url

    title = _choose_first_text(
        crawl_result.title if crawl_result is not None else None,
        article.title,
        canonical_url,
        article.url,
    )
    body = crawl_result.body if crawl_result is not None else None
    summary = article.summary
    published_selection = _select_article_published_at(
        crawler_published_at=crawl_result.published_at if crawl_result is not None else None,
        gdelt_published_at=article.published_at,
        run_ingested_at=run_ingested_at,
    )

    dedupe_hashes = build_dedupe_hashes(title=title, body=body, summary=summary)
    raw_payload = _build_article_raw_payload(
        article=article,
        crawl_result=crawl_result,
        canonical_url=canonical_url,
        dedupe_hashes=dedupe_hashes,
        published_selection=published_selection,
    )
    write_payload = ArticleWritePayload(
        url=article.url,
        canonical_url=canonical_url,
        title=title,
        body=body,
        summary=summary,
        published_at=published_selection.published_at,
        ingested_at=run_ingested_at,
        source_id=source_id,
        lang=article.lang,
        raw=raw_payload,
    )

    hard_match = repository.find_article_by_canonical_url(canonical_url)
    if hard_match is not None:
        merged_payload = _merge_payload_fill_missing(
            repository=repository,
            article_id=hard_match.article_id,
            incoming=write_payload,
            incoming_unknown_published_at=published_selection.unknown_published_at,
        )
        repository.update_article(hard_match.article_id, merged_payload)
        return DedupeUpsertResult(
            article_id=hard_match.article_id,
            outcome="updated",
            dedupe_reason="canonical_url",
            published_at_source=published_selection.source,
            unknown_published_at=published_selection.unknown_published_at,
        )

    candidates = repository.find_soft_candidates_by_content_hash(
        content_hash=dedupe_hashes.content_hash,
    )
    title_candidates = repository.find_soft_candidates_by_title_hash(
        title_hash=dedupe_hashes.title_hash,
        source_domain=source_domain,
        published_at=published_selection.published_at,
    )
    merged_candidates = _merge_soft_candidates(candidates, title_candidates)
    soft_match = select_soft_dedupe_match(
        merged_candidates,
        content_hash=dedupe_hashes.content_hash,
        title_hash=dedupe_hashes.title_hash,
        source_domain=source_domain,
        published_at=published_selection.published_at,
    )
    if soft_match is not None:
        if (
            soft_match.reason == "title_window"
            and soft_match.candidate.source_domain != source_domain
        ):
            inserted_id = repository.insert_article(write_payload)
            return DedupeUpsertResult(
                article_id=inserted_id,
                outcome="inserted",
                dedupe_reason=None,
                published_at_source=published_selection.source,
                unknown_published_at=published_selection.unknown_published_at,
            )
        merged_payload = _merge_payload_fill_missing(
            repository=repository,
            article_id=soft_match.candidate.article_id,
            incoming=write_payload,
            incoming_unknown_published_at=published_selection.unknown_published_at,
        )
        repository.update_article(soft_match.candidate.article_id, merged_payload)
        return DedupeUpsertResult(
            article_id=soft_match.candidate.article_id,
            outcome="updated",
            dedupe_reason=soft_match.reason,
            published_at_source=published_selection.source,
            unknown_published_at=published_selection.unknown_published_at,
        )

    inserted_id = repository.insert_article(write_payload)
    return DedupeUpsertResult(
        article_id=inserted_id,
        outcome="inserted",
        dedupe_reason=None,
        published_at_source=published_selection.source,
        unknown_published_at=published_selection.unknown_published_at,
    )


def _build_article_raw_payload(
    *,
    article: GdeltArticle,
    crawl_result: CrawlResult | None,
    canonical_url: str,
    dedupe_hashes: DedupeHashes,
    published_selection: PublishedAtSelection,
) -> dict[str, Any]:
    return canonicalize_json(
        {
            "gdelt": article.raw,
            "crawler": crawl_result.raw if crawl_result is not None else {"attempted": False},
            "dedupe": {
                "canonical_url": canonical_url,
                "content_hash": dedupe_hashes.content_hash,
                "title_hash": dedupe_hashes.title_hash,
            },
            "ingest": {
                "published_at_source": published_selection.source,
                "unknown_published_at": published_selection.unknown_published_at,
            },
        }
    )


def _select_article_published_at(
    *,
    crawler_published_at: datetime | None,
    gdelt_published_at: datetime | None,
    run_ingested_at: datetime,
) -> PublishedAtSelection:
    if crawler_published_at is not None:
        return PublishedAtSelection(
            published_at=_as_utc_datetime(crawler_published_at),
            source="crawler_published_at",
            unknown_published_at=False,
        )
    if gdelt_published_at is not None:
        return PublishedAtSelection(
            published_at=_as_utc_datetime(gdelt_published_at),
            source="gdelt_published_at",
            unknown_published_at=False,
        )
    return PublishedAtSelection(
        published_at=_as_utc_datetime(run_ingested_at),
        source="ingested_at_fallback",
        unknown_published_at=True,
    )


def _merge_payload_fill_missing(
    *,
    repository: NewsRepository,
    article_id: int,
    incoming: ArticleWritePayload,
    incoming_unknown_published_at: bool,
) -> ArticleWritePayload:
    existing = repository.get_article(article_id)
    if existing is None:
        return incoming

    existing_unknown_published_at = _read_unknown_published_flag(existing.raw)
    use_incoming_published_at = existing_unknown_published_at and not incoming_unknown_published_at
    merged_published_at = (
        incoming.published_at if use_incoming_published_at else existing.published_at
    )
    merged_unknown_published_at = (
        incoming_unknown_published_at
        if use_incoming_published_at
        else existing_unknown_published_at
    )

    merged_raw = _merge_json_fill_missing(existing.raw, incoming.raw)
    merged_raw = _set_unknown_published_flag(merged_raw, merged_unknown_published_at)

    return ArticleWritePayload(
        url=incoming.url,
        canonical_url=_prefer_existing_text(existing.canonical_url, incoming.canonical_url),
        title=_prefer_existing_text(existing.title, incoming.title),
        body=_prefer_existing_optional_text(existing.body, incoming.body),
        summary=_prefer_existing_optional_text(existing.summary, incoming.summary),
        published_at=merged_published_at,
        ingested_at=incoming.ingested_at,
        source_id=existing.source_id,
        lang=_prefer_existing_optional_text(existing.lang, incoming.lang),
        raw=canonicalize_json(merged_raw),
    )


def _read_unknown_published_flag(raw_payload: Mapping[str, Any]) -> bool:
    ingest = raw_payload.get("ingest")
    if not isinstance(ingest, Mapping):
        return False
    return bool(ingest.get("unknown_published_at"))


def _set_unknown_published_flag(raw_payload: dict[str, Any], value: bool) -> dict[str, Any]:
    merged = dict(raw_payload)
    ingest = merged.get("ingest")
    ingest_dict: dict[str, Any]
    if isinstance(ingest, Mapping):
        ingest_dict = dict(ingest)
    else:
        ingest_dict = {}
    ingest_dict["unknown_published_at"] = bool(value)
    merged["ingest"] = ingest_dict
    return merged


def _merge_json_fill_missing(existing: Any, incoming: Any) -> Any:
    if isinstance(existing, Mapping) and isinstance(incoming, Mapping):
        existing_map = {str(key): value for key, value in existing.items()}
        incoming_map = {str(key): value for key, value in incoming.items()}
        merged: dict[str, Any] = {}
        keys = sorted(set(existing_map.keys()) | set(incoming_map.keys()))
        for key in keys:
            existing_value = existing_map.get(key)
            incoming_value = incoming_map.get(key)
            merged[key] = _merge_json_fill_missing(existing_value, incoming_value)
        return merged
    if isinstance(existing, list) and isinstance(incoming, list):
        if existing:
            return list(existing)
        return list(incoming)
    if _is_empty_value(existing):
        return incoming
    return existing


def _is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, Mapping):
        return len(value) == 0
    if isinstance(value, list):
        return len(value) == 0
    return False


def _prefer_existing_text(existing: str | None, incoming: str) -> str:
    if existing is not None:
        normalized = existing.strip()
        if normalized and normalized.lower() != "untitled":
            return normalized
    return incoming


def _prefer_existing_optional_text(existing: str | None, incoming: str | None) -> str | None:
    if existing is not None:
        normalized = existing.strip()
        if normalized and normalized.lower() != "untitled":
            return normalized
    if incoming is None:
        return None
    normalized_incoming = incoming.strip()
    return normalized_incoming if normalized_incoming else None


def _merge_soft_candidates(
    left: Sequence[SoftDedupeCandidate],
    right: Sequence[SoftDedupeCandidate],
) -> list[SoftDedupeCandidate]:
    merged: dict[int, SoftDedupeCandidate] = {}
    for candidate in left:
        merged[int(candidate.article_id)] = candidate
    for candidate in right:
        merged[int(candidate.article_id)] = candidate
    return [merged[key] for key in sorted(merged.keys())]


def _build_links_for_article(
    *,
    article: GdeltArticle,
    crawl_result: CrawlResult | None,
    lexicon: Sequence[MarketLexiconEntry],
) -> list[LinkedMarketScore]:
    title = _choose_first_text(
        crawl_result.title if crawl_result is not None else None,
        article.title,
    )
    body = crawl_result.body if crawl_result is not None else None
    return link_article_markets(
        title=title,
        body=body,
        lexicon=lexicon,
        top_k=5,
    )


def _apply_max_per_domain(
    articles: Sequence[GdeltArticle],
    *,
    max_per_domain: int | None,
) -> list[GdeltArticle]:
    ordered = sorted(
        articles,
        key=lambda article: (
            article.published_at or datetime.min.replace(tzinfo=UTC),
            article.url,
        ),
    )
    if max_per_domain is None:
        return ordered

    counts: dict[str, int] = {}
    filtered: list[GdeltArticle] = []
    for article in ordered:
        domain = article.domain or extract_domain(article.url) or "unknown.local"
        seen = counts.get(domain, 0)
        if seen >= max_per_domain:
            continue
        counts[domain] = seen + 1
        filtered.append(article)
    return filtered


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest news from GDELT + whitelist crawler.")
    parser.add_argument(
        "--since-published",
        dest="since_published",
        default=None,
        help="Optional ISO datetime lower bound for GDELT published timestamps.",
    )
    parser.add_argument(
        "--max-articles",
        type=int,
        default=None,
        help="Optional maximum number of GDELT articles to process.",
    )
    parser.add_argument(
        "--max-per-domain",
        type=int,
        default=None,
        help="Optional per-domain cap applied after deterministic sorting.",
    )
    parser.add_argument(
        "--crawl-primary",
        action="store_true",
        dest="crawl_primary",
        help="Enable primary-source whitelist crawling (default).",
    )
    parser.add_argument(
        "--no-crawl-primary",
        action="store_false",
        dest="crawl_primary",
        help="Disable primary-source whitelist crawling.",
    )
    parser.set_defaults(crawl_primary=True)
    return parser.parse_args(argv)


def _parse_optional_datetime_arg(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid --since-published value: {raw!r}") from exc
    return _as_utc_datetime(parsed)


def _choose_first_text(*values: str | None) -> str:
    for value in values:
        text = _as_text(value)
        if text:
            return text
    return "untitled"


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _load_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be > 0, got {parsed}")
    return parsed


def _load_non_negative_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be >= 0, got {parsed}")
    return parsed


def _load_positive_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a float, got {raw!r}") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be > 0, got {parsed}")
    return parsed


def _log(
    logger: logging.Logger,
    level: int,
    message: str,
    run_context: RunContext,
    **extra_fields: Any,
) -> None:
    payload: dict[str, Any] = dict(run_context.as_log_context())
    payload["extra_fields"] = extra_fields
    logger.log(level, message, extra=payload)


if __name__ == "__main__":
    raise SystemExit(main())
