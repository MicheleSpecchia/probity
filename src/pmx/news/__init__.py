"""News ingestion normalization, dedupe, and linking helpers."""

from pmx.news.dedupe import (
    DedupeHashes,
    SoftDedupeCandidate,
    build_dedupe_hashes,
    select_soft_dedupe_candidate,
)
from pmx.news.linking import (
    LinkedMarketScore,
    MarketLexiconEntry,
    build_market_lexicon,
    link_article_markets,
)
from pmx.news.normalize import canonicalize_url, extract_domain, normalize_text
from pmx.news.primary_sources import (
    PrimarySourceConfig,
    PrimarySourcePolicy,
    load_primary_sources_config,
    match_primary_source_policy,
)

__all__ = [
    "DedupeHashes",
    "LinkedMarketScore",
    "MarketLexiconEntry",
    "PrimarySourceConfig",
    "PrimarySourcePolicy",
    "SoftDedupeCandidate",
    "build_dedupe_hashes",
    "build_market_lexicon",
    "canonicalize_url",
    "extract_domain",
    "link_article_markets",
    "load_primary_sources_config",
    "match_primary_source_policy",
    "normalize_text",
    "select_soft_dedupe_candidate",
]
