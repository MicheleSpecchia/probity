"""Ingestion clients and payload normalization helpers."""

from pmx.ingest.clob_client import (
    CandleRecord,
    ClobClientConfig,
    ClobRestClient,
    OrderbookSnapshot,
    TradeRecord,
    build_trade_hash,
    normalize_orderbook,
)
from pmx.ingest.clob_wss_client import (
    ClobReconnectEvent,
    ClobStreamEvent,
    ClobWssClient,
    ClobWssConfig,
    parse_stream_message,
)
from pmx.ingest.gamma_catalog import (
    MarketRecord,
    MarketTokenRecord,
    extract_market_tokens,
    market_sort_key,
    normalize_market_payload,
    parse_rule_text,
)
from pmx.ingest.gamma_client import GammaClient, GammaClientConfig
from pmx.ingest.gdelt_client import (
    GdeltArticle,
    GdeltClient,
    GdeltClientConfig,
    parse_gdelt_articles,
)
from pmx.ingest.whitelist_crawler import (
    CrawlResult,
    WhitelistCrawler,
    WhitelistCrawlerConfig,
    extract_article_fields,
)

__all__ = [
    "CandleRecord",
    "ClobClientConfig",
    "ClobReconnectEvent",
    "ClobRestClient",
    "ClobStreamEvent",
    "ClobWssClient",
    "ClobWssConfig",
    "CrawlResult",
    "GammaClient",
    "GammaClientConfig",
    "GdeltArticle",
    "GdeltClient",
    "GdeltClientConfig",
    "MarketRecord",
    "MarketTokenRecord",
    "OrderbookSnapshot",
    "TradeRecord",
    "WhitelistCrawler",
    "WhitelistCrawlerConfig",
    "build_trade_hash",
    "extract_article_fields",
    "extract_market_tokens",
    "market_sort_key",
    "normalize_market_payload",
    "normalize_orderbook",
    "parse_gdelt_articles",
    "parse_rule_text",
    "parse_stream_message",
]
