from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import psycopg

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.db.outcomes_repository import MarketOutcomeRecord, OutcomesRepository
from pmx.ingest.gamma_catalog import infer_market_outcome
from pmx.ingest.gamma_client import (
    DEFAULT_GAMMA_BASE_URL,
    DEFAULT_GAMMA_PAGE_SIZE,
    DEFAULT_GAMMA_TIMEOUT_SECONDS,
    GammaClient,
    GammaClientConfig,
)

JOB_NAME = "gamma_outcomes_refresh"


@dataclass(frozen=True, slots=True)
class GammaOutcomesRefreshConfig:
    gamma_base_url: str
    gamma_timeout_seconds: int
    gamma_page_size: int
    ingest_epsilon_seconds: int

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "gamma_base_url": self.gamma_base_url,
            "gamma_timeout_seconds": self.gamma_timeout_seconds,
            "gamma_page_size": self.gamma_page_size,
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.max_markets is not None and args.max_markets <= 0:
        raise ValueError("--max-markets must be > 0")

    config = load_gamma_outcomes_refresh_config()
    stats = run_gamma_outcomes_refresh(
        config=config,
        max_markets=args.max_markets,
        only_resolved=bool(args.only_resolved),
    )
    return 0 if stats["errors"] == 0 else 1


def run_gamma_outcomes_refresh(
    *,
    config: GammaOutcomesRefreshConfig,
    max_markets: int | None,
    only_resolved: bool,
) -> dict[str, int]:
    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("Missing DB URL: set DATABASE_URL or APP_DATABASE_URL")

    started_at = datetime.now(tz=UTC)
    run_context = build_run_context(
        JOB_NAME,
        {
            **config.as_hash_dict(),
            "max_markets": max_markets,
            "only_resolved": only_resolved,
        },
        started_at=started_at,
    )
    run_uuid = UUID(run_context.run_id)

    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    _log(
        logger,
        logging.INFO,
        "gamma_outcomes_refresh_started",
        run_context,
        max_markets=max_markets,
        only_resolved=only_resolved,
    )

    client = GammaClient(
        GammaClientConfig(
            base_url=config.gamma_base_url,
            timeout_seconds=config.gamma_timeout_seconds,
            page_size=config.gamma_page_size,
        )
    )
    fetched_markets = client.iter_markets(max_markets=max_markets)
    sorted_markets = sorted(fetched_markets, key=_stable_market_sort_key)

    stats = {
        "errors": 0,
        "markets_seen": len(sorted_markets),
        "markets_skipped_missing_id": 0,
        "outcomes_upserted": 0,
        "resolved_upserted": 0,
        "unresolved_upserted": 0,
        "unresolved_skipped": 0,
    }

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        repository = OutcomesRepository(connection)
        repository.insert_run(
            run_id=run_uuid,
            run_type=JOB_NAME,
            decision_ts=started_at,
            ingest_epsilon_seconds=config.ingest_epsilon_seconds,
            code_version=run_context.code_version,
            config_hash=run_context.config_hash,
        )

        for payload in sorted_markets:
            market_id = _extract_market_id(payload)
            if market_id is None:
                stats["markets_skipped_missing_id"] += 1
                _log(
                    logger,
                    logging.WARNING,
                    "gamma_outcomes_market_skipped_missing_id",
                    run_context,
                    market_payload=payload,
                )
                continue

            resolved, outcome, resolved_ts, resolver_source = infer_market_outcome(payload)
            if only_resolved and not resolved:
                stats["unresolved_skipped"] += 1
                continue

            record = MarketOutcomeRecord(
                market_id=market_id,
                resolved=resolved,
                outcome=outcome,
                resolved_ts=resolved_ts,
                resolver_source=resolver_source,
                ingested_at=started_at,
            )
            try:
                repository.upsert_market_outcome(record)
            except psycopg.Error as exc:
                stats["errors"] += 1
                _log(
                    logger,
                    logging.ERROR,
                    "gamma_outcome_upsert_failed",
                    run_context,
                    market_id=market_id,
                    resolver_source=resolver_source,
                    error=str(exc),
                )
                continue

            stats["outcomes_upserted"] += 1
            if resolved:
                stats["resolved_upserted"] += 1
            else:
                stats["unresolved_upserted"] += 1

    elapsed_seconds = int((datetime.now(tz=UTC) - started_at).total_seconds())
    _log(
        logger,
        logging.INFO,
        "gamma_outcomes_refresh_completed",
        run_context,
        elapsed_seconds=elapsed_seconds,
        **stats,
    )
    return stats


def load_gamma_outcomes_refresh_config() -> GammaOutcomesRefreshConfig:
    gamma_base_url = os.getenv("GAMMA_BASE_URL", DEFAULT_GAMMA_BASE_URL)
    gamma_timeout_seconds = _load_positive_int(
        "GAMMA_TIMEOUT_SECONDS",
        DEFAULT_GAMMA_TIMEOUT_SECONDS,
    )
    gamma_page_size = _load_positive_int("GAMMA_PAGE_SIZE", DEFAULT_GAMMA_PAGE_SIZE)
    ingest_epsilon_seconds = _load_positive_int("INGEST_EPSILON_SECONDS", 300)

    return GammaOutcomesRefreshConfig(
        gamma_base_url=gamma_base_url,
        gamma_timeout_seconds=gamma_timeout_seconds,
        gamma_page_size=gamma_page_size,
        ingest_epsilon_seconds=ingest_epsilon_seconds,
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh market outcomes from Polymarket Gamma.")
    parser.add_argument(
        "--max-markets",
        type=int,
        default=None,
        help="Optional max number of markets to process.",
    )
    parser.add_argument(
        "--only-resolved",
        action="store_true",
        help="Persist only rows inferred as resolved outcomes.",
    )
    return parser.parse_args(argv)


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


def _extract_market_id(payload: Mapping[str, Any]) -> str | None:
    value = (
        payload.get("market_id")
        or payload.get("marketId")
        or payload.get("id")
        or payload.get("condition_id")
        or payload.get("conditionId")
    )
    return _as_text(value)


def _stable_market_sort_key(payload: Mapping[str, Any]) -> tuple[str, str, str]:
    market_id = _extract_market_id(payload) or ""
    slug = _as_text(payload.get("slug")) or ""
    title = _as_text(payload.get("title") or payload.get("question")) or ""
    return market_id, slug, title


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


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
