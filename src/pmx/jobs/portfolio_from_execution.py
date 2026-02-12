from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.execution.validate_artifact import validate_execution_artifact
from pmx.portfolio.artifact import PORTFOLIO_POLICY_VERSION, build_portfolio_artifact
from pmx.portfolio.canonical import canonical_hash
from pmx.portfolio.ledger import LedgerConfig, build_ledger
from pmx.portfolio.positions import apply_ledger_to_positions
from pmx.portfolio.validate_artifact import validate_portfolio_artifact
from pmx.portfolio.valuation import (
    MarkSource,
    build_reference_prices,
    mark_to_model,
    missing_reference_keys,
)

JOB_NAME = "portfolio_from_execution"


@dataclass(frozen=True, slots=True)
class PortfolioFromExecutionConfig:
    artifacts_root: str
    fee_bps: float
    fee_usd: float
    mark_source: MarkSource
    reference_prices_json: str | None

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "artifacts_root": self.artifacts_root,
            "fee_bps": self.fee_bps,
            "fee_usd": self.fee_usd,
            "mark_source": self.mark_source,
            "reference_prices_json": self.reference_prices_json,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_portfolio_from_execution_config(
        artifacts_root=args.artifacts_root,
        fee_bps=args.fee_bps,
        fee_usd=args.fee_usd,
        mark_source=args.mark_source,
        reference_prices_json=args.reference_prices_json,
    )
    run_portfolio_from_execution(
        execution_artifact_paths=_parse_execution_artifact_paths(args.execution_artifacts),
        config=config,
        nonce=args.nonce,
    )
    return 0


def run_portfolio_from_execution(
    *,
    execution_artifact_paths: list[Path],
    config: PortfolioFromExecutionConfig,
    nonce: str | None = None,
) -> dict[str, Any]:
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    execution_inputs = _load_execution_inputs(execution_artifact_paths)
    ordered_artifacts = [artifact for _, artifact in execution_inputs]
    input_paths = [path for path, _ in execution_inputs]

    input_execution_hashes = [_resolve_execution_hash(artifact) for artifact in ordered_artifacts]
    started_at = _resolve_started_at(ordered_artifacts)
    external_reference_prices = _load_reference_prices(config.reference_prices_json)

    deterministic_nonce = nonce or canonical_hash(
        {
            "input_execution_hashes": input_execution_hashes,
            "input_execution_paths": input_paths,
            "params": config.as_hash_dict(),
            "policy_version": PORTFOLIO_POLICY_VERSION,
        }
    )
    run_context = build_run_context(
        JOB_NAME,
        {
            "input_execution_hashes": input_execution_hashes,
            "input_execution_paths": input_paths,
            "params": config.as_hash_dict(),
            "policy_version": PORTFOLIO_POLICY_VERSION,
        },
        started_at=started_at,
        nonce=deterministic_nonce,
    )
    _log(
        logger,
        logging.INFO,
        "portfolio_from_execution_started",
        run_context,
        execution_artifact_paths=input_paths,
        n_execution_inputs=len(input_paths),
    )

    ledger_result = build_ledger(
        ordered_artifacts,
        LedgerConfig(fee_bps=config.fee_bps, fee_usd=config.fee_usd),
    )
    positions = apply_ledger_to_positions(ledger_result.entries)
    reference_prices, reference_warnings = build_reference_prices(
        ordered_artifacts,
        mark_source=config.mark_source,
        external_prices=external_reference_prices,
    )
    missing_keys = missing_reference_keys(positions, reference_prices)
    if missing_keys:
        missing_str = ", ".join(missing_keys)
        raise ValueError(
            "Missing reference prices for positions: "
            f"{missing_str}. Provide --reference-prices-json or compatible mark source."
        )
    valuation = mark_to_model(
        positions,
        reference_prices=reference_prices,
        mark_source=config.mark_source,
    )
    artifact = build_portfolio_artifact(
        run_context=run_context,
        execution_artifacts=ordered_artifacts,
        params=config.as_hash_dict(),
        ledger_entries=ledger_result.entries,
        positions=positions,
        valuation=valuation,
        quality_flags=list(ledger_result.quality_flags),
        quality_warnings=[*ledger_result.quality_warnings, *reference_warnings],
        policy_version=PORTFOLIO_POLICY_VERSION,
    )
    artifact["counts"] = {
        **artifact.get("counts", {}),
        **ledger_result.counts,
    }

    artifact_errors = validate_portfolio_artifact(artifact)
    if artifact_errors:
        raise ValueError(f"Invalid portfolio artifact: {artifact_errors[0]}")

    artifact_path = _write_artifact(config.artifacts_root, run_context.run_id, artifact)
    _log(
        logger,
        logging.INFO,
        "portfolio_from_execution_completed",
        run_context,
        artifact_path=str(artifact_path),
        n_execution_inputs=len(input_paths),
        n_ledger_entries=len(artifact["ledger_entries"]),
        n_positions=len(artifact["positions"]),
        ledger_hash=str(artifact["ledger_hash"]),
        positions_hash=str(artifact["positions_hash"]),
        valuation_hash=str(artifact["valuation_hash"]),
        portfolio_payload_hash=str(artifact["portfolio_payload_hash"]),
    )
    artifact["artifact_path"] = str(artifact_path)
    return artifact


def load_portfolio_from_execution_config(
    *,
    artifacts_root: str | None,
    fee_bps: float | None,
    fee_usd: float | None,
    mark_source: str | None,
    reference_prices_json: str | None,
) -> PortfolioFromExecutionConfig:
    resolved_root = artifacts_root or os.getenv("PORTFOLIO_ARTIFACTS_ROOT") or "artifacts"
    resolved_fee_bps = (
        fee_bps if fee_bps is not None else _load_non_negative_float("PORTFOLIO_FEE_BPS", 0.0)
    )
    resolved_fee_usd = (
        fee_usd if fee_usd is not None else _load_non_negative_float("PORTFOLIO_FEE_USD", 0.0)
    )
    resolved_mark_source = _parse_mark_source(
        mark_source or os.getenv("PORTFOLIO_MARK_SOURCE") or "execution_price"
    )
    resolved_reference_prices_json = (
        reference_prices_json
        if reference_prices_json is not None
        else _optional_env_text("PORTFOLIO_REFERENCE_PRICES_JSON")
    )
    return PortfolioFromExecutionConfig(
        artifacts_root=resolved_root,
        fee_bps=resolved_fee_bps,
        fee_usd=resolved_fee_usd,
        mark_source=resolved_mark_source,
        reference_prices_json=resolved_reference_prices_json,
    )


def _parse_execution_artifact_paths(raw_values: list[str]) -> list[Path]:
    parts: list[str] = []
    for raw in raw_values:
        for split_value in raw.split(","):
            value = split_value.strip()
            if value:
                parts.append(value)
    if not parts:
        raise ValueError("At least one execution artifact path is required")
    return [Path(value) for value in parts]


def _load_execution_inputs(paths: list[Path]) -> list[tuple[str, dict[str, Any]]]:
    loaded: list[tuple[str, dict[str, Any]]] = []
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"Execution artifact not found: {path}")
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        if not isinstance(raw, dict):
            raise ValueError(f"Execution artifact root must be an object: {path}")
        errors = validate_execution_artifact(raw)
        if errors:
            raise ValueError(f"Invalid execution artifact {path}: {errors[0]}")
        normalized_path = str(path.resolve())
        loaded.append((normalized_path, raw))
    loaded.sort(key=lambda item: (item[0], str(item[1].get("run_id", ""))))
    return loaded


def _resolve_execution_hash(execution_artifact: Mapping[str, Any]) -> str:
    raw = execution_artifact.get("execution_payload_hash")
    if isinstance(raw, str):
        normalized = raw.strip()
        if len(normalized) == 64 and all(char in "0123456789abcdef" for char in normalized):
            return normalized
    return canonical_hash(execution_artifact)


def _resolve_started_at(execution_artifacts: list[dict[str, Any]]) -> datetime:
    candidates: list[datetime] = []
    for artifact in execution_artifacts:
        parsed = _parse_optional_datetime(artifact.get("generated_at_utc"))
        if parsed is not None:
            candidates.append(parsed)
    if candidates:
        return min(candidates)
    return datetime.now(UTC)


def _parse_optional_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _load_reference_prices(path: str | None) -> dict[str, Any] | None:
    if path is None:
        return None
    price_path = Path(path)
    if not price_path.exists():
        raise FileNotFoundError(f"Reference prices JSON not found: {price_path}")
    with price_path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError("Reference prices JSON root must be an object")
    return raw


def _write_artifact(artifacts_root: str, run_id: str, artifact: dict[str, Any]) -> Path:
    root = Path(artifacts_root) / "portfolios"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic portfolio artifact from execution artifacts."
    )
    parser.add_argument(
        "--execution-artifacts",
        action="append",
        required=True,
        help=(
            "Execution artifact path(s). Repeat the flag or pass comma-separated "
            "values in a single flag."
        ),
    )
    parser.add_argument(
        "--artifacts-root",
        default=None,
        help="Artifacts root directory. Output is written under <root>/portfolios.",
    )
    parser.add_argument("--fee-bps", type=float, default=None, help="Fee in basis points.")
    parser.add_argument("--fee-usd", type=float, default=None, help="Flat fee (USD) per fill.")
    parser.add_argument(
        "--mark-source",
        choices=["execution_price", "execution_p_cal", "execution_price_prob"],
        default=None,
        help="Reference mark source. Falls back to --reference-prices-json when missing.",
    )
    parser.add_argument(
        "--reference-prices-json",
        default=None,
        help="Optional JSON map for missing reference prices (token or token|side keys).",
    )
    parser.add_argument(
        "--nonce",
        default=None,
        help="Optional nonce override for deterministic run_id generation.",
    )
    return parser.parse_args(argv)


def _parse_mark_source(raw: str) -> MarkSource:
    value = raw.strip()
    allowed = {"execution_price", "execution_p_cal", "execution_price_prob"}
    if value not in allowed:
        raise ValueError(f"Unsupported mark_source: {raw!r}")
    return cast(MarkSource, value)


def _load_non_negative_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if parsed < 0.0:
        raise ValueError(f"{name} must be >= 0, got {parsed}")
    return parsed


def _optional_env_text(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


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
