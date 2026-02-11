# Guardrails

## As-of time (mandatory)
- For any decision timestamp `t`, use only information available as of that moment.
- News records must satisfy both constraints:
  - `published_at <= t`
  - `ingested_at <= t + epsilon`
- Any violation triggers `no_trade` with `leak` reason.

## As-of query rules
- Always anchor filtering to a `runs` row (`decision_ts`, `ingest_epsilon_seconds`).
- For market-event tables (`trades`, `orderbook_snapshots`, `candles`):
  - `event_ts <= decision_ts`
  - `ingested_at <= decision_ts + epsilon`
- For news tables (`articles`, claim evidence sourced from articles):
  - `published_at <= decision_ts`
  - `ingested_at <= decision_ts + epsilon`
- Canonical SQL predicate pattern:
  ```sql
  <event_or_published_col> <= r.decision_ts
  AND ingested_at <= (r.decision_ts + make_interval(secs => r.ingest_epsilon_seconds))
  ```
- This contract is smoke-tested in `tests/test_asof_contract.py`.
- Catalog ingestion note:
  - Gamma catalog refresh stores ingestion metadata (`audit.ingested_at`)
    at write time for auditability.
  - News ingestion stores `published_at` and run-level `ingested_at`
    on every `articles` row (no null timestamps).
  - News raw evidence is persisted in `articles.raw.gdelt` and
    `articles.raw.crawler` for replay/audit.
  - CLOB REST ingestion stores a single run-level `ingested_at` timestamp
    across `orderbook_snapshots`, `trades`, and `candles`.
  - As-of eligibility is still decided downstream with `decision_ts + epsilon`
    filters in backtest/forecast selection.

## Feature snapshots as-of contract
- Microstructure feature snapshots must be computed with the same as-of contract:
  - input market data must satisfy:
    - `event_ts <= decision_ts`
    - `ingested_at <= decision_ts + epsilon`
- Window definitions for microstructure v1:
  - trades 5m window: `[decision_ts - 5m, decision_ts]`
  - candles 1h window: `[decision_ts - 1h, decision_ts]` on `start_ts`
  - last snapshot/trade selection uses deterministic tie-break:
    `event_ts DESC`, `ingested_at DESC`, stable id DESC.
- Feature persistence is idempotent:
  - store as `feature_snapshots` with deterministic token-scoped
    `feature_set_version` and upsert behavior.

## Backtest harness as-of contract
- Walk-forward backtests must reconstruct examples at each `decision_ts`
  using only as-of-eligible market data:
  - `event_ts <= decision_ts`
  - `ingested_at <= decision_ts + epsilon`
- Baseline A input (`price_prob`) must be selected with the same as-of gating
  (orderbook `mid` preferred, last trade fallback).
- Labels are allowed only when the market has known resolution and
  `decision_ts < resolved_ts`.
- Every backtest run must persist reproducibility metadata:
  - `dataset_hash`
  - `config_hash`
  - `code_version`
  - `feature_set`

## Selection bias control (market selector)
- Deep-dive market selection must always persist:
  - primary selector (`selector_v1`)
  - baseline comparators (`baseline_top_volume`, `baseline_random_stratified`)
- Baseline selectors are mandatory to monitor selection-bias drift.
- A comparative evaluation report is mandatory for each decision cycle:
  - selector vs both baselines on backtest metrics (`Brier`, `ECE`)
  - output must be produced even when one selector set is missing
    (`missing_selection` status in report).
- Selector scoring is deterministic:
  - stable feature extraction order
  - stable tie-breaks (`score`, liquidity, volume, market_id)
  - deterministic stratified-random seed derived from
    `sha256(decision_ts + config_hash)`.
- Selector as-of safety:
  - all market data inputs (`price_prob`, volume/liquidity signals) must satisfy:
    - `event_ts <= decision_ts`
    - `ingested_at <= decision_ts + epsilon`
  - feature snapshots must be selected with `asof_ts <= decision_ts`;
    payloads with explicit `ingest_bound_ts` above `decision_ts + epsilon`
    are considered ineligible.

## News ingestion semantics
- Primary-source configuration is loaded from `config/primary_sources.yaml`:
  - defaults define `is_primary`, `trust_score`, per-domain crawl `rps`,
    and `allow_subdomains`.
  - listed domains are upserted into `sources` at job start.
- URL canonicalization is deterministic and conservative:
  - lowercase host and drop leading `www.`
  - drop fragments
  - remove tracking params (`utm_*`, `gclid`, `gbraid`, `wbraid`,
    `fbclid`, `mc_cid`, `mc_eid`, `mkt_tok`)
  - keep non-tracking params and sort remaining query pairs.
- Dedupe policy:
  - hard dedupe on `canonical_url`.
  - soft dedupe on content hash (`sha256(normalized_text)`).
  - fallback soft dedupe on title hash with same domain and
    `published_at` within 24 hours.
  - action policy is explicit and deterministic:
    - `canonical_url` match => update existing row (no insert)
    - `content_hash` match => update existing row (no insert)
    - `title_hash + domain + window` match => update existing row (no insert)
  - updates merge raw payloads and fill missing fields only.
  - each dedupe update logs `dedupe_reason`:
    `canonical_url`, `content_hash`, or `title_window`.
- `published_at` policy for ingestion:
  - precedence: crawler `published_at` > GDELT `published_at` > none.
  - if both are missing, fallback to run `ingested_at` and mark
    `raw.ingest.unknown_published_at = true` with
    `raw.ingest.published_at_source = "ingested_at_fallback"`.
- Market linking v1 is deterministic and non-LLM:
  - lexicon built from `markets.title` and `markets.slug`.
  - score: `2*title_hits + body_hits + 0.5*slug_hits`.
  - topK=5 with stable tie-break (`score DESC`, `market_id ASC`).
  - tokenization uses a fixed stopword list to avoid drift across runs.

## LLM extractor contracts (schema-first)
- LLM extraction outputs are contract-first and must validate before use:
  - `schemas/claim_extract.v1.json`
  - `schemas/evidence_checklist.v1.json`
- Validation entrypoints:
  - `pmx.claims.validate_claim_extract(payload)`
  - `pmx.claims.validate_evidence_checklist(payload)`
- Validation combines JSON schema + deterministic custom guardrails:
  - max `80` raw claims per market payload
  - max `25` canonical claims per market payload (when provided)
  - max `10` source URLs per claim/checklist item
  - duplicate source URLs inside one claim/checklist item are rejected
- Payloads are canonicalized with stable key ordering before downstream use.
- Invalid payloads are rejected with deterministic error tuples:
  `code`, `path`, `reason`.
- Claim extraction runner (stub mode) must emit an audit-ready JSON artifact:
  - `artifacts/claim_extract/<run_id>_<market_id>.json`
  - required fields include:
    - `run_id`, `job_name`, `code_version`, `config_hash`
    - `decision_ts`, `ingest_epsilon_seconds`
    - `schema_versions`, `prompt_hash`
    - `input_article_ids`, `input_canonical_urls`
    - `validator_errors`, `no_trade_flags`, `payload`
- If extractor output is invalid, fallback is mandatory and deterministic:
  - set `no_trade_flags` to include `llm_invalid_output`
  - payload fallback shape includes empty claims plus validation errors:
    - `claims=[]`
    - `claims_raw=[]`
    - `errors=[{code,path,reason}, ...]`

## Claim graph v1 (canonicalization + echo controls)
- Input is validated claim payload (`claims_raw`) from schema v1.
- Canonicalization is deterministic and capped:
  - max `25` canonical claims per market (`canonicalize_claims(..., max_canonical=25)`).
  - stable processing order:
    `published_at_min ASC`, `domain_min ASC`, `claim_id ASC`.
  - greedy clustering threshold:
    token Jaccard similarity `>= 0.5`.
- Canonical representative selection is deterministic:
  - choose the shortest claim text in cluster, tie-break by `claim_id`.
- Source aggregation rules:
  - source URLs are canonicalized and deduped.
  - max `10` primary sources retained per canonical claim.
- Echo/diversity metrics per canonical claim:
  - `unique_domains`
  - `primary_domains`
  - `diversity_score` in `[0,1]`
  - `echo_penalty` in `[0,1]` (higher means stronger single-domain concentration).
- Mapping output (`claim_id -> canonical_claim_id`) and dropped claim IDs
  are deterministic and reproducible.

## CLOB ingestion semantics
- `--since-ts` is inclusive and normalized to UTC before filtering:
  - trades: keep `event_ts >= since_ts`
  - candles: keep `start_ts >= since_ts`
- Orderbook snapshot timestamp policy:
  - if CLOB payload has a parseable timestamp, use it as `event_ts`
  - otherwise fallback to run ingestion timestamp (`run_ingested_at`)
    so snapshots remain auditable and replayable.
- Orderbook normalization must be deterministic:
  - bids sorted by `price DESC`
  - asks sorted by `price ASC`
  - price/size quantized to 8 decimals
  - optional deterministic depth cap (`CLOB_ORDERBOOK_DEPTH`)
- Trade identity fallback:
  - if upstream `seq` and hash/id are both missing, derive
    deterministic `trade_hash` with SHA-256 from canonical trade fields
    (`token_id`, `event_ts`, `price`, `size`, `side`, optional stable extras).
- WSS + reconcile policy:
  - WSS stream is best-effort (can drop/out-of-order messages).
  - REST is the source of truth for reconciliation windows.
  - `supports_seq` definition:
    - `supports_seq = true` when protocol evidence shows any seq-like field
      (`seq`, `sequence`, `offset`) in observed WSS messages.
    - Evidence is derived from both:
      - offline fixtures (`tests/fixtures/wss/*.json`)
      - manual probe output (`scripts/wss_probe.py`)
    - Current repository expectation:
      `supports_seq = true` for shipped fixtures.
  - Gap detection strategy:
    - seq-based when `supports_seq = true` and configured seq fields are non-empty:
      field extraction precedence is:
      `CLOB_WSS_SEQ_FIELD` (legacy override, if set) else
      `CLOB_WSS_SEQ_FIELDS` (default `seq,sequence,offset`).
      If `CLOB_WSS_SEQ_FIELD` is explicitly empty/whitespace, it disables
      seq-based mode and is never treated as a field name.
      Gap when extracted seq is present and `seq != last_seq + 1`.
    - heuristic when `supports_seq = false`, configured fields are empty,
      or a specific message has no parseable seq-like field:
      gap if stream becomes stale (`now - last_trade_ts > CLOB_RECONCILE_GAP_SECONDS`);
      if no stream timestamp exists, reconcile still runs every tick.
    - `msg_id` and `event_id` remain diagnostic-only; they do not drive
      seq-based gap detection.
  - Mismatch strategy:
    - compute mid divergence in bps between stream and REST snapshots;
      mismatch when divergence exceeds `CLOB_RECONCILE_MISMATCH_BPS`.
  - Per-token state tracked and logged monotonically:
    - `last_seq`
    - `last_trade_ts`
    - `last_book_ts`
    - `last_reconcile_ts`
  - Reconciler emits structured audit logs for:
    - `reconcile_gap` (missing sequence / timestamp regression)
    - `reconcile_mismatch` (top-of-book mismatch)
    - `wss_reconnect` (disconnect/retry lifecycle)
  - Gap/mismatch audit payload includes:
    - `action_taken`
    - `window_start`
    - `window_end`
    - `rest_calls`
    - `rows_upserted`
  - Probe command (manual, no DB writes):
    - `python scripts/wss_probe.py --token-ids tokenA,tokenB --max-messages 200 --out tmp/wss_probe.jsonl`
    - `TOKEN_IDS="tokenA,tokenB" python scripts/wss_probe.py --max-messages 200 --out tmp/wss_probe.jsonl` (POSIX)
    - `$env:TOKEN_IDS="tokenA,tokenB"; python scripts/wss_probe.py --max-messages 200 --out tmp/wss_probe.jsonl` (PowerShell)
    - `python scripts/wss_probe.py --subscribe-payload-json path/to/subscribe_payload.json --max-messages 200 --out tmp/wss_probe.jsonl`
  - Fixture refresh workflow:
    - capture probe output in JSONL
    - redact sensitive values preserving structure
    - export 3-5 representative messages into `tests/fixtures/wss/*.json`
    - re-run protocol fixture test to validate `supports_seq`
      and update expected value if protocol support changes.
  - Repair actions must remain idempotent via existing upsert keys.

## Timestamp naming conventions
- `event_ts`: when the market event happened externally (trade, book snapshot, candle start).
- `published_at`: when a news article was published externally.
- `ingested_at`: when data entered PMX ingestion/storage.
- `decision_ts`: run decision timestamp in `runs`.
- `asof_ts`: materialized feature snapshot timestamp for model input bundles.

## Anti-leak controls
- Do not join or aggregate on fields that include post-`t` information.
- Keep event-time filters explicit in queries and feature jobs.
- Store `asof_ts` in every run artifact for audit replay.

## Anti-echo controls
- Claim graph deduplication is mandatory before scoring evidence.
- Penalize repeated claims from correlated outlets.
- Enforce minimum source diversity from primary sources whenever available.

## Idempotency
- Jobs must expose a deterministic idempotency key boundary.
- Re-running the same job with identical config and as-of inputs
  must not create duplicate side effects.
- Persist idempotency decisions in audit logs.
- Gamma catalog refresh upserts by natural keys:
  - `markets.market_id`
  - `market_tokens (market_id, outcome)` with `unique(token_id)` enforcement
- If `token_id` is already owned by another market, log a data-quality
  issue and continue the run without crashing.
- CLOB REST ingestion upserts by market-data natural keys:
  - `orderbook_snapshots (token_id, event_ts)`
  - `trades` via `trades_idempotency_uk` (`token_id`, `event_ts`, `seq_norm`, `trade_hash_norm`)
  - `candles (token_id, interval, start_ts)`

## Audit raw payload
- Catalog ingestion persists raw Gamma payload alongside rule parser output
  in `markets.rule_parse_json` with `audit.ingested_at`.
- Until a dedicated raw-ingestion table is introduced, `rule_parse_json` is a
  temporary container for both parser stub output and audit payload under the
  `audit.*` namespace.
- This provides a reproducible input snapshot for catalog-level audits,
  even before dedicated raw ingestion tables are introduced.

## Determinism
- Configuration hashing must be deterministic (stable canonical serialization).
- Randomness must be seeded or explicitly represented in run metadata.
- Tests must avoid hidden real-time dependencies (freeze/mocked clock or explicit timestamps).

## Audit bundle (high level)
Each forecast output is expected to include:
- Calibrated probability (`p_cal`) and 50/90 intervals.
- Driver summary and resolution-aware evidence checklist.
- No-trade flags with rationale.
- Reproducibility metadata (`run_id`, `code_version`, `config_hash`, input snapshot references).

## Forecast core v1 guardrails
- Forecast pipeline must read examples only from the as-of dataset builder
  (`pmx.backtest.asof_dataset`) to preserve leak prevention.
- Each forecast run artifact must include:
  - `dataset_hash`
  - `model_hash`
  - `calibration_hash`
  - `uncertainty_hash`
  - `forecast_payload_hash`
- Calibration + uncertainty are walk-forward only:
  - calibration window uses rows with `train_decision_ts < current_decision_ts`
  - conformal split is deterministic by time order (no random split)
- Interval quality is always reported in artifacts:
  - `coverage_50`, `coverage_90`, `sharpness_50`, `sharpness_90`
  - expected long-run target is approximate nominal coverage
    (`coverage_50 ~= 0.50`, `coverage_90 ~= 0.90`) and must be monitored.
