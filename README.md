# PMX Research Scaffold

Research-grade Python scaffold for deterministic and auditable market forecasting workflows.

See `CONTRIBUTING.md` for verification and merge policy.

## Scope of this milestone
- Repository scaffold and quality guardrails.
- CI checks (lint, typecheck, tests).
- Local PostgreSQL for development.
- Run context primitives for reproducibility (`run_id`, `config_hash`, `code_version`).

No real ingestion/model/backtest implementation is included in this milestone.

## Quickstart
1. Copy environment variables:
   ```powershell
   Copy-Item .env.example .env
   ```
2. Start PostgreSQL:
   ```powershell
   docker compose up -d
   docker compose ps
   ```
3. Install dependencies (default):
   ```powershell
   python -m pip install -e ".[dev]"
   ```
   Faster alternative with uv:
   ```powershell
   uv pip install --system -e ".[dev]"
   ```
4. Run quality gates:
   ```powershell
   ruff format --check .
   ruff check .
   mypy src
   pytest -q
   ```
   Single entrypoint:
   ```powershell
   python scripts/verify.py
   ```

## Make / Just shortcuts
- `make install-pip` or `just install-pip`
- `make install` or `just install`
- `make lint` or `just lint`
- `make typecheck` or `just typecheck`
- `make test` or `just test`
- `make verify` or `just verify`
- `make check` or `just check`

## Migrations bootstrap
- Alembic is initialized under `migrations/`.
- For DB-backed tests, set `DATABASE_URL` or `APP_DATABASE_URL`
  (`APP_DATABASE_URL` recommended).
- DB tests resolve DSN in this order:
  `DATABASE_URL` then `APP_DATABASE_URL`.
- Example DSN:
  `postgresql+psycopg://pmx:pmx_dev_password@localhost:5432/pmx`
- Apply schema:
  ```powershell
  python -m alembic upgrade head
  ```
- See `docs/db.md` for partition strategy and future monthly partition creation.

## Gamma catalog refresh
- Required env:
  - `APP_DATABASE_URL` or `DATABASE_URL`
    (lookup order in code: `DATABASE_URL` then `APP_DATABASE_URL`)
  - `GAMMA_BASE_URL` (default: `https://gamma-api.polymarket.com`)
  - `GAMMA_TIMEOUT_SECONDS` (default: `20`)
  - `GAMMA_PAGE_SIZE` (default: `200`)
- Run full refresh:
  ```powershell
  python -m pmx.jobs.gamma_catalog_refresh
  ```
- Run incremental refresh:
  ```powershell
  python -m pmx.jobs.gamma_catalog_refresh --since-updated-at 2026-01-01T00:00:00Z
  ```
- Dev limit:
  ```powershell
  python -m pmx.jobs.gamma_catalog_refresh --max-markets 100
  ```
- Timestamp semantics:
  - Catalog ingestion writes a run-level ingestion timestamp (`audit.ingested_at`)
    for reproducibility and audit.
  - As-of gating (`event/published <= decision_ts` and
    `ingested_at <= decision_ts + epsilon`) is enforced later by
    backtest/forecast selection logic, not by this catalog ingestion job.
- Temporary audit namespace:
  - `markets.rule_parse_json` currently stores both parser stub output and
    audit payload under `audit.*` (for example `audit.gamma_raw`).

## News ingestion v1 (GDELT + whitelist crawler)
- Required env:
  - `APP_DATABASE_URL` or `DATABASE_URL`
  - `GDELT_BASE_URL` (default: `https://api.gdeltproject.org/api/v2/doc/doc`)
  - `GDELT_TIMEOUT_SECONDS` (default: `20`)
  - `GDELT_MAX_RETRIES` (default: `4`)
  - `GDELT_BACKOFF_SECONDS` (default: `0.5`)
  - `GDELT_MAX_RECORDS` (default: `250`)
  - `NEWS_PRIMARY_SOURCES_CONFIG` (default: `config/primary_sources.yaml`)
  - `NEWS_CRAWLER_CONNECT_TIMEOUT_SECONDS` (default: `5`)
  - `NEWS_CRAWLER_READ_TIMEOUT_SECONDS` (default: `15`)
  - `NEWS_CRAWLER_MAX_RETRIES` (default: `3`)
  - `NEWS_CRAWLER_BACKOFF_SECONDS` (default: `0.5`)
- Run:
  ```powershell
  python -m pmx.jobs.news_ingest --since-published 2026-01-01T00:00:00Z --max-articles 200 --max-per-domain 20 --crawl-primary
  ```
- Disable crawler:
  ```powershell
  python -m pmx.jobs.news_ingest --since-published 2026-01-01T00:00:00Z --no-crawl-primary
  ```
- Notes:
  - Primary-source whitelist is loaded from `config/primary_sources.yaml` and upserted into `sources`.
  - URL canonicalization is conservative and strips tracking params (`utm_*`, `fbclid`, `gclid`, etc.).
  - Dedupe policy:
    - hard dedupe on `canonical_url`;
    - soft dedupe on `content_hash` updates the existing row (no insert);
    - fallback soft dedupe on `title_hash + same domain + published_at within 24h` updates the existing row (no insert).
    - every dedupe update logs `dedupe_reason` in structured logs:
      `canonical_url`, `content_hash`, `title_window`.
  - Raw audit payloads are persisted in `articles.raw`:
    - `raw.gdelt` for source payload;
    - `raw.crawler` for crawler response/extraction metadata;
    - `raw.ingest` for timestamp policy metadata (`published_at_source`, `unknown_published_at`).
  - `published_at` policy is explicit:
    - prefer crawler `published_at`;
    - fallback to GDELT `published_at`;
    - if both missing, fallback to run `ingested_at` and mark `raw.ingest.unknown_published_at=true`.
  - As-of safety is preserved by always storing `published_at` and run-level `ingested_at`.
  - Market linking is deterministic (`score = 2*title_hits + body_hits + 0.5*slug_hits`, topK=5, stable tie-break) with a fixed stopword list in tokenization.

## CLOB REST ingestion (time-series)
- Required env:
  - `APP_DATABASE_URL` or `DATABASE_URL`
    (lookup order in code: `DATABASE_URL` then `APP_DATABASE_URL`)
  - `CLOB_BASE_URL` (default: `https://clob.polymarket.com`)
  - `CLOB_TIMEOUT_SECONDS` (default: `20`)
  - `CLOB_RATE_LIMIT_RPS` (default: `5`)
  - `CLOB_ORDERBOOK_DEPTH` (optional deterministic top-of-book depth)
  - `CLOB_API_KEY` (optional, only if endpoint requires auth)
- Run:
  ```powershell
  python -m pmx.jobs.clob_ingest_rest --max-tokens 100 --since-ts 2026-01-01T00:00:00Z --interval 1m
  ```
- Notes:
  - Token ingestion order is deterministic (`token_id` sorted).
  - `--since-ts` is inclusive:
    - trades: include rows with `event_ts >= since_ts`
    - candles: include rows with `start_ts >= since_ts`
  - A single run-level `ingested_at` timestamp is applied to all inserted rows
    in `orderbook_snapshots`, `trades`, and `candles`.
  - Orderbook snapshot `event_ts` policy:
    - use API timestamp when available and parseable
    - fallback to run ingestion timestamp (`run_ingested_at`) when missing.
  - Orderbook normalization is deterministic:
    - bids sorted by price desc, asks sorted by price asc
    - price/size quantized to 8 decimals
    - optional depth cap via `CLOB_ORDERBOOK_DEPTH`.
  - Trade identity fallback:
    - when both `seq` and upstream hash/id are missing, a deterministic
      SHA-256 hash is derived from canonical trade fields to stabilize
      idempotent upsert keys.
  - As-of gating is applied downstream by `decision_ts + epsilon` filters.

## CLOB WSS listener + reconcile
- Required env:
  - `APP_DATABASE_URL` or `DATABASE_URL`
  - `CLOB_WSS_URL` (default: `wss://clob.polymarket.com/ws`)
  - `CLOB_WSS_TIMEOUT_SECONDS` (default: `20`)
  - `CLOB_WSS_MAX_RECONNECTS` (default: `8`)
  - `CLOB_WSS_BACKOFF_SECONDS` (default: `0.5`)
  - `CLOB_WSS_MAX_BACKOFF_SECONDS` (default: `30`)
  - `CLOB_WSS_SEQ_FIELDS` (default: `seq,sequence,offset`)
  - `CLOB_WSS_SEQ_FIELD` (legacy override; if set, forces a single field, empty disables seq-based detection)
  - `CLOB_RECONCILE_GAP_SECONDS` (default: `60`)
  - `CLOB_RECONCILE_MISMATCH_BPS` (default: `10`)
  - REST envs from the previous section (used by reconciler).
- Run:
  ```powershell
  python -m pmx.jobs.clob_wss_listener `
    --max-tokens 100 `
    --reconcile-every-seconds 60 `
    --since-ts 2026-01-01T00:00:00Z `
    --run-seconds 300
  ```
- Explicit token list:
  ```powershell
  python -m pmx.jobs.clob_wss_listener --token-ids tokenA,tokenB --reconcile-every-seconds 60
  ```
- Notes:
  - WSS ingestion is best-effort and can be out-of-order or incomplete.
  - REST is treated as source of truth during reconcile windows.
  - Gap detection strategy:
    - seq-based mode (active when configured seq fields include monotonic fields):
      extraction order is `CLOB_WSS_SEQ_FIELD` (legacy, if set) otherwise
      `CLOB_WSS_SEQ_FIELDS` (default `seq,sequence,offset`), and gap when
      `seq != last_seq + 1`.
      If `CLOB_WSS_SEQ_FIELD` is explicitly set to empty/whitespace, it
      disables seq-based detection (it is never interpreted as a field name).
    - heuristic mode (no configured seq fields, or event has no seq-like value):
      gap when stream is stale versus `CLOB_RECONCILE_GAP_SECONDS`; if no
      stream trade timestamp is available, reconcile still runs every tick.
    - `msg_id`/`event_id` are diagnostic-only and are not used for gap detection.
  - Mismatch strategy:
    - top-of-book mismatch when REST mid diverges from stream mid by more than
      `CLOB_RECONCILE_MISMATCH_BPS` basis points.
  - Per-token state is monotonic and logged periodically:
    - `last_seq`, `last_trade_ts`, `last_book_ts`, `last_reconcile_ts`
  - Reconcile emits structured audit events:
    - `reconcile_gap`
    - `reconcile_mismatch`
    - `wss_reconnect`
  - Reconcile gap/mismatch logs include:
    - `action_taken`, `window_start`, `window_end`, `rest_calls`, `rows_upserted`
  - Repairs are idempotent because writes use DB upsert constraints.

## Micro feature store v1 (as-of snapshots)
- Required env:
  - `APP_DATABASE_URL` or `DATABASE_URL`
  - `INGEST_EPSILON_SECONDS` (default: `300`)
- Run:
  ```powershell
  python -m pmx.jobs.compute_micro_features --decision-ts 2026-02-11T12:00:00Z --max-tokens 200 --epsilon-seconds 300
  ```
- Explicit token list:
  ```powershell
  python -m pmx.jobs.compute_micro_features --token-ids tokenA,tokenB --decision-ts 2026-02-11T12:00:00Z
  ```
- Notes:
  - As-of input gating is enforced on every query:
    - `event_ts <= decision_ts`
    - `ingested_at <= decision_ts + epsilon`
  - Persisted feature set is `micro_v1`, token-scoped via deterministic
    `feature_set_version` (`micro_v1:token:<token_id>`) to keep idempotent
    snapshots per token without schema changes.
  - Current microstructure features:
    - `mid_price`, `spread_bps`
    - `top_depth_bid`, `top_depth_ask`, `book_imbalance_1`
    - `last_trade_price`, `last_trade_size`
    - `trade_count_5m`, `volume_5m`, `return_5m`
    - `realized_vol_1h`, `stale_seconds_last_trade`, `stale_seconds_last_book`

## Backtest baselines A/B (walk-forward, as-of)
- Baseline A:
  - `p = price_prob` where `price_prob` is selected as-of from market data
    (`mid` from orderbook, fallback to last trade price).
- Baseline B:
  - deterministic logistic scorer over `micro_v1` features (no training step).
- Run:
  ```powershell
  python -m pmx.jobs.backtest_baselines `
    --token-ids tokenA,tokenB `
    --from 2026-01-01T00:00:00Z `
    --to 2026-01-03T00:00:00Z `
    --step-hours 4 `
    --epsilon-seconds 300 `
    --feature-set micro_v1
  ```
- Artifact:
  - `artifacts/backtests/<run_id>.json`
  - includes aggregate/per-token metrics (`Brier`, `ECE`, `sharpness`),
    plus `dataset_hash`, `config_hash`, `code_version`, and skip counters.
- See `docs/backtest.md` for details.

## Market selector v1 (accuracy-first)
- Run selector:
  ```powershell
  python -m pmx.jobs.select_markets `
    --decision-ts 2026-02-11T12:00:00Z `
    --epsilon-seconds 300 `
    --max-candidates 1500 `
    --k-deep 200
  ```
- Selector outputs:
  - deterministic `selector_v1` deep-dive set
  - baseline comparator sets:
    - `baseline_top_volume`
    - `baseline_random_stratified`
- Persistence:
  - writes `selection_runs` + `selection_items`
  - writes selector audit artifact in `artifacts/selector/<run_id>.json`
  - candidate score fallback artifact used if dedicated candidate table is absent.
- See `docs/selector.md` for scoring components, bucket mix, and constraints.

## WSS protocol probe (no DB writes)
- Goal:
  - capture real WSS message shapes and confirm seq-like fields.
- Current expectation from shipped fixtures:
  - `supports_seq = True` based on `tests/fixtures/wss/*.json`.
- Probe with explicit token ids:
  ```text
  python scripts/wss_probe.py --token-ids tokenA,tokenB --max-messages 200 --out wss_probe.jsonl
  ```
- Probe with `TOKEN_IDS` env:
  ```text
  # POSIX (bash/zsh)
  TOKEN_IDS="tokenA,tokenB" python scripts/wss_probe.py --max-messages 200 --out wss_probe.jsonl

  # PowerShell
  $env:TOKEN_IDS="tokenA,tokenB"; python scripts/wss_probe.py --max-messages 200 --out wss_probe.jsonl
  ```
- Probe with custom subscribe payload file:
  ```text
  python scripts/wss_probe.py --subscribe-payload-json path/to/subscribe_payload.json --max-messages 200 --out wss_probe.jsonl
  ```
- Fixture refresh workflow:
  - rerun probe when upstream protocol changes;
  - update `tests/fixtures/wss/*.json` with redacted representative messages;
  - rerun `tests/test_wss_protocol_fixtures.py`;
  - if protocol support changes, update expected `supports_seq`.

## Repository layout
```text
src/pmx/
  config/
  db/
  ingest/
  jobs/
  news/
  features/
  models/
  backtest/
  audit/
tests/
migrations/
docs/
```

## Determinism and guardrails
See `docs/guardrails.md` for as-of policy, anti-leak rules, idempotency, and audit expectations.

## Verification contract
- If tools are unavailable locally, checks are marked as not run/ skipped.
- CI in `.github/workflows/ci.yml` is the source of truth for merge readiness.
- CI must pass before merge (lint, typecheck, tests).

## Docker availability note
- `docker-compose.yml` is provided for local/dev environments where Docker is available.
- If Docker is unavailable locally, use an external Postgres and set
  `DATABASE_URL` or `APP_DATABASE_URL`.
