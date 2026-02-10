# Database Schema v1

## Overview
Schema v1 is managed by Alembic migration `0001_schema_v1`.

Core areas:
- Audit/runs
- Market catalog + token mapping
- Time-series microstructure (`trades`, `orderbook_snapshots`, `candles`)
- News/sources/articles
- Claim graph
- Feature snapshots
- Selection logs
- Forecast outputs + intervals + drivers + no-trade flags

## Partitioning strategy
High-volume tables are natively partitioned by time:
- `trades` by `event_ts`
- `orderbook_snapshots` by `event_ts`
- `candles` by `start_ts`

Current bootstrap partition:
- `*_p_default` covering `2020-01-01` to `2030-01-01`

This avoids ingestion failures before monthly partition automation is introduced.

## Indexing policy for partitioned tables
- Define required indexes on the parent partitioned tables first:
  - `trades`: `(token_id, event_ts DESC)`, `(token_id, ingested_at DESC)`
  - `orderbook_snapshots`: `(token_id, event_ts DESC)`, `(token_id, ingested_at DESC)`
  - `candles`: `(token_id, start_ts DESC)`, `(token_id, ingested_at DESC)`
- Parent indexes are the canonical contract; each newly added partition
  must be checked for index consistency before ingestion is enabled.

Verification checklist for every new monthly partition:
1. Confirm partition is attached to the expected parent:
   ```sql
   SELECT parent.relname AS parent_table, child.relname AS partition_table
   FROM pg_inherits AS i
   JOIN pg_class AS parent ON parent.oid = i.inhparent
   JOIN pg_class AS child ON child.oid = i.inhrelid
   WHERE child.relname LIKE 'trades_p_%'
      OR child.relname LIKE 'orderbook_snapshots_p_%'
      OR child.relname LIKE 'candles_p_%';
   ```
2. Confirm required indexes exist for the new partition:
   ```sql
   SELECT schemaname, tablename, indexname
   FROM pg_indexes
   WHERE schemaname = 'public'
     AND (
       tablename LIKE 'trades_p_%'
       OR tablename LIKE 'orderbook_snapshots_p_%'
       OR tablename LIKE 'candles_p_%'
     )
   ORDER BY tablename, indexname;
   ```
3. Operational gate:
   new partition => verify required indexes before writing ingestion data.

## Future monthly partitions
Create partitions ahead of time (example for January 2027):

```sql
CREATE TABLE trades_p_2027_01
PARTITION OF trades
FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2027-02-01 00:00:00+00');

CREATE TABLE orderbook_snapshots_p_2027_01
PARTITION OF orderbook_snapshots
FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2027-02-01 00:00:00+00');

CREATE TABLE candles_p_2027_01
PARTITION OF candles
FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2027-02-01 00:00:00+00');
```

Operational rule:
- Keep at least 3 future monthly partitions pre-created.
- Keep one wide fallback partition only as safety net, not as long-term storage target.
