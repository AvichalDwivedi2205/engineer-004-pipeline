-- Serving store for dashboards. ClickHouse Cloud turns MergeTree-family engines into their
-- Shared* equivalents, so the same DDL runs unchanged in `clickhouse local` (tools/ddl_check.sh).

-- One row per accepted event. The sorting key is the dedupe key: a retry that outlived the
-- gate's 1-hour dedupe window, or a batch re-sent after a sink failure, carries the same
-- (tenant_id, event_id) and collapses on merge. Collapsing only happens inside a partition,
-- and each copy keeps its own received_at, so a late retry that crosses a month boundary
-- survives even FINAL. Exact reads and the nightly repair (schema/jobs.sql) therefore keep
-- the latest version with `ORDER BY version DESC LIMIT 1 BY tenant_id, event_id`.
-- Rows come from ingest_gate.gate.clickhouse_row, which lifts path, referrer and is_bot out
-- of properties.
CREATE TABLE events
(
    tenant_id         LowCardinality(String),
    event_id          String,
    anonymous_id      String,
    user_id           Nullable(String),
    type              LowCardinality(String),
    client_ts         Nullable(DateTime64(3, 'UTC')),
    received_at       DateTime64(3, 'UTC'),   -- collector stamp, else Kinesis arrival time
    event_time        DateTime64(3, 'UTC'),   -- client time when trusted, else received_at
    event_time_source LowCardinality(String),
    path              String DEFAULT '',
    referrer          String DEFAULT '',
    properties        String,                 -- JSON; PII already HMAC'd by the gate
    flags             Array(LowCardinality(String)),
    is_bot            Bool,
    ingest_node       LowCardinality(String),
    ingest_seq        UInt64,
    version           UInt64                  -- gate write time in ms; the latest copy wins a merge
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(received_at)          -- server time only: a 2027 client clock cannot pick the partition
ORDER BY (tenant_id, event_id)
TTL toDateTime(received_at) + INTERVAL 90 DAY
SETTINGS min_age_to_force_merge_seconds = 86400;  -- deleted rows leave disk within a day of the mutation

-- Visitors flagged after the fact (the first three page views of a rate-only bot).
CREATE TABLE bot_visitors
(
    tenant_id    LowCardinality(String),
    anonymous_id String,
    flagged_at   DateTime64(3, 'UTC'),
    reason       LowCardinality(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (tenant_id, anonymous_id);

-- Per-minute rollup the dashboards read. An incremental materialized view sees each insert
-- block once and never retracts, so duplicates that reach `events` are counted here until the
-- day is repaired. The drill measures how many reach the store (out/drill_clean.json).
CREATE TABLE page_views_1m
(
    tenant_id LowCardinality(String),
    minute    DateTime('UTC'),
    path      String,
    views     SimpleAggregateFunction(sum, UInt64),
    visitors  AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(minute)
ORDER BY (tenant_id, minute, path)
TTL minute + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW page_views_1m_mv TO page_views_1m AS
SELECT tenant_id,
       toStartOfMinute(toDateTime(event_time, 'UTC')) AS minute,
       path,
       count() AS views,
       uniqState(anonymous_id) AS visitors
FROM events
WHERE type = 'page_view' AND NOT is_bot
GROUP BY tenant_id, minute, path;

-- Staging table for the nightly repair (schema/jobs.sql): same structure and partition key.
CREATE TABLE page_views_1m_repair AS page_views_1m;
