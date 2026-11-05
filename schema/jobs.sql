-- Scheduled jobs against schema/clickhouse.sql. Parameters use clickhouse-client's
-- {name:Type} syntax, e.g. --param_day=2026-11-27 --param_part=20261127.
-- Run as the pipeline role in schema/access.sql; a tenant role would see only its own rows.

-- 1. Nightly repair of a closed day: rebuild the rollup from the deduplicated base table,
--    dropping visitors flagged as bots after their first events, then swap the partition in.
--    Dedupe runs before the day filter, over every received_at that can hold a copy of the
--    day's events (5 s of clock lead before it, 7 days of backfill after it), and does not rely
--    on FINAL, which cannot collapse copies in different month partitions.
TRUNCATE TABLE page_views_1m_repair;
INSERT INTO page_views_1m_repair
SELECT tenant_id,
       toStartOfMinute(toDateTime(event_time, 'UTC')) AS minute,
       path,
       count() AS views,
       uniqState(anonymous_id) AS visitors
FROM
(
    SELECT tenant_id, event_id, anonymous_id, type, event_time, path, is_bot
    FROM events
    WHERE received_at >= toDateTime64({day:Date}, 3, 'UTC') - INTERVAL 1 DAY
      AND received_at <  toDateTime64({day:Date}, 3, 'UTC') + INTERVAL 8 DAY
    ORDER BY version DESC
    LIMIT 1 BY tenant_id, event_id
)
WHERE type = 'page_view' AND NOT is_bot
  AND toDate(event_time) = {day:Date}
  AND (tenant_id, anonymous_id) NOT IN (SELECT tenant_id, anonymous_id FROM bot_visitors FINAL)
GROUP BY tenant_id, minute, path;
ALTER TABLE page_views_1m REPLACE PARTITION {part:UInt32} FROM page_views_1m_repair;

-- 2. Daily deletion batch, after the subjects' tombstones are written (the gate already
--    suppresses them). ALTER ... DELETE is a mutation that rewrites the affected parts, unlike a
--    lightweight DELETE, which only masks rows until a later merge. The bytes are gone from the
--    live table when system.mutations shows is_done = 1; that row is the completion evidence.
ALTER TABLE events DELETE
WHERE tenant_id = {tenant:String}
  AND (anonymous_id IN {anon_ids:Array(String)} OR user_id IN {user_ids:Array(String)});

SELECT mutation_id, is_done, latest_fail_reason
FROM system.mutations
WHERE database = currentDatabase() AND table = 'events' AND NOT is_done;
-- Rollups hold counts and visitor sketches, no ids. Re-running job 1 for each affected day
-- removes the subject from those sketches.
