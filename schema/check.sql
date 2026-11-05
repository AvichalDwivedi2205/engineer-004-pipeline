-- Appended to schema/clickhouse.sql by tools/ddl_check.sh. Rows are fixture events as
-- ingest_gate.gate.clickhouse_row maps them, except the two marked SYNTHETIC.
-- evt-0002, fixture lines 2 and 5: one click on /pricing that arrived twice, 7.4 s apart.
INSERT INTO events (tenant_id, event_id, anonymous_id, type, client_ts, received_at, event_time, event_time_source,
                    path, referrer, properties, flags, is_bot, ingest_node, ingest_seq, version) VALUES
('t-042', 'evt-0002', 'anon-9f2', 'click', '2026-06-15 14:02:19.882', '2026-06-15 14:02:20.014',
 '2026-06-15 14:02:19.882', 'client', '/pricing', '', '{"path":"/pricing","selector":"#cta-demo"}', [], false, 'n1', 2, 1),
('t-042', 'evt-0002', 'anon-9f2', 'click', '2026-06-15 14:02:19.882', '2026-06-15 14:02:27.442',
 '2026-06-15 14:02:19.882', 'client', '/pricing', '', '{"path":"/pricing","selector":"#cta-demo"}', [], false, 'n1', 5, 2);
-- evt-0001, a /pricing page view, written twice: the second copy is a batch Kinesis redelivered
-- after a sink failure (SYNTHETIC), so it carries the same collector stamp.
INSERT INTO events (tenant_id, event_id, anonymous_id, type, client_ts, received_at, event_time, event_time_source,
                    path, referrer, properties, flags, is_bot, ingest_node, ingest_seq, version) VALUES
('t-042', 'evt-0001', 'anon-9f2', 'page_view', '2026-06-15 14:02:11.104', '2026-06-15 14:02:11.310',
 '2026-06-15 14:02:11.104', 'client', '/pricing', 'https://google.com', '{"path":"/pricing","referrer":"https://google.com"}',
 [], false, 'n1', 1, 3),
('t-042', 'evt-0001', 'anon-9f2', 'page_view', '2026-06-15 14:02:11.104', '2026-06-15 14:02:11.310',
 '2026-06-15 14:02:11.104', 'client', '/pricing', 'https://google.com', '{"path":"/pricing","referrer":"https://google.com"}',
 [], false, 'n1', 1, 4);
-- evt-0012, the scanner's first page view: kept, but the rollup view must skip it.
INSERT INTO events (tenant_id, event_id, anonymous_id, type, client_ts, received_at, event_time, event_time_source,
                    path, referrer, properties, flags, is_bot, ingest_node, ingest_seq, version) VALUES
('t-042', 'evt-0012', 'anon-8fc', 'page_view', '2026-06-15 14:10:00.001', '2026-06-15 14:10:00.120',
 '2026-06-15 14:10:00.001', 'client', '/pricing', 'https://scanner.example-bot.net',
 '{"path":"/pricing","referrer":"https://scanner.example-bot.net"}', ['bot_referrer'], true, 'n1', 7, 5);
-- SYNTHETIC late retry across a month boundary: re-sent 2.5 h later, past the 1 h dedupe TTL,
-- so the gate accepts it again. The client clock is trusted, so event_time is unchanged.
INSERT INTO events (tenant_id, event_id, anonymous_id, type, client_ts, received_at, event_time, event_time_source,
                    path, referrer, properties, flags, is_bot, ingest_node, ingest_seq, version) VALUES
('t-042', 'syn-late', 'anon-5aa', 'page_view', '2026-06-30 22:59:59.800', '2026-06-30 22:59:59.950',
 '2026-06-30 22:59:59.800', 'client', '/pricing', '', '{"path":"/pricing"}', [], false, 'n2', 40, 6),
('t-042', 'syn-late', 'anon-5aa', 'page_view', '2026-06-30 22:59:59.800', '2026-07-01 01:30:00.000',
 '2026-06-30 22:59:59.800', 'client', '/pricing', '', '{"path":"/pricing"}', [], false, 'n3', 12, 7);

SELECT 'events rows as stored', count() FROM events;                                    -- 7
SELECT 'events rows with FINAL', count() FROM events FINAL;                             -- 5: syn-late is in two partitions
SELECT 'events rows with LIMIT 1 BY', count()
FROM (SELECT * FROM events ORDER BY version DESC LIMIT 1 BY tenant_id, event_id);       -- 4
SELECT 'rollup views before repair', sum(views) FROM page_views_1m;                     -- 4: both copies of evt-0001 and syn-late, no evt-0012

-- Repair as in schema/jobs.sql, for 2026-06-15 and then 2026-06-30.
TRUNCATE TABLE page_views_1m_repair;
INSERT INTO page_views_1m_repair
SELECT tenant_id, toStartOfMinute(toDateTime(event_time, 'UTC')) AS minute, path, count() AS views,
       uniqState(anonymous_id) AS visitors
FROM (SELECT tenant_id, event_id, anonymous_id, type, event_time, path, is_bot FROM events
      WHERE received_at >= toDateTime64('2026-06-15', 3, 'UTC') - INTERVAL 1 DAY
        AND received_at <  toDateTime64('2026-06-15', 3, 'UTC') + INTERVAL 8 DAY
      ORDER BY version DESC LIMIT 1 BY tenant_id, event_id)
WHERE type = 'page_view' AND NOT is_bot AND toDate(event_time) = '2026-06-15'
  AND (tenant_id, anonymous_id) NOT IN (SELECT tenant_id, anonymous_id FROM bot_visitors FINAL)
GROUP BY tenant_id, minute, path;
ALTER TABLE page_views_1m REPLACE PARTITION 20260615 FROM page_views_1m_repair;
TRUNCATE TABLE page_views_1m_repair;
INSERT INTO page_views_1m_repair
SELECT tenant_id, toStartOfMinute(toDateTime(event_time, 'UTC')) AS minute, path, count() AS views,
       uniqState(anonymous_id) AS visitors
FROM (SELECT tenant_id, event_id, anonymous_id, type, event_time, path, is_bot FROM events
      WHERE received_at >= toDateTime64('2026-06-30', 3, 'UTC') - INTERVAL 1 DAY
        AND received_at <  toDateTime64('2026-06-30', 3, 'UTC') + INTERVAL 8 DAY
      ORDER BY version DESC LIMIT 1 BY tenant_id, event_id)
WHERE type = 'page_view' AND NOT is_bot AND toDate(event_time) = '2026-06-30'
  AND (tenant_id, anonymous_id) NOT IN (SELECT tenant_id, anonymous_id FROM bot_visitors FINAL)
GROUP BY tenant_id, minute, path;
ALTER TABLE page_views_1m REPLACE PARTITION 20260630 FROM page_views_1m_repair;
SELECT 'rollup views after repair', sum(views) FROM page_views_1m;                      -- 2

ALTER TABLE events DELETE WHERE tenant_id = 't-042' AND anonymous_id IN ['anon-9f2'] SETTINGS mutations_sync = 1;
SELECT 'events rows after deletion mutation', count() FROM events;                      -- 3: evt-0012 and syn-late's two copies
