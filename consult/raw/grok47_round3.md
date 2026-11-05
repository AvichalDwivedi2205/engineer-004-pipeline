The durability and dashboard claims are the ones a walkthrough will break. Several of the headline numbers in `ANSWER.md` do not match `out/`, `schema/`, or the limits already quoted in `evidence/`.

## 1. Factual / technical errors

**Ack guarantee contradicts the outage path.** Line 9 says the collector "acks only once Kinesis holds the record." Line 84 says that if Kinesis fails for 2 seconds the collector "spills to S3, acks, and replays later." `PutRecords` also returns HTTP 200 when `FailedRecordCount` is non-zero, so a 2xx is not "the record is in the shard." Fix the lede: a 2xx means a durable write to Kinesis or the spill object, and the client retries only the failed records.

**"FINAL removes" is labeled observed, and FINAL would not do it anyway.** Line 86: "1.5% of ClickHouse rows are copies that FINAL removes [Observed] (`out/drill_clean.json`)." That file's `clickhouse_duplicate_rows_before_dedupe` is 15,351 extra writes into a Python `defaultdict`. Nothing in the drill runs ClickHouse. The evidence log already marks the schema Tier 0 ("not executed"). Separately, `ReplacingMergeTree` dedupes only inside one partition. `PARTITION BY toYYYYMM(received_at)` plus a 7-day replay means a retry can land in the next month while `event_time` stays on the old day (client time is still trusted up to 7 days behind). `FINAL` returns both copies, and `schema/jobs.sql` counts both. The comment in `schema/clickhouse.sql` that the nightly `FINAL` repairs this is wrong. Fix: exact reads and the repair should `GROUP BY tenant_id, event_id` with `argMax(..., version)`. Relabel the 1.5% as extra mock writes.

**The rollup does not see path or bots.** Line 64 says dashboards read the minute view and bots are excluded. `page_views_1m_mv` groups by the `path` column and filters `NOT is_bot`. The gate never sets either; path stays inside `properties`, and `is_bot` defaults to false. `schema/check.sql` inserts `evt-0002` with `is_bot = false` and a made-up path. `evt-0001`, `evt-0009`, and `evt-0012` would not show up as pricing views, and the scanner would. Fix: map `properties.path`, `referrer`, and `is_bot` in the insert.

**Row policies fail open.** Line 92: "a user without one reads nothing, so a missing policy fails closed." ClickHouse does not filter a user who is not the subject of any policy. `schema/access.sql` only creates a permissive policy `TO tenant_t042`. Everyone else, including a missed tenant and the default Cloud user, can read `events`. Fix: one restrictive policy `USING 0 TO ALL`, then a permissive policy per tenant, plus an explicit ingest/repair role that the nightly job uses.

**"$485/mo" is the wrong comparison.** Line 57: "Avoiding it costs $485/mo [Estimated]." `out/cost_sheet.md` defines that number as the shard-hour gap between 22 shards and 3. It is not the cost of provisioned versus on-demand. The on-demand prices are in `evidence/prices_2026-10-29.md` and never appear as a column. The 2x-peak / ~15 minute throttle sentence matches the quotes in that file. Fix: show the on-demand bill at 1.5B events next to the $583 provisioned line, and keep $485 only if you label it "22 shards versus 3."

**The spike does not fit the Lambda settings you quoted.** Line 70 budgets 789 records/s per shard. Your own evidence says the event-source mapping polls about once per second and that `BatchSize` defaults to 100. That is about 100 records/s per shard, ~2,200/s across 22 shards, against a 17,361/s spike. Line 79's "Lambda poll ≤1 s [Benchmarked]" is that poll period, not a measured latency. Fix: set batch size to at least 1,000, one ClickHouse insert and one Valkey pipeline per invocation, and drop the ≤1 s label to [Assumed] until a canary exists.

**One Valkey script cannot touch those keys on the cluster you sized.** Line 85: "Counters increment in the same Valkey script as the commit." The sheet is "2 shards × (primary+replica)." A Lua script whose keys are `(tenant, event_id)` and `(tenant, anonymous_id)` hits `CROSSSLOT` unless every key shares a hash tag. A `{tenant}` tag pins a hot tenant to one Valkey shard. Fix: stop claiming one script. Dedupe with `SET NX`, and take visitor counters from the nightly rebuild you already run for late events.

**Firehose 200 is not an Iceberg commit, and the payload is nested.** Line 60 treats Direct PUT plus `AppendOnly` as the lake write past the 5 MiB/s default. `PutRecordBatch` returning 200 means Firehose accepted bytes. The Iceberg commit happens later; failures go to the error prefix after the Lambda has already checkpointed the shard. Counting that prefix as a "sink failure" (line 199) cannot abort a batch that has finished. The same evidence note says Firehose keeps only the first level of nested JSON. `properties` is nested, so these records are the type-mismatch case you already fear. The evidence file also lists a second us-east-1 Direct PUT quota of 5 MiB/s, "raised on request," which `AppendOnly` is not shown to lift. At 1 KB the spike is ~17 MB/s. Fix: stringify `properties`, request the quota in writing, and reconcile the error prefix with a separate consumer after the checkpoint.

**`consult/` is cited and not in the tree.** The disclosure points at `consult/DECISIONS.md` and `consult/raw/`. Nothing under `consult/` is in this repo (`.gitignore` does not explain it). Delete the citation until the files ship.

## 2. Architecture

The weakest decision is making one Lambda return the commit for ClickHouse, both Firehose streams, and Valkey. The drill's sinks are in-process dicts that ack inline (`tools/spike_drill.py`), so `out/drill_clean.json` never tests a slow ClickHouse or a late Iceberg failure. On a walkthrough I would pause ClickHouse for a few seconds, then fail the Iceberg commit after Firehose has already returned 200, and ask what the dashboard, the export, and the iterator age are. I would also ask for the dictionary refresh interval, because "resolved at query time" can add seconds you have already spent in the latency budget.

What holds up: freezing the SDK hostname and payload; dedupe keys committed only after the sinks you do control have acked (the before/after pair in `out/drill_dedupe_before_sink.json` is the real result: 6,416 and 13,321 rows missing); high-water marks finding 214 of 214 injected losses; refusing client clocks, client counters, and unauthenticated deletes; a cutover script with HOLD and ROLLBACK; managed services and Python for two people.

## 3. Brief coverage

Rollback is actually specified: ALB weights, A1–A4, automation may reverse and may not advance.

"Viewed pricing 3x" is only a warning about `evt-0019`. There is no window, no stitched identity, no bot rule, and no statement that legacy `pageview` counts. `evt-0009` is mapped for storage, but with `event_time_source = client_unverified` the gate sets `no_state_update` and `no_trigger` even when Kinesis arrival time is present (`tests/test_gate.py` only checks `partition_date`). A customer who cannot upgrade the SDK never moves a Valkey counter.

Triggers are a Valkey lookup plus "MVP leaves out triggers," and phase 4 is the word "triggers." Warehouse export is "Parquet" and "a deletion manifest," with no cadence, schema, or what Snowflake and BigQuery do when the manifest arrives late. SOC 2 is ClickHouse's SOC 2 Type II in the evidence file, not your access reviews, change control, or audit log. Five hundred tenants show up as row policies and a partition cliff; the policies don't fail closed, and ~500 CMKs at $1/key are not on the sheet. Existing integrations are the hostname and a Postgres adapter. Current Redis consumers and any outbound webhooks are unnamed, so "not breaking them" is still a slogan.

## 4. Fixture

Checksum matches `fixtures/SHA256SUM`. The audit's classes match the file: duplicate `evt-0002` (7,428 ms, content equal aside from `received_at`), clocks `evt-0005` / `evt-0006` (+65:00.000) / `evt-0016`, legacy `evt-0009`, null tenant `evt-0011`, PII `evt-0007` linked later by `evt-0022`, scanner `evt-0012`–`evt-0015`, unauthenticated delete `evt-0017` (no identify to `anon-77a`; only `evt-0006` is in reach), truncated `evt-0020`, client counter `evt-0019`, unmerged `evt-0016` and `evt-0024`. Median transit 163 ms, n=17, matches the sane rows. I would not reopen those.

Mishandled:

- `evt-0009`: live arrival time fixes the partition and still withholds counters and triggers. That is the legacy pricing view the segment is about.
- `evt-0001`, `evt-0009`, `evt-0012`, `evt-0021`: the pricing comparison in `out/fixture_audit.md` is a Python count. The SQL rollup cannot reproduce it until `path` and `is_bot` are real columns.
- `evt-0004`: `"referrer": null` is a different JSON type from every other referrer and from `evt-0009`'s string `ref`. Unclassified. A strict string column drops a good event.
- `schema/check.sql` calls its insert "the fixture's evt-0002 retry" and writes a `page_view` of `/features`. The fixture row is a click on `/pricing`.
- The referrer tokens `scanner` and `bot` are fitted to `evt-0012`'s host. Fine as a fixture rule, not as a production signature. The test only requires `bot_referrer` or `bot_rate`, so it does not lock the "all four by referrer" claim.

## 5. Versus baseline

`claude_baseline.md` is a withholding notice. It has no design, so there is nothing on the page to beat sentence for sentence.

Against the rubric's picture of a generic answer, the fixture audit, the loss drill, the dedupe-before-sink counterexample, sourced prices, and the cutover gate are ahead of a component essay. The places a careful competing answer still wins are the ones you over-claim: FINAL, Firehose-as-commit, fail-closed tenancy, and a $6.9K total whose "why not on-demand" number doesn't price on-demand. A plainer design that refused those four sentences would beat this one in a live reread. The unchecked DDL is already marked Tier 0; the prose then uses it as if it had run.

## 6. Voice

> "Optimizing for no edge loss, full accounting, and two-person operability. Giving up exactly-once (idempotent at-least-once instead), event time and live personalization for skewed clients (nightly rebuild instead), and multi-region DR."

Rewrite: "Duplicate rows are allowed. A nightly job rebuilds the rollup. We will not run a second region or a second stream processor. Two people watch sequence gaps and the cutover script."

> "Overload degrades dashboard freshness first ("data as of" banner), then segments (last-known state), then exports."

Rewrite: "When iterator age passes 60s, the banner shows the oldest timestamp still in Kinesis. Segments keep serving the last repaired day. Exports stay on the last Iceberg snapshot. The collector spills only after PutRecords fails."

## 7. Top 6 edits

1. Make Firehose's error prefix a real reconciler, flatten `properties`, and name the 5 MiB/s quota increase. Cut the p95 latency stack (line 79); it is unmeasured.
2. Replace FINAL-as-dedupe with `argMax` by `(tenant_id, event_id)`, add a restrictive row policy, and map `path` / `is_bot`. Cut the OpenSearch, Flink, and self-managed sentences in the choice table.
3. Price on-demand beside the $583 Kinesis line and demote $485 to "22 shards vs 3." Cut the stress-month column from the written table; it already lives in `out/cost_sheet.md`.
4. Set the Kinesis batch size to ≥1,000 and drop the single Valkey script. Cut the MSK and API Gateway asides.
5. Write the pricing-3x rule in one sentence (window, stitched ids, bots out, legacy `pageview` in, `count_today` ignored) and let `evt-0009` update counters when arrival time exists. Cut the "not concluded from 25 rows" list to the 3% loss and the latency.
6. Fix the opening ack sentence, relabel the 1.5% line as mock duplicate writes, and remove `consult/` until those files are in the repo. Cut the tradeoff paragraph in §7 down to the rewrite above.
