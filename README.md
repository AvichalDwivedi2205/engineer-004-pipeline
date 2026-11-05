# Engineer 004: operating artifact

The written answer is [ANSWER.md](ANSWER.md). This repo is the artifact behind it. It contains:

- the ingest gate that sits between the collector and every store;
- tools that check the design against the fixture, a synthetic spike with injected faults, a migration gate and a cost model;
- the ClickHouse schema the design depends on.

Brief version 2026-07 · fixture sha256 `1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235` (see `fixtures/SHA256SUM`).

## Run it

```
$ make all
```

You need Python 3.10 or newer and nothing else: no packages, network access or credentials. A full run takes about two minutes on a laptop. Most of that is the three 1M-event drills.

| Command | What it shows | Output |
|---|---|---|
| `make test` | 45 unit tests. **Gate (33):** each fixture anomaly class, plus unhappy paths the fixture doesn't contain: tenant spoofing, id collisions, sink-failure redelivery, dedupe TTL expiry, deletion replay, PII in arrays and in quarantine copies, a missing received_at, and the ClickHouse row the rollup reads (path, is_bot, flat properties). **Cutover gate (10):** every counterexample that passed its first version. **Prices (2):** the cost sheet's AWS prices against Price List skus. | stdout |
| `make audit` | The gate run over the fixture, plus audit-only cross-checks: arrival order, client aggregates, deletion scope, identity candidates, and count parity that hides different events. | `out/fixture_audit.md`, `.json` |
| `make drill` | 1M synthetic events across 500 Zipf-skewed tenants, with a 10x hour and every fixture anomaly injected. Faults are injected at deliberately high rates: partial sink failures, crashes after commit, a collector node crash and late retries. **Run 1** commits dedupe keys after both sinks ack. **Run 2** also drops 0.02% of records after the collector, to test the high-water-mark loss detector. **Run 3** uses the first design, marking keys before the sinks ack; it is expected to fail its checks. | `out/drill_clean.json`, `out/drill_loss.json`, `out/drill_dedupe_before_sink.json`, `out/counts_*.csv` |
| `make bugfix` | Compares the first bot-rate rule (windowed on client time) with the current rule (windowed on server time) on identical input. | `out/bot_window_regression.json` |
| `make cutover` | The migration acceptance gate on the drill counts. The clean run ends in HOLD, because a drill can't measure A4 freshness. The loss run ends in ROLLBACK. | `out/cutover_*.txt` |
| `make cost` | Cost of a normal month, a Black Friday month, and a stress month with every hour at spike rate. Every input is labelled. | `out/cost_sheet.md` |
| `make ddl` | Runs `schema/clickhouse.sql` and `schema/check.sql` in `clickhouse local`: fixture rows plus a synthetic retry across a month boundary, which FINAL keeps and `LIMIT 1 BY` collapses. It skips with a message if no `clickhouse` binary is installed. I have not been able to run it. | stdout |

## Layout

```
ingest_gate/gate.py          parse -> normalize -> redact -> tenant -> tombstones -> dedupe -> control routing
                             -> time policy -> bot -> identity; one bucket per record; batch begin/commit/abort around the sinks;
                             clickhouse_row() is the flat row both ClickHouse and the Iceberg lake receive
tools/fixture_audit.py       fixture report
tools/spike_drill.py         synthetic spike, two sinks, fault injection, ground-truth checks, high-water-mark loss detector
tools/bot_window_regression.py  first vs current bot window on the same stream
tools/cutover_gate.py        parallel-run acceptance (PASS / HOLD / ROLLBACK)
tools/cost_sheet.py          sizing and cost model
tools/ddl_check.sh           runs the schema in clickhouse-local if available
schema/                      ClickHouse tables, rollup repair and deletion jobs, deny-by-default row policies, a check script
evidence/                    dated price sources (AWS Price List skus, clickhouse.com quotes), extractor, bot-window diff
docs/changed_constraints.md  $10K budget, own-account-only, EU residency, 72 h deletion
consult/DECISIONS.md         where GPT-6.1 and Grok 4.7 disagreed or found errors, and what I changed or rejected
consult/raw/                 the prompts and both models' answers, unedited apart from local paths
```

## What this artifact is not

- **Not run on AWS.** Throughput comes from one laptop core. End-to-end latency is not measured anywhere here.
- **The legacy system is simulated.** Its loss and retry rates are my assumptions, not measurements.
- **The collector, Kinesis, Firehose, ClickHouse and Valkey are stand-ins.** They are represented by their contracts in the drill. The gate's in-memory dicts take the place of Valkey.
- **The ClickHouse DDL has not been executed.** See `make ddl`.
