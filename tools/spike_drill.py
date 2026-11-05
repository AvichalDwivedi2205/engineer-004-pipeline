#!/usr/bin/env python3
"""Synthetic spike drill for the ingest gate, checked against ground truth.

    python3 tools/spike_drill.py --events 500000 --seed 7
    python3 tools/spike_drill.py --events 500000 --seed 7 --inject-loss 0.0002
    python3 tools/spike_drill.py --events 200000 --seed 7 --dedupe-commit before-sink   # the first design: loses data

What it does:
  1. Generates a 6-hour stream for 500 tenants (Zipf-skewed) with hour 3 at 10x
     volume, and injects every anomaly class seen in the fixture at assumed rates,
     plus retries that arrive 2-5 hours late, after the dedupe keys expired.
  2. Simulated collector nodes stamp each record with (node, incarnation, ingest_seq)
     and persist a high-water mark of acknowledged sequence numbers once a second.
     Node 0 crashes once in the spike hour and comes back as a new incarnation.
     Optionally drops records *after* stamping (loss between collector and gate).
  3. Feeds the stream to the gate in 500-record batches with two sinks (ClickHouse,
     lake). A batch attempt can fail after one sink wrote and the other did not, or
     crash after commit and before the Kinesis checkpoint; either way the process
     restarts and Kinesis redelivers the batch.
  4. Checks both sinks, the personalization counters and the per-record ledger
     against the generator's ground truth. The loss detector reads only the
     high-water marks and the sequence numbers seen downstream, never the generator.
     Writes out/drill_<tag>.json and per tenant-hour counts (edge / new pipeline /
     simulated legacy) for tools/cutover_gate.py.

All rates are [Assumed] and printed with the results. The throughput figure is a
single-threaded pure-Python number on whatever machine runs this; it says nothing
about production hardware beyond a lower bound for this code path. Sinks, Valkey and
Kinesis are dicts here: the drill tests the delivery contract, not those services.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import json
import random
import sys
import platform
import subprocess
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from ingest_gate.gate import IngestGate  # noqa: E402

RATES = {  # [Assumed] per unique event unless noted
    "retry_duplicate": 0.010,
    "malformed": 0.001,
    "legacy_schema": 0.020,
    "null_tenant": 0.0005,
    "clock_ahead": 0.005,
    "pii_in_props": 0.002,
    "id_conflict": 0.0001,
    "privacy_requests_total": 50,
    "bot_bursts_total": 400,  # each burst = 6 page views inside 300 ms
    "late_retry": 0.001,  # same event re-sent 2-5 h later (offline queue, spill replay), past the dedupe TTL
    "sink_error_after_partial_write": 0.02,  # per batch attempt, deliberately high: one sink acked, the other failed
    "crash_after_commit": 0.01,  # per batch attempt, deliberately high: committed, died before the checkpoint
    "legacy_loss_normal_hour": 0.002,
    "legacy_loss_spike_hour": 0.03,  # the brief's ~3% peak loss, applied to the simulated old system
}
TENANTS, VISITORS_PER_TENANT, NODES, HOURS, SPIKE_HOUR, SPIKE_X = 500, 2000, 8, 6, 3, 10
T0 = datetime(2026, 11, 27, 12, 0, tzinfo=timezone.utc)
CRASHED_NODE, CRASH_AT = 0, T0 + timedelta(hours=SPIKE_HOUR, minutes=30)
TYPES = (("page_view", 0.60), ("click", 0.25), ("custom", 0.10), ("form_submit", 0.03), ("identify", 0.02))


def iso(dt):
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def generate(n, rng):
    weights = [1 / (k ** 1.1) for k in range(1, TENANTS + 1)]
    cum = [sum(weights[:i + 1]) for i in range(len(weights))]
    type_cum, acc = [], 0.0
    for _, p in TYPES:
        acc += p
        type_cum.append(acc)
    hour_w = [SPIKE_X if h == SPIKE_HOUR else 1 for h in range(HOURS)]
    hour_cum = [sum(hour_w[:i + 1]) for i in range(HOURS)]
    truth, records = {}, []

    def tenant():
        return f"t-{bisect.bisect(cum, rng.random() * cum[-1]):03d}"

    def at_random_time():
        h = bisect.bisect(hour_cum, rng.random() * hour_cum[-1])
        return T0 + timedelta(hours=h, seconds=rng.random() * 3600)

    for i in range(n):
        t = tenant()
        anon = f"anon-{t}-{rng.randrange(VISITORS_PER_TENANT)}"
        etype = TYPES[bisect.bisect(type_cum, rng.random() * type_cum[-1])][0]
        recv = at_random_time()
        client = recv - timedelta(milliseconds=rng.randint(80, 400))
        e = {"event_id": f"evt-{i:09d}", "tenant_id": t, "anonymous_id": anon,
             "user_id": f"u-{anon}" if etype == "identify" else None, "type": etype,
             "ts": iso(client), "properties": {"path": rng.choice(["/", "/pricing", "/features", "/docs"])}}
        tags = set()
        r = rng.random
        if r() < RATES["legacy_schema"] and etype == "page_view":
            e = {"event_id": e["event_id"], "tenant_id": t, "anonymous_id": anon, "user_id": None,
                 "type": "pageview", "timestamp": e["ts"], "properties": {"page_path": e["properties"]["path"]}}
            tags.add("legacy")
        if r() < RATES["null_tenant"]:
            e["tenant_id"] = None
            tags.add("null_tenant")
        if r() < RATES["clock_ahead"]:
            ahead = rng.choice([6, 47, 3900, 86400 * 365])
            key = "timestamp" if "timestamp" in e else "ts"
            e[key] = iso(recv + timedelta(seconds=ahead))
            tags.add("clock_ahead")
        if r() < RATES["pii_in_props"]:
            e["type"], e["user_id"] = "custom", None
            e["properties"] = {"name": "quote_requested", "contact_email": f"person{i}@example.org"}
            tags.discard("legacy")
            tags.add("pii")
        records.append((recv, e, tags))
        truth[(e["tenant_id"], e["event_id"])] = tags

    for b in range(RATES["bot_bursts_total"]):
        t, recv = tenant(), at_random_time()
        anon = f"bot-{b}"
        ref = "https://scanner.example-bot.net" if b % 2 == 0 else None
        for k in range(6):
            ts = recv + timedelta(milliseconds=50 * k)
            e = {"event_id": f"bot-{b}-{k}", "tenant_id": t, "anonymous_id": anon, "user_id": None,
                 "type": "page_view", "ts": iso(ts), "properties": {"path": f"/p{k}", "referrer": ref}}
            tags = {"bot", "bot_ref" if ref else "bot_rate_only"}
            records.append((ts + timedelta(milliseconds=120), e, tags))
            truth[(t, e["event_id"])] = tags
    for p in range(RATES["privacy_requests_total"]):
        t, recv = tenant(), at_random_time()
        e = {"event_id": f"dsr-{p}", "tenant_id": t, "anonymous_id": f"anon-{t}-{p}", "user_id": f"u-{p}",
             "type": "privacy_request", "ts": iso(recv - timedelta(milliseconds=150)),
             "properties": {"request": "delete_all_data", "regulation": "GDPR"}}
        records.append((recv, e, {"privacy"}))
        truth[(t, e["event_id"])] = {"privacy"}

    deliveries = []
    for recv, e, tags in records:
        deliveries.append((recv, json.dumps(e), tags, "original"))
        if rng.random() < RATES["retry_duplicate"]:
            deliveries.append((recv + timedelta(seconds=rng.uniform(1, 30)), json.dumps(e), tags, "retry"))
        if rng.random() < RATES["id_conflict"]:
            clash = dict(e, properties={"path": "/collision"})
            deliveries.append((recv + timedelta(seconds=2), json.dumps(clash), tags, "conflict"))
        if rng.random() < RATES["malformed"]:
            deliveries[-1] = (deliveries[-1][0], deliveries[-1][1][:-1], tags | {"malformed"}, deliveries[-1][3])
    deliveries.sort(key=lambda d: d[0])
    return deliveries, truth


def collect(deliveries, rng):
    """Collector nodes stamp (node, incarnation) and a per-incarnation ingest_seq, and persist
    a high-water mark of acknowledged seqs once a second (a conditional DynamoDB write in
    production). Node 0 crashes in the spike hour: the records it stamped after its last mark
    have no mark, and it restarts as a new incarnation from seq 1. Nodes drain cleanly at the end."""
    seq, inc, hwm, last_mark = defaultdict(int), defaultdict(int), {}, {}
    crashed, stamped = False, []
    for i, (recv, raw, tags, kind) in enumerate(deliveries):
        node = rng.randrange(NODES)
        if node == CRASHED_NODE and not crashed and recv >= CRASH_AT:
            crashed = True
            inc[node] += 1
            seq[node] = 0
        key = (node, inc[node])
        second = int(recv.timestamp())
        if last_mark.get(key) != second:
            if seq[node]:
                hwm[key] = seq[node]
            last_mark[key] = second
        seq[node] += 1
        stamped.append({"i": i, "recv": recv, "raw": raw, "tags": tags, "kind": kind, "node": key, "seq": seq[node]})
    for node in range(NODES):
        if seq[node]:
            hwm[(node, inc[node])] = seq[node]
    return stamped, hwm


def run(args):
    rng = random.Random(args.seed)
    t_gen = time.perf_counter()
    deliveries, truth = generate(args.events, rng)
    # Everything added after generate() draws from its own generator, so generate() stays
    # identical for a given seed (tools/bot_window_regression.py relies on that).
    frng = random.Random(args.seed + 1)
    late = [(recv + timedelta(hours=frng.uniform(2, 5)), raw, tags | {"late_retry"}, "late_retry")
            for recv, raw, tags, kind in deliveries if kind == "original" and frng.random() < RATES["late_retry"]]
    deliveries = sorted(deliveries + late, key=lambda d: d[0])
    stamped, hwm = collect(deliveries, frng)
    lost_ids = {s["i"] for s in stamped if frng.random() < args.inject_loss}
    stream = [s for s in stamped if s["i"] not in lost_ids]
    gen_s = time.perf_counter() - t_gen

    gate = IngestGate()
    sinks = {"clickhouse": defaultdict(int), "lake": defaultdict(int)}  # rows written per (tenant_id, event_id)
    counters = defaultdict(int)  # personalization counter increments per event (Valkey)
    ledger, seen_seq, committed, faults = {}, defaultdict(set), [], Counter()
    gate_s = 0.0
    for start in range(0, len(stream), 500):
        batch = stream[start:start + 500]
        while True:
            gate.begin_batch()
            t = time.perf_counter()
            outs = [gate.process_line(s["raw"], server_received_at=iso(s["recv"])) for s in batch]
            gate_s += time.perf_counter() - t
            if args.dedupe_commit == "before-sink":
                gate.commit_batch()
            roll = frng.random()
            failed_sink = frng.choice(list(sinks)) if roll < RATES["sink_error_after_partial_write"] else None
            for name, rows in sinks.items():
                if name != failed_sink:
                    for o in outs:
                        if o.bucket == "accepted":
                            rows[(o.event["tenant_id"], o.event["event_id"])] += 1
            if failed_sink:
                faults[f"{failed_sink}_failed_after_other_sink_wrote"] += 1
                gate.abort_batch()
                gate = gate.restart()
                continue
            gate.commit_batch()
            # Ledger, seq ledger and counters commit together with the dedupe keys (one Valkey
            # script in production), so each is written once per record however often it is redelivered.
            for s, o in zip(batch, outs):
                if s["i"] in ledger:
                    continue
                ledger[s["i"]] = o.bucket
                seen_seq[s["node"]].add(s["seq"])
                if o.bucket == "accepted":
                    committed.append((o.event, s["tags"]))
                    if "no_state_update" not in o.event["flags"]:
                        counters[(o.event["tenant_id"], o.event["event_id"])] += 1
            if roll < RATES["sink_error_after_partial_write"] + RATES["crash_after_commit"]:
                faults["crash_after_commit_before_checkpoint"] += 1
                gate = gate.restart()
                continue
            break

    # ---- checks against ground truth -----------------------------------------
    # Expected = every event with at least one well-formed copy that reached the gate,
    # a resolvable tenant, and an analytics (not control) type. Decided from what the
    # generator sent, not from the gate's own rules.
    expected = set()
    for s in stream:
        if not _parses(s["raw"]):
            continue
        e = json.loads(s["raw"])
        if e.get("tenant_id") and e["type"] != "privacy_request":
            expected.add((e["tenant_id"], e["event_id"]))

    checks = {"every_record_has_one_committed_bucket": len(ledger) == len(stream)}
    for name, rows in sinks.items():
        checks[f"{name}_events_missing"] = len(expected - set(rows))
        checks[f"{name}_unexpected_events"] = len(set(rows) - expected)
        checks[f"{name}_duplicate_rows_before_dedupe"] = sum(rows.values()) - len(rows)
    checks["counter_double_increments"] = sum(1 for v in counters.values() if v > 1)
    checks["counter_coverage"] = f"{len(counters)}/{len(expected)}"
    raw_pii = sum(1 for ev, tags in committed if "pii" in tags and "@" in json.dumps(ev["properties"]))
    checks["raw_pii_reaching_storage"] = raw_pii

    def recall(tag, pred, unless=None):
        rows = [ev for ev, tags in committed if tag in tags and unless not in tags]
        return f"{sum(1 for ev in rows if pred(ev))}/{len(rows)}"

    bots = gate.bot_visitors
    checks["detection"] = {
        "legacy_mapped": recall("legacy", lambda ev: ev["type"] == "page_view" and "path" in ev["properties"]),
        # A late retry of a skewed event arrives hours later, so its clock no longer looks ahead.
        "clock_ahead_to_server_time": recall("clock_ahead", lambda ev: ev["event_time_source"] == "server",
                                             unless="late_retry"),
        "bot_flagged_in_stream_referrer": recall("bot_ref", lambda ev: "bot_referrer" in ev["flags"]),
        "bot_flagged_in_stream_rate_only": recall("bot_rate_only", lambda ev: {"bot_rate", "bot_visitor"} & set(ev["flags"])),
        "bot_excluded_after_visitor_rollup": recall("bot", lambda ev: (ev["tenant_id"], ev["anonymous_id"]) in bots),
        "pii_redacted": recall("pii", lambda ev: "pii_redacted" in ev["flags"]),
        "late_retry_kept_out_of_counters": recall("late_retry", lambda ev: "no_state_update" in ev["flags"]),
    }
    clean = [ev for ev, tags in committed if not tags]
    checks["false_positive_flags_on_clean_events"] = sum(1 for ev in clean if ev["flags"])
    checks["clean_events"] = len(clean)

    # Loss detector: reads only the persisted high-water marks and the seqs seen downstream.
    detected, past_mark = 0, 0
    for node in set(hwm) | set(seen_seq):
        mark = hwm.get(node, 0)
        detected += mark - sum(1 for q in seen_seq[node] if q <= mark)
        past_mark += sum(1 for q in seen_seq[node] if q > mark)
    invisible = sum(1 for s in stamped if s["i"] in lost_ids and s["seq"] > hwm.get(s["node"], 0))
    checks["loss_detector"] = {"records_lost_injected": len(lost_ids), "records_missing_by_seq_gap": detected,
                               "records_seen_after_last_high_water_mark": past_mark,
                               "lost_records_after_last_mark_ground_truth": invisible}

    result = {
        "tag": args.tag, "seed": args.seed, "dedupe_commit": args.dedupe_commit, "rates_assumed": RATES,
        "unique_events_requested": args.events, "records_delivered_to_collector": len(deliveries),
        "records_reaching_gate": len(stream), "collector_incarnations": len(hwm),
        "faults": dict(faults), "gate_attempts": gate.stats.records_in,
        "buckets_committed": dict(Counter(ledger.values())), "reasons": dict(gate.stats.reasons), "checks": checks,
        "throughput": {"gate_records_processed": gate.stats.records_in, "gate_seconds": round(gate_s, 2),
                       "records_per_second_single_thread": round(gate.stats.records_in / gate_s),
                       "generation_seconds": round(gen_s, 2), "python": sys.version.split()[0],
                       "machine": platform.platform(), "cpu": _cpu_name()},
    }
    _write_counts(args.tag, stamped, ledger, lost_ids, rng)
    out = ROOT / "out"
    out.mkdir(exist_ok=True)
    (out / f"drill_{args.tag}.json").write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps(result, indent=2, default=str))
    ld = checks["loss_detector"]
    ok = (checks["every_record_has_one_committed_bucket"] and checks["counter_double_increments"] == 0
          and all(checks[f"{n}_events_missing"] == 0 and checks[f"{n}_unexpected_events"] == 0 for n in sinks)
          and raw_pii == 0
          and ld["records_missing_by_seq_gap"] + ld["lost_records_after_last_mark_ground_truth"]
          == ld["records_lost_injected"])
    return 0 if ok else 1


def _cpu_name():
    try:
        return subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True,
                              text=True, timeout=2).stdout.strip() or platform.processor()
    except (OSError, subprocess.SubprocessError):
        return platform.processor()


def _parses(raw):
    try:
        json.loads(raw)
        return True
    except ValueError:
        return False


def _write_counts(tag, stamped, ledger, lost_ids, rng):
    """Per tenant-hour counts from three independent views, for cutover_gate.py.
    edge  = records the collectors acknowledged (their receipt manifests)
    new_* = committed per-record outcome in the new pipeline (redeliveries don't inflate)
    legacy_landed = simulated old system: no dedupe, drops 0.2% normally and 3% in the spike hour."""
    rows = defaultdict(lambda: defaultdict(int))
    for s in stamped:
        hour = int((s["recv"] - T0).total_seconds() // 3600)
        tenant = _tenant_of(s["raw"])
        r = rows[(tenant, hour)]
        r["edge"] += 1
        b = "lost" if s["i"] in lost_ids else ledger.get(s["i"], "lost")
        r[f"new_{b}"] += 1
        loss = RATES["legacy_loss_spike_hour"] if hour == SPIKE_HOUR else RATES["legacy_loss_normal_hour"]
        if _parses(s["raw"]) and rng.random() >= loss:
            r["legacy_landed"] += 1
    cols = ["edge", "new_accepted", "new_duplicate", "new_quarantined", "new_dead_letter", "new_control",
            "new_suppressed", "new_lost", "legacy_landed"]
    with open(ROOT / "out" / f"counts_{tag}.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["tenant_id", "hour"] + cols)
        for (tenant, hour), r in sorted(rows.items(), key=lambda kv: (str(kv[0][0]), kv[0][1])):
            w.writerow([tenant, hour] + [r[c] for c in cols])


def _tenant_of(raw):
    try:
        return json.loads(raw).get("tenant_id") or "unresolved"
    except ValueError:
        import re
        m = re.search(r'"tenant_id"\s*:\s*"([^"]+)"', raw)
        return m.group(1) if m else "unresolved"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=200_000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--inject-loss", type=float, default=0.0, help="fraction of stamped records dropped before the gate")
    ap.add_argument("--dedupe-commit", choices=["after-sink", "before-sink"], default="after-sink",
                    help="before-sink reproduces the first design, which marked events seen before the sinks acked")
    ap.add_argument("--tag", default="clean")
    sys.exit(run(ap.parse_args()))
