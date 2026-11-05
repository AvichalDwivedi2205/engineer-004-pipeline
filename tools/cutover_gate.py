#!/usr/bin/env python3
"""Parallel-run acceptance gate for moving tenants from the legacy pipeline to the new one.

    python3 tools/cutover_gate.py out/counts_clean.csv                       # HOLD: A4 not measured by the drill
    python3 tools/cutover_gate.py out/counts_loss.csv                        # ROLLBACK
    python3 tools/cutover_gate.py counts.csv --canary-p95-ms 2400 --approved reviews.tsv

Input: per (tenant, hour) counts from three independent views
    edge           records the collectors acknowledged, from their receipt manifests (not from the pipeline)
    new_*          committed per-record outcome in the new pipeline
    legacy_landed  rows the old pipeline stored for the same tenant-hour
(`new_lost` in the drill CSV is generator ground truth; this gate never reads it.)

A tenant is "data accuracy verified" only if every check holds in every one of its tenant-hours:
    A1 completeness   edge == accepted + duplicate + quarantined + dead_letter + control + suppressed,
                      exactly, per tenant-hour. A surplus fails like a deficit: one tenant's missing
                      record must not be hidden by another tenant's extra one.
    A2 data quality   (quarantined + dead_letter) / edge <= 0.5%, per tenant-hour with >= 1,000 records
                      and over the tenant's whole window. A tenant with fewer than 1,000 records in the
                      window and any quarantined or dead-lettered record needs a recorded human review
                      (a line in the --approved file) before it can pass.
    A3 denominator    legacy_landed <= edge per tenant-hour. If the old system stored rows the edge
                      manifest never counted, the manifest is incomplete and A1 proves nothing.
    A4 freshness      canary p95 event->query < 5 s over the window (--canary-p95-ms). Missing means HOLD.
Records with no resolvable tenant ("unresolved") must still satisfy A1; there is no tenant to move.
Thresholds are [Assumed] starting points for the design review, not measured values.

Verdict: ROLLBACK if A1 fails anywhere, HOLD if anything else fails or is unmeasured, PASS otherwise.
Exit code 0 / 3 / 2 for PASS / HOLD / ROLLBACK so CI or a cutover script can act on it.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict

QUALITY_MAX = 0.005
LOW_VOLUME = 1_000
FRESHNESS_P95_MAX_MS = 5_000
OUTCOMES = ("new_accepted", "new_duplicate", "new_quarantined", "new_dead_letter", "new_control", "new_suppressed")


def evaluate(rows, canary_p95_ms=None, approved=frozenset()):
    """Returns (verdict, {tenant: [(check, detail)]}, global_problems, totals)."""
    problems = defaultdict(list)
    window = defaultdict(lambda: defaultdict(int))
    for r in rows:
        t, h = r["tenant_id"], r["hour"]
        n = defaultdict(int, {k: int(v) for k, v in r.items() if k not in ("tenant_id", "hour")})
        unexplained = n["edge"] - sum(n[k] for k in OUTCOMES)
        bad = n["new_quarantined"] + n["new_dead_letter"]
        if unexplained:
            problems[t].append(("A1", f"hour {h}: {unexplained:+d} of {n['edge']:,} records unaccounted for"))
        if t != "unresolved" and n["edge"] >= LOW_VOLUME and bad / n["edge"] > QUALITY_MAX:
            problems[t].append(("A2", f"hour {h}: {bad} of {n['edge']:,} quarantined or dead-lettered"))
        if n["legacy_landed"] > n["edge"]:
            problems[t].append(("A3", f"hour {h}: legacy stored {n['legacy_landed']:,} rows, edge counted {n['edge']:,}"))
        for k in ("edge", "legacy_landed", "new_duplicate", "new_quarantined", "new_dead_letter"):
            window[t][k] += n[k]
    for t, w in window.items():
        bad = w["new_quarantined"] + w["new_dead_letter"]
        if t == "unresolved" or not bad:
            continue
        if w["edge"] >= LOW_VOLUME and bad / w["edge"] > QUALITY_MAX:
            problems[t].append(("A2", f"window: {bad} of {w['edge']:,} quarantined or dead-lettered"))
        elif w["edge"] < LOW_VOLUME and t not in approved:
            problems[t].append(("A2-review", f"window: {bad} of {w['edge']:,} quarantined or dead-lettered, "
                                             f"under {LOW_VOLUME:,} records; needs a recorded review"))
    global_problems = []
    if canary_p95_ms is None:
        global_problems.append(("A4", "not measured: needs the staging canary p95 for this window"))
    elif canary_p95_ms >= FRESHNESS_P95_MAX_MS:
        global_problems.append(("A4", f"canary p95 {canary_p95_ms:,.0f} ms >= {FRESHNESS_P95_MAX_MS:,} ms"))
    any_a1 = any(c == "A1" for ps in problems.values() for c, _ in ps)
    verdict = "ROLLBACK" if any_a1 else "HOLD" if problems or global_problems else "PASS"
    totals = {k: sum(w[k] for w in window.values()) for k in ("edge", "legacy_landed", "new_duplicate")}
    totals["tenants"] = len([t for t in window if t != "unresolved"])
    return verdict, problems, global_problems, totals


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("counts", nargs="?", default="out/counts_clean.csv")
    ap.add_argument("--canary-p95-ms", type=float, help="A4 input: measured canary event->query p95 for the window")
    ap.add_argument("--approved", help="TSV of tenant_id, reviewer, note: low-volume tenants a person has reviewed")
    args = ap.parse_args(argv)
    with open(args.counts) as fh:
        rows = list(csv.DictReader(fh))
    approved = set()
    if args.approved:
        with open(args.approved) as fh:
            approved = {line.split("\t")[0].strip() for line in fh if line.strip() and not line.startswith("#")}
    verdict, problems, global_problems, tot = evaluate(rows, args.canary_p95_ms, approved)

    by_check = defaultdict(set)
    for t, ps in problems.items():
        for c, _ in ps:
            by_check[c].add(t)
    print(f"tenant-hours: {len(rows):,}   tenants: {tot['tenants']}   edge records: {tot['edge']:,}")
    for check, label in (("A1", "completeness"), ("A2", "data quality"), ("A2-review", "low-volume review"),
                         ("A3", "denominator vs legacy")):
        tenants = sorted(by_check.get(check, ()))
        print(f"{check:<10}{label:<23}{'ok' if not tenants else f'{len(tenants)} tenant(s): ' + ', '.join(tenants[:8])}")
        for t in tenants[:3]:
            for c, detail in problems[t]:
                if c == check:
                    print(f"              {t} {detail}")
                    break
    for c, detail in global_problems:
        print(f"{c:<10}{'freshness':<23}{detail}")
    if not global_problems:
        print(f"{'A4':<10}{'freshness':<23}ok ({args.canary_p95_ms:,.0f} ms)")
    clean = tot["tenants"] - len({t for t in problems if t != "unresolved"})
    print(f"legacy gap for context: {tot['edge'] - tot['legacy_landed']:,} records "
          f"({(tot['edge'] - tot['legacy_landed']) / tot['edge']:.2%}); legacy also stored "
          f"{tot['new_duplicate']:,} retries as real events")
    print(f"tenants with no tenant-level problem: {clean} of {tot['tenants']}")
    print(f"VERDICT: {verdict}")
    return {"PASS": 0, "HOLD": 3, "ROLLBACK": 2}[verdict]


if __name__ == "__main__":
    sys.exit(main())
