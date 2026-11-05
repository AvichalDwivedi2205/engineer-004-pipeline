#!/usr/bin/env python3
"""Run the production ingest gate over the fixture and add audit-only cross-checks.

    python3 tools/fixture_audit.py fixtures/event_sample.jsonl

Writes out/fixture_audit.md and out/fixture_audit.json. Exits 1 if the bucket
invariant (every record lands in exactly one bucket) does not hold.
Stdlib only.
"""
from __future__ import annotations

import hashlib
import json
import re
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from ingest_gate.gate import IngestGate, clickhouse_row, parse_ts, ms_between  # noqa: E402


def main(path: Path) -> int:
    raw_bytes = path.read_bytes()
    sha = hashlib.sha256(raw_bytes).hexdigest()
    lines = raw_bytes.decode("utf-8").splitlines()

    gate = IngestGate()
    outcomes = [gate.process_line(line, line_no=i) for i, line in enumerate(lines, 1)]

    # Lenient view of every parseable line, for audit-only checks that look across records.
    loose = []
    for i, line in enumerate(lines, 1):
        try:
            loose.append((i, json.loads(line)))
        except ValueError:
            pass

    findings = defaultdict(list)
    for o in outcomes:
        eid = (o.event or {}).get("event_id") or _regex_id(o.raw)
        if o.bucket != "accepted":
            findings[o.reason].append((eid, o.line_no))
        for f in (o.event or {}).get("flags", []):
            findings[f].append((eid, o.line_no))

    audit = {
        "fixture": str(path.name), "sha256": sha, "records_in": gate.stats.records_in,
        "buckets": dict(gate.stats.buckets), "invariant_balanced": gate.stats.balanced(),
        "findings": {k: v for k, v in sorted(findings.items())},
        "dsr_tickets": gate.dsr_tickets, "identity_links": gate.identity_events,
        "identity_candidates": gate.identity_candidates,
        "bot_visitors": sorted(f"{t}/{a}" for t, a in gate.bot_visitors),
    }

    # 1. Arrival order: does file order agree with server received_at?
    order_breaks, prev = [], None
    for i, p in loose:
        r = parse_ts(p.get("received_at"))
        if r and prev and r < prev[1]:
            order_breaks.append({"line": i, "event_id": p.get("event_id"), "received_at": p["received_at"],
                                 "previous_line": prev[0]})
        if r:
            prev = (i, r)
    audit["arrival_order_breaks"] = order_breaks
    audit["duplicates"] = [{"event_id": o.event["event_id"], "line": o.line_no,
                            "retry_gap_ms": o.event.get("retry_gap_ms")}
                           for o in outcomes if o.bucket == "duplicate"]

    # 2. Transit time (received_at - ts) for records whose client clock looks sane.
    transit = []
    for o in outcomes:
        ev = o.event or {}
        if o.bucket == "accepted" and ev.get("event_time_source") == "client":
            transit.append(-ev["client_skew_ms"])
    audit["transit_ms_sane_clients"] = {
        "n": len(transit), "min": min(transit), "median": statistics.median(transit), "max": max(transit),
    } if transit else {}
    audit["skewed_clients"] = [
        {"event_id": o.event["event_id"], "client_minus_server_ms": o.event["client_skew_ms"],
         "client_ts": o.event["client_ts"], "received_at": o.event["received_at"]}
        for o in outcomes if o.event and "client_clock_ahead" in o.event.get("flags", [])]

    # 3. Client-computed aggregates vs what the raw stream shows (same tenant, stitched identity).
    accepted = [o.event for o in outcomes if o.bucket == "accepted"]
    ident = {(l["tenant_id"], l["anonymous_id"]): l["user_id"] for l in gate.identity_events}

    def person(ev):
        return ev.get("user_id") or ident.get((ev["tenant_id"], ev.get("anonymous_id"))) or \
            f"anon:{ev.get('anonymous_id')}"

    client_counts = []
    for ev in accepted:
        props = ev["properties"]
        for k, v in props.items():
            if k.startswith("count") and isinstance(v, (int, float)):
                name = props.get("name", "")
                m = re.match(r"viewed_(\w+)", name)
                path_hint = f"/{m.group(1)}" if m else None
                seen = [e["event_id"] for e in accepted
                        if e["tenant_id"] == ev["tenant_id"] and person(e) == person(ev)
                        and e["type"] == "page_view" and e["properties"].get("path") == path_hint
                        and "bot_visitor" not in e["flags"]]
                client_counts.append({"event_id": ev["event_id"], "claims": f"{name}.{k}={v}",
                                      "person": person(ev), "raw_page_views_in_sample": seen})
    audit["client_reported_aggregates"] = client_counts

    # 4. Pricing-page views per tenant: naive count vs gated count. The brief's own segment example.
    def is_pricing_view(p):
        props = p.get("properties") or {}
        return p.get("type") == "page_view" and props.get("path") == "/pricing"
    naive, gated = defaultdict(list), defaultdict(list)
    for _, p in loose:
        if is_pricing_view(p):
            naive[p.get("tenant_id")].append(p["event_id"])
    for e in accepted:
        if is_pricing_view(e) and (e["tenant_id"], e.get("anonymous_id")) not in gate.bot_visitors:
            gated[e["tenant_id"]].append(e["event_id"])
    # The same count as page_views_1m_mv sees it: the gate's ClickHouse row, live flags only.
    rollup = defaultdict(list)
    for e in accepted:
        r = clickhouse_row(e)
        if r["type"] == "page_view" and r["path"] == "/pricing" and not r["is_bot"]:
            rollup[r["tenant_id"]].append(r["event_id"])
    audit["pricing_views_by_tenant"] = {"naive_exact_type_match": dict(naive), "after_gate": dict(gated),
                                        "rollup_view_filter": dict(rollup)}

    # 5. Deletion scope for each privacy request, and why user_id alone is not enough.
    scope = []
    for t in gate.dsr_tickets:
        by_user = [e["event_id"] for e in accepted if e["tenant_id"] == t["tenant_id"]
                   and person(e) == t["user_id"]]
        by_anon = [e["event_id"] for e in accepted if e["tenant_id"] == t["tenant_id"]
                   and e.get("anonymous_id") == t["anonymous_id"]]
        verified_link = ident.get((t["tenant_id"], t["anonymous_id"])) == t["user_id"]
        scope.append({"ticket_event_id": t["event_id"], "user_id": t["user_id"],
                      "events_reachable_by_user_id": by_user,
                      "events_reachable_only_via_request_anon_id": sorted(set(by_anon) - set(by_user)),
                      "anon_link_confirmed_by_identify": verified_link})
    audit["deletion_scope"] = scope

    # 6. PII that becomes attached to a known user once stitching runs.
    audit["pii_linked_after_stitch"] = [
        {"event_id": e["event_id"], "fields": e.get("pii_fields"), "now_attributed_to": person(e)}
        for e in accepted if "pii_redacted" in e["flags"]]

    # 7. Event id sequence (only meaningful if ids were sequential, which production ids are not).
    nums = sorted({int(m.group(1)) for _, p in loose
                   if (m := re.match(r"evt-(\d+)$", str(p.get("event_id"))))}
                  | {int(m.group(1)) for o in outcomes if o.raw and (m := re.search(r'"evt-(\d+)"', o.raw))})
    audit["event_id_gaps"] = [n for n in range(nums[0], nums[-1] + 1) if n not in nums] if nums else []
    audit["tenant_mix"] = dict(Counter(str(p.get("tenant_id")) for _, p in loose))

    out = ROOT / "out"
    out.mkdir(exist_ok=True)
    (out / "fixture_audit.json").write_text(json.dumps(audit, indent=2, default=str))
    (out / "fixture_audit.md").write_text(render(audit))
    print(render(audit))
    return 0 if audit["invariant_balanced"] else 1


def _regex_id(raw):
    m = re.search(r'"event_id"\s*:\s*"([^"]+)"', raw or "")
    return m.group(1) if m else None


def render(a: dict) -> str:
    L = [f"# Fixture audit: {a['fixture']}", "",
         f"sha256 `{a['sha256']}`", "",
         f"records_in={a['records_in']}  buckets={a['buckets']}  invariant_balanced={a['invariant_balanced']}", "",
         "## Findings by class (gate output)", "", "| class | event_ids (line) |", "|---|---|"]
    for k, v in a["findings"].items():
        L.append(f"| {k} | " + ", ".join(f"{e} (L{n})" for e, n in v) + " |")
    L += ["", "## Audit-only cross-checks", ""]
    L.append(f"- Arrival-order breaks (file order vs received_at): {a['arrival_order_breaks']}")
    for d in a["duplicates"]:
        L.append(f"- Duplicate {d['event_id']} (L{d['line']}) re-sent {d['retry_gap_ms']} ms after the first copy")
    L.append(f"- Transit ms for sane clients: {a['transit_ms_sane_clients']}")
    for s in a["skewed_clients"]:
        L.append(f"- Clock ahead: {s['event_id']} client-server = {s['client_minus_server_ms']} ms "
                 f"({s['client_ts']} vs {s['received_at']})")
    for c in a["client_reported_aggregates"]:
        L.append(f"- Client aggregate {c['event_id']} claims {c['claims']} for {c['person']}; "
                 f"non-bot raw page views in sample: {c['raw_page_views_in_sample']}")
    L.append(f"- /pricing page views by tenant: {a['pricing_views_by_tenant']}")
    for d in a["deletion_scope"]:
        L.append(f"- DSR {d['ticket_event_id']} for {d['user_id']}: by user_id -> {d['events_reachable_by_user_id']}; "
                 f"only via the request's anon id -> {d['events_reachable_only_via_request_anon_id']}; "
                 f"anon link confirmed by an identify event: {d['anon_link_confirmed_by_identify']}")
    for p in a["pii_linked_after_stitch"]:
        L.append(f"- PII {p['fields']} in {p['event_id']} becomes attributable to {p['now_attributed_to']} after stitching")
    L.append(f"- Identity links: " + ", ".join(f"{l['anonymous_id']}->{l['user_id']} via {l['via']} ({l['event_id']})"
                                              for l in a["identity_links"]))
    L.append("- Identity candidates (not merged): " + ", ".join(
        f"{l['anonymous_id']}->{l['user_id']} via {l['via']} ({l['event_id']})" for l in a["identity_candidates"]))
    L.append(f"- Bot visitors: {a['bot_visitors']}")
    L.append(f"- DSR tickets: {[(t['event_id'], t['status']) for t in a['dsr_tickets']]}")
    L.append(f"- Event-id gaps: {a['event_id_gaps']}  tenant mix: {a['tenant_mix']}")
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1] if len(sys.argv) > 1 else ROOT / "fixtures" / "event_sample.jsonl")))
