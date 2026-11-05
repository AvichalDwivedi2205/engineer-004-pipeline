"""Ingest gate: the stage between the HTTP collector and everything downstream.

Every input record ends in exactly one bucket:

    accepted     -> canonical event, goes to the real-time store and the lake
    duplicate    -> same (tenant_id, event_id) and same content as an earlier record; counted, not stored
    quarantined  -> parseable but not trustworthy (no tenant, id conflict, unknown type, ...); kept in S3 for review
    dead_letter  -> not parseable at all; raw bytes kept in S3
    control      -> not analytics (privacy requests); routed to the DSR workflow
    suppressed   -> belongs to a subject whose deletion was executed; counted, content discarded,
                    so a replay from Kinesis, the raw archive or the S3 spill cannot resurrect it

so `sum(buckets) == records_in` can be checked per tenant per minute. That invariant
is how loss is detected instead of assumed away.

Delivery is at-least-once, so the gate is used one Kinesis batch at a time:

    gate.begin_batch(); outcomes = [gate.process_line(r) for r in batch]
    write sinks (ClickHouse, Firehose); then gate.commit_batch()   # or abort_batch() on any sink error

Dedupe keys from a batch become visible to later batches only at commit, after every
sink has acknowledged. A sink failure therefore redelivers the batch as new events (the
sinks dedupe the partial first write) instead of as duplicates that never get stored.
Outside a batch (fixture replay, tests) keys commit immediately.

Thresholds below are design assumptions, not measured values. They are the knobs a
reviewer should argue with; see ANSWER.md for why each was picked.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

# [Assumed] client clock may run ahead of the server by up to 5 s before we stop trusting it.
MAX_CLIENT_AHEAD_MS = 5_000
# [Assumed] events older than 7 days at arrival are treated as replays/backfill, not real time.
MAX_CLIENT_BEHIND_MS = 7 * 24 * 3600 * 1000
# [Assumed] 4 page views inside 1 s from one visitor is not a human reading pages.
BOT_BURST_COUNT = 4
BOT_BURST_WINDOW_MS = 1_000
BOT_HOST_TOKENS = {"bot", "crawler", "spider", "scanner", "headless"}
# [Assumed] one user stitched to more than 50 anonymous ids is a shared/test account, not a person.
IDENTITY_SUPERNODE_LIMIT = 50
# [Assumed] events arriving more than 60 s after they happened update state but never fire a
# personalization trigger (a visitor who left an hour ago should not get a pop-up now).
TRIGGER_MAX_AGE_MS = 60_000
# [Assumed] TTL of the dedupe keys in Valkey. A retry older than this is accepted again; ClickHouse
# (ReplacingMergeTree on tenant_id, event_id) and the lake export dedupe it, and it does not touch
# personalization counters, which the nightly rebuild from ClickHouse corrects.
DEDUPE_TTL_MS = 3_600_000

ANALYTICS_TYPES = {"page_view", "click", "form_submit", "custom", "identify"}
CONTROL_TYPES = {"privacy_request"}

# Legacy SDK payloads still in the wild. We cannot make customers upgrade, so the
# gate maps them to the canonical shape. Only aliases actually seen in data go here.
LEGACY_TYPE_ALIASES = {"pageview": "page_view"}
LEGACY_TOP_LEVEL_FIELDS = {"timestamp": "ts"}
LEGACY_PROPERTY_FIELDS = {"page_path": "path", "ref": "referrer"}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"^\+?[\d\s().-]{7,20}$")
PII_KEY_HINTS = ("email", "phone", "mobile", "ssn", "address")


def parse_ts(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def ms_between(later: datetime, earlier: datetime) -> int:
    return int((later - earlier).total_seconds() * 1000)


def host_tokens(url: str | None) -> set[str]:
    if not url or "://" not in url:
        return set()
    host = url.split("://", 1)[1].split("/", 1)[0].lower()
    return set(re.split(r"[.\-]", host))


def content_fingerprint(ev: dict) -> str:
    """Hash of what the client sent. Excludes received_at so a retry of the same
    event fingerprints identically even though it arrived later."""
    body = {k: ev.get(k) for k in ("type", "anonymous_id", "user_id", "client_ts", "properties")}
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()


BOT_FLAGS = {"bot_referrer", "bot_rate", "bot_visitor"}


def _text(value) -> str:
    if value is None:
        return ""  # evt-0004 sends "referrer": null
    return value if isinstance(value, str) else json.dumps(value)


def clickhouse_row(ev: dict, ingest_node: str = "", ingest_seq: int = 0, version_ms: int = 0) -> dict:
    """An accepted event as a row of `events` (schema/clickhouse.sql). The rollup view groups on
    `path` and filters on `is_bot`, so both are lifted out of properties here. properties is
    stored as a JSON string: Firehose keeps only the first level of nested JSON for Iceberg, so
    the lake gets this same flat row."""
    flags = list(ev.get("flags", []))
    props = ev.get("properties") or {}
    return {
        "tenant_id": ev["tenant_id"], "event_id": ev["event_id"],
        "anonymous_id": ev.get("anonymous_id") or "", "user_id": ev.get("user_id"),
        "type": ev["type"], "client_ts": ev.get("client_ts"), "received_at": ev.get("received_at"),
        "event_time": ev["event_time"], "event_time_source": ev["event_time_source"],
        "path": _text(props.get("path")), "referrer": _text(props.get("referrer")),
        "properties": json.dumps(props, sort_keys=True, separators=(",", ":")),
        "flags": flags, "is_bot": bool(BOT_FLAGS & set(flags)),
        "ingest_node": ingest_node, "ingest_seq": ingest_seq, "version": version_ms,
    }


@dataclass
class Outcome:
    bucket: str
    reason: str | None
    event: dict | None
    raw: str | None = None
    line_no: int | None = None


@dataclass
class GateStats:
    records_in: int = 0
    buckets: dict = field(default_factory=lambda: defaultdict(int))
    reasons: dict = field(default_factory=lambda: defaultdict(int))
    flags: dict = field(default_factory=lambda: defaultdict(int))

    def balanced(self) -> bool:
        return sum(self.buckets.values()) == self.records_in


class IngestGate:
    """One instance per Kinesis shard consumer. Every attribute except `_pending` stands in
    for state that lives in Valkey (dedupe keys, bot windows) or Postgres (identity, DSR
    tickets, tombstones) in production, so it survives a process restart."""

    def __init__(self, pii_hmac_key: bytes = b"per-tenant-key-from-kms", tombstones: set | None = None):
        self.stats = GateStats()
        # (tenant_id, user_id or anonymous_id) of executed deletions; loaded from Postgres in production.
        self.tombstones: set[tuple] = set(tombstones or ())
        self._seen: dict[tuple, tuple[str, datetime | None]] = {}
        self._pending: dict[tuple, tuple[str, datetime | None]] | None = None
        self._pv_times: dict[tuple, deque] = defaultdict(deque)
        self.bot_visitors: set[tuple] = set()
        self.identity: dict[tuple, str] = {}
        self._anon_count_per_user: dict[tuple, set] = defaultdict(set)
        self.identity_events: list[dict] = []
        self.identity_candidates: list[dict] = []
        self.dsr_tickets: list[dict] = []
        self._pii_key = pii_hmac_key

    # ---- public entry points -------------------------------------------------

    def process_line(self, raw: str, line_no: int | None = None, key_tenant: str | None = None,
                     server_received_at: str | None = None, replay: bool = False,
                     arrival_ts: str | None = None) -> Outcome:
        """`key_tenant` is the tenant resolved from the SDK write key at the collector.
        `server_received_at` is stamped by the collector; `arrival_ts` is Kinesis's
        ApproximateArrivalTimestamp. All are None when replaying an export (like the
        fixture), and the gate degrades to trusting the payload."""
        self.stats.records_in += 1
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("top-level JSON is not an object")
        except ValueError as exc:
            return self._finish(Outcome("dead_letter", f"unparseable: {exc.__class__.__name__}",
                                        None, raw=raw, line_no=line_no))
        return self._finish(self._process(payload, key_tenant, server_received_at, line_no, replay,
                                          parse_ts(arrival_ts)))

    def begin_batch(self):
        self._pending = {}

    def commit_batch(self):
        """Call only after every sink acknowledged the batch's accepted events."""
        self._seen.update(self._pending or {})
        self._pending = None

    def abort_batch(self):
        """A sink failed or the process is going down: forget this batch's dedupe keys so the
        redelivered batch is processed as new, not marked duplicate of a write that never landed."""
        self._pending = None

    def restart(self) -> "IngestGate":
        """The process after a crash: durable state carries over, the open batch does not."""
        g = IngestGate.__new__(IngestGate)
        g.__dict__.update(self.__dict__)
        g._pending = None
        return g

    # ---- pipeline ------------------------------------------------------------

    def _process(self, p: dict, key_tenant, server_received_at, line_no, replay=False, arrival=None) -> Outcome:
        flags: list[str] = []
        ev, variant = self._normalize(p, flags)
        # Redact first, so quarantined copies are scrubbed too. Only the raw archive and the
        # dead-letter store hold unredacted bytes.
        self._redact_pii(ev, key_tenant or ev.get("tenant_id") or "unresolved", flags)

        payload_tenant = ev.get("tenant_id")
        if key_tenant and payload_tenant and key_tenant != payload_tenant:
            return Outcome("quarantined", "tenant_mismatch", ev, line_no=line_no)
        ev["tenant_id"] = key_tenant or payload_tenant
        if not ev["tenant_id"]:
            return Outcome("quarantined", "tenant_unresolved", ev, line_no=line_no)

        if not ev.get("event_id"):
            ev["event_id"] = "syn-" + content_fingerprint(ev)[:16]
            flags.append("event_id_synthesized")
        if not ev.get("anonymous_id") and not ev.get("user_id"):
            return Outcome("quarantined", "no_identity", ev, line_no=line_no)
        if {(ev["tenant_id"], ev.get("anonymous_id")), (ev["tenant_id"], ev.get("user_id"))} & self.tombstones:
            return Outcome("suppressed", "deleted_subject", None, line_no=line_no)

        received = parse_ts(server_received_at) or parse_ts(ev.get("received_at"))
        if received is None:
            flags.append("received_at_missing")
            # Kinesis's arrival time is server time too, a few ms after the collector would have
            # stamped. Using it keeps a stampless record (evt-0009) eligible for counters.
            if arrival is not None:
                received = arrival
                flags.append("received_at_from_kinesis")
        ev["received_at"] = received.isoformat() if received else None

        dup = self._dedupe(ev, received)
        if dup:
            return Outcome(dup[0], dup[1], ev, line_no=line_no)

        if ev["type"] in CONTROL_TYPES:
            self._open_dsr(ev)
            return Outcome("control", "privacy_request_pending_verification", ev, line_no=line_no)
        if ev["type"] not in ANALYTICS_TYPES:
            return Outcome("quarantined", f"unknown_type:{ev['type']}", ev, line_no=line_no)

        client = parse_ts(ev.get("client_ts"))
        if client is None:
            return Outcome("quarantined", "client_ts_unparseable", ev, line_no=line_no)
        self._apply_time_policy(ev, client, received, flags)
        # How old the event is, known only when the client clock is trusted. With an untrusted
        # clock a retry that outlived its dedupe key looks new, so it must not touch counters or
        # triggers (the spike drill double-counted one such retry before this rule).
        age = ms_between(received, client) if ev["event_time_source"] == "client" else None
        if replay or age is None or age > TRIGGER_MAX_AGE_MS:
            flags.append("no_trigger")
        if replay or age is None or age > DEDUPE_TTL_MS:
            flags.append("no_state_update")
        self._bot_check(ev, client, flags)
        self._stitch(ev, flags)

        ev["schema_variant"] = variant
        ev["flags"] = flags
        return Outcome("accepted", None, ev, line_no=line_no)

    def _normalize(self, p: dict, flags: list[str]) -> tuple[dict, str]:
        variant = "v1"
        p = dict(p)
        for old, new in LEGACY_TOP_LEVEL_FIELDS.items():
            if old in p and new not in p:
                p[new] = p.pop(old)
                variant = "legacy"
        props = p.get("properties") or {}
        if not isinstance(props, dict):
            props = {"_unstructured": props}
            flags.append("properties_not_object")
        props = dict(props)
        for old, new in LEGACY_PROPERTY_FIELDS.items():
            if old in props and new not in props:
                props[new] = props.pop(old)
                variant = "legacy"
        etype = p.get("type")
        if etype in LEGACY_TYPE_ALIASES:
            etype = LEGACY_TYPE_ALIASES[etype]
            variant = "legacy"
        if variant == "legacy":
            flags.append("legacy_schema_mapped")
        ev = {
            "event_id": p.get("event_id"),
            "tenant_id": p.get("tenant_id"),
            "anonymous_id": p.get("anonymous_id"),
            "user_id": p.get("user_id"),
            "type": etype,
            "client_ts": p.get("ts"),
            "received_at": p.get("received_at"),
            "properties": props,
        }
        return ev, variant

    def _dedupe(self, ev: dict, received: datetime | None):
        key = (ev["tenant_id"], ev["event_id"])
        fp = content_fingerprint(ev)
        prior = (self._pending or {}).get(key) or self._seen.get(key)
        if prior and received and prior[1] and ms_between(received, prior[1]) > DEDUPE_TTL_MS:
            prior = None  # the Valkey key would have expired by now
        if prior:
            first_fp, first_received = prior
            if first_fp == fp:
                gap = ms_between(received, first_received) if received and first_received else None
                ev["retry_gap_ms"] = gap
                return ("duplicate", "same_id_same_content")
            return ("quarantined", "event_id_conflict")
        (self._pending if self._pending is not None else self._seen)[key] = (fp, received)
        return None

    def _apply_time_policy(self, ev, client: datetime, received: datetime | None, flags):
        """Dashboards and storage partitions use server time. Client time is kept for
        in-session ordering only, and only when it is plausible."""
        if received is None:
            # Neither a collector stamp nor a Kinesis arrival time: only an offline replay of an
            # export (evt-0009 in the fixture) gets here, and it is marked as such.
            ev["event_time"], ev["event_time_source"] = client.isoformat(), "client_unverified"
            flags.append("partition_time_unverified")
            ev["partition_date"] = client.date().isoformat()
            return
        skew = ms_between(client, received)
        ev["client_skew_ms"] = skew
        if skew > MAX_CLIENT_AHEAD_MS:
            flags.append("client_clock_ahead")
            ev["event_time"], ev["event_time_source"] = received.isoformat(), "server"
        elif -skew > MAX_CLIENT_BEHIND_MS:
            flags.append("late_backfill")
            ev["event_time"], ev["event_time_source"] = received.isoformat(), "server"
        else:
            ev["event_time"], ev["event_time_source"] = client.isoformat(), "client"
        ev["partition_date"] = received.date().isoformat()

    def _redact_pii(self, ev, tenant: str, flags):
        """HMAC is pseudonymisation, not anonymisation: the tokens stay personal data and are
        deleted with the subject's rows."""
        hits = []

        def scrub(key: str, value):
            if isinstance(value, dict):
                return {k: scrub(k, v) for k, v in value.items()}
            if isinstance(value, list):
                return [scrub(key, v) for v in value]
            if not isinstance(value, str):
                return value
            key_hint = any(h in key.lower() for h in PII_KEY_HINTS)
            if EMAIL_RE.search(value) or (key_hint and PHONE_RE.match(value)) or \
                    (PHONE_RE.match(value) and sum(c.isdigit() for c in value) >= 10):
                hits.append(key)
                digest = hmac.new(self._pii_key, f"{tenant}|{value}".encode(), hashlib.sha256).hexdigest()[:16]
                return f"pii:hmac:{digest}"
            return value

        ev["properties"] = {k: scrub(k, v) for k, v in ev["properties"].items()}
        if hits:
            flags.append("pii_redacted")
            ev["pii_fields"] = sorted(set(hits))

    def _bot_check(self, ev, client: datetime, flags):
        visitor = (ev["tenant_id"], ev.get("anonymous_id"))
        if BOT_HOST_TOKENS & host_tokens(ev["properties"].get("referrer")):
            flags.append("bot_referrer")
            self.bot_visitors.add(visitor)
        if ev["type"] == "page_view":
            # Window on server time: a client clock a year ahead must not sit in the window
            # forever (tools/bot_window_regression.py). Entries are keyed by event_id, as a
            # Valkey sorted set would be, so reprocessing a redelivered batch adds nothing.
            now = parse_ts(ev.get("received_at")) or client
            window = self._pv_times[visitor]
            if all(eid != ev["event_id"] for _, eid in window):
                window.append((now, ev["event_id"]))
            while window and abs(ms_between(now, window[0][0])) > BOT_BURST_WINDOW_MS:
                window.popleft()
            if len(window) >= BOT_BURST_COUNT:
                flags.append("bot_rate")
                self.bot_visitors.add(visitor)
        if visitor in self.bot_visitors and "bot_referrer" not in flags and "bot_rate" not in flags:
            flags.append("bot_visitor")

    def _stitch(self, ev, flags):
        """Every id from a browser is a claim. An identify call is trusted for merging because the
        tenant's own page code makes it after login. Any other event that happens to carry both
        ids (evt-0016) is recorded as a candidate: it is not merged and never widens a deletion
        scope. A server-side identify signed with the tenant's secret key would be the
        authenticated kind (post-MVP)."""
        anon, user, tenant = ev.get("anonymous_id"), ev.get("user_id"), ev["tenant_id"]
        if not (anon and user):
            return
        key = (tenant, anon)
        known = self.identity.get(key)
        if ev["type"] != "identify":
            if known != user:
                flags.append("identity_candidate")
                self.identity_candidates.append({"tenant_id": tenant, "anonymous_id": anon, "user_id": user,
                                                 "via": ev["type"], "event_id": ev["event_id"]})
            return
        if known and known != user:
            flags.append("identity_conflict")
            return
        if known is None:
            self.identity[key] = user
            self._anon_count_per_user[(tenant, user)].add(anon)
            self.identity_events.append({"tenant_id": tenant, "anonymous_id": anon, "user_id": user,
                                         "via": ev["type"], "event_id": ev["event_id"]})
            if len(self._anon_count_per_user[(tenant, user)]) > IDENTITY_SUPERNODE_LIMIT:
                flags.append("identity_supernode")

    def _open_dsr(self, ev):
        """An SDK-originated privacy request is unauthenticated: anyone can POST one with
        any user_id. It opens a ticket the tenant must confirm; deletion never runs from it
        directly, and it does not add an identity link. Idempotent on the request's event_id."""
        if any(t["tenant_id"] == ev["tenant_id"] and t["event_id"] == ev["event_id"] for t in self.dsr_tickets):
            return
        self.dsr_tickets.append({
            "tenant_id": ev["tenant_id"], "user_id": ev.get("user_id"),
            "anonymous_id": ev.get("anonymous_id"),
            "request": (ev.get("properties") or {}).get("request"),
            "regulation": (ev.get("properties") or {}).get("regulation"),
            "event_id": ev["event_id"], "status": "pending_tenant_verification",
            "source": "sdk_unauthenticated",
        })

    def _finish(self, out: Outcome) -> Outcome:
        self.stats.buckets[out.bucket] += 1
        if out.reason:
            self.stats.reasons[out.reason] += 1
        for f in (out.event or {}).get("flags", []):
            self.stats.flags[f] += 1
        return out
