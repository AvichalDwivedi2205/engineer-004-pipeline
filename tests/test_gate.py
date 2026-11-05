"""python3 -m unittest discover -s tests -v"""
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from ingest_gate.gate import IngestGate, clickhouse_row  # noqa: E402

FIXTURE = ROOT / "fixtures" / "event_sample.jsonl"


def ev(**kw):
    base = {"event_id": "e1", "tenant_id": "t-1", "anonymous_id": "a1", "user_id": None, "type": "page_view",
            "ts": "2026-06-15T14:00:00.000Z", "received_at": "2026-06-15T14:00:00.150Z",
            "properties": {"path": "/"}}
    base.update(kw)
    return json.dumps(base)


class FixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gate = IngestGate()
        cls.out = {}
        for i, line in enumerate(FIXTURE.read_text().splitlines(), 1):
            o = cls.gate.process_line(line, line_no=i)
            cls.out[i] = o

    def by_id(self, eid):
        return [o for o in self.out.values() if o.event and o.event["event_id"] == eid]

    def test_every_record_lands_in_exactly_one_bucket(self):
        self.assertTrue(self.gate.stats.balanced())
        self.assertEqual(self.gate.stats.records_in, 25)

    def test_retry_duplicate_is_counted_not_stored(self):
        first, second = self.by_id("evt-0002")
        self.assertEqual((first.bucket, second.bucket), ("accepted", "duplicate"))

    def test_truncated_json_goes_to_dead_letter_with_raw_bytes(self):
        self.assertEqual(self.out[21].bucket, "dead_letter")
        self.assertIn("evt-0020", self.out[21].raw)

    def test_null_tenant_is_quarantined_not_guessed(self):
        self.assertEqual(self.by_id("evt-0011")[0].reason, "tenant_unresolved")

    def test_future_client_clocks_fall_back_to_server_time(self):
        for eid in ("evt-0005", "evt-0006", "evt-0016"):
            e = self.by_id(eid)[0].event
            self.assertIn("client_clock_ahead", e["flags"], eid)
            self.assertEqual(e["event_time_source"], "server")
            self.assertEqual(e["partition_date"], "2026-06-15")  # not a 2027 partition

    def test_legacy_payload_is_mapped_not_dropped(self):
        e = self.by_id("evt-0009")[0].event
        self.assertEqual((e["type"], e["properties"]["path"], e["properties"]["referrer"]),
                         ("page_view", "/pricing", "https://bing.com"))
        self.assertIn("received_at_missing", e["flags"])

    def test_pii_is_redacted_before_storage(self):
        e = self.by_id("evt-0007")[0].event
        blob = json.dumps(e)
        self.assertNotIn("maria.gonzalez", blob)
        self.assertNotIn("555-0142", blob)
        self.assertEqual(e["pii_fields"], ["contact_email", "phone"])

    def test_scanner_visitor_flagged_not_dropped(self):
        for eid in ("evt-0012", "evt-0013", "evt-0014", "evt-0015"):
            o = self.by_id(eid)[0]
            self.assertEqual(o.bucket, "accepted")
            self.assertIn("bot_referrer", o.event["flags"], eid)  # ANSWER.md: the referrer rule catches all four

    def test_privacy_request_is_control_traffic_without_identity_link(self):
        o = self.by_id("evt-0017")[0]
        self.assertEqual(o.bucket, "control")
        self.assertEqual(self.gate.dsr_tickets[0]["status"], "pending_tenant_verification")
        self.assertNotIn(("t-088", "anon-77a"), self.gate.identity)

    def test_identify_links_are_tenant_scoped(self):
        self.assertEqual(self.gate.identity[("t-042", "anon-9f2")], "u-5511")
        self.assertEqual(self.gate.identity[("t-042", "anon-3d0")], "u-7304")


class UnhappyPathTests(unittest.TestCase):
    def test_payload_tenant_cannot_override_write_key_tenant(self):
        o = IngestGate().process_line(ev(tenant_id="t-victim"), key_tenant="t-attacker")
        self.assertEqual((o.bucket, o.reason), ("quarantined", "tenant_mismatch"))

    def test_same_id_different_content_is_a_conflict_not_a_dedupe(self):
        g = IngestGate()
        g.process_line(ev())
        o = g.process_line(ev(properties={"path": "/other"}))
        self.assertEqual(o.reason, "event_id_conflict")

    def test_same_event_id_in_two_tenants_is_not_a_duplicate(self):
        g = IngestGate()
        g.process_line(ev(tenant_id="t-1"))
        self.assertEqual(g.process_line(ev(tenant_id="t-2")).bucket, "accepted")

    def test_replaying_a_batch_after_a_crash_changes_nothing(self):
        g = IngestGate()
        batch = [ev(event_id=f"e{i}") for i in range(100)]
        first = [g.process_line(x).bucket for x in batch]
        replay = [g.process_line(x).bucket for x in batch]
        self.assertEqual(first.count("accepted"), 100)
        self.assertEqual(replay.count("duplicate"), 100)
        self.assertTrue(g.stats.balanced())

    def test_unknown_event_type_is_quarantined(self):
        self.assertEqual(IngestGate().process_line(ev(type="purchase_v9")).reason, "unknown_type:purchase_v9")

    def test_nested_pii_and_pii_in_urls(self):
        o = IngestGate().process_line(ev(properties={"path": "/", "form": {"work_email": "a@b.io"},
                                                     "referrer": "https://x.com/?e=a@b.io"}))
        self.assertNotIn("a@b.io", json.dumps(o.event))

    def test_hostname_containing_bot_substring_is_not_a_bot(self):
        o = IngestGate().process_line(ev(properties={"path": "/", "referrer": "https://robotics-weekly.com/x"}))
        self.assertFalse({"bot_referrer", "bot_visitor"} & set(o.event["flags"]))

    def test_dates_are_not_mistaken_for_phone_numbers(self):
        o = IngestGate().process_line(ev(properties={"path": "/", "trial_end": "2026-07-01"}))
        self.assertNotIn("pii_redacted", o.event["flags"])

    def test_skewed_client_clock_does_not_poison_bot_window(self):
        g = IngestGate()
        g.process_line(ev(event_id="s", ts="2027-06-15T14:00:00.000Z"))
        flags = []
        for i in range(5):
            minute = f"2026-06-15T14:0{i + 1}:00"
            o = g.process_line(ev(event_id=f"h{i}", ts=minute + ".000Z", received_at=minute + ".150Z"))
            flags += o.event["flags"]
        self.assertNotIn("bot_rate", flags)

    def test_replay_after_deletion_does_not_resurrect_the_subject(self):
        lines = FIXTURE.read_text().splitlines()
        g = IngestGate(tombstones={("t-088", "anon-77a"), ("t-088", "u-1077")})
        o = g.process_line(lines[6])  # evt-0006, anon-77a
        self.assertEqual((o.bucket, o.reason, o.event), ("suppressed", "deleted_subject", None))
        self.assertTrue(g.stats.balanced())

    def test_late_and_replayed_events_update_state_but_cannot_trigger(self):
        g = IngestGate()
        late = g.process_line(ev(event_id="l", received_at="2026-06-15T14:02:00.000Z"))
        fresh = g.process_line(ev(event_id="f"))
        replayed = g.process_line(ev(event_id="r"), replay=True)
        self.assertIn("no_trigger", late.event["flags"])
        self.assertNotIn("no_trigger", fresh.event["flags"])
        self.assertIn("no_trigger", replayed.event["flags"])

    def test_non_object_json_is_dead_lettered(self):
        self.assertEqual(IngestGate().process_line("[1,2,3]").bucket, "dead_letter")

    def test_anonymous_id_reassigned_to_second_user_is_flagged_not_merged(self):
        g = IngestGate()
        g.process_line(ev(event_id="i1", type="identify", user_id="u-1"))
        o = g.process_line(ev(event_id="i2", type="identify", user_id="u-2"))
        self.assertIn("identity_conflict", o.event["flags"])
        self.assertEqual(g.identity[("t-1", "a1")], "u-1")

    def test_custom_event_carrying_both_ids_is_a_candidate_not_a_merge(self):
        g = IngestGate()
        lines = FIXTURE.read_text().splitlines()
        o = g.process_line(lines[16])  # evt-0016, custom event with anon-52d and u-8842
        self.assertIn("identity_candidate", o.event["flags"])
        self.assertNotIn(("t-017", "anon-52d"), g.identity)
        self.assertEqual(g.identity_candidates[0]["event_id"], "evt-0016")


class DeliveryTests(unittest.TestCase):
    """At-least-once delivery between Kinesis, the gate and the sinks."""

    def test_sink_failure_redelivers_as_new_not_as_duplicate(self):
        g = IngestGate()
        g.begin_batch()
        self.assertEqual(g.process_line(ev()).bucket, "accepted")
        g.abort_batch()  # ClickHouse acked, Firehose failed: the Lambda returns an error
        g = g.restart()
        g.begin_batch()
        self.assertEqual(g.process_line(ev()).bucket, "accepted")
        g.commit_batch()
        g.begin_batch()
        self.assertEqual(g.process_line(ev()).bucket, "duplicate")

    def test_duplicate_inside_one_batch_is_caught_before_commit(self):
        g = IngestGate()
        g.begin_batch()
        buckets = [g.process_line(ev()).bucket, g.process_line(ev(received_at="2026-06-15T14:00:07.000Z")).bucket]
        self.assertEqual(buckets, ["accepted", "duplicate"])

    def test_retry_after_dedupe_ttl_is_accepted_but_cannot_touch_state(self):
        g = IngestGate()
        g.process_line(ev())
        late = g.process_line(ev(received_at="2026-06-15T16:30:00.000Z"))
        self.assertEqual(late.bucket, "accepted")  # Valkey key expired; ClickHouse collapses it on (tenant_id, event_id)
        self.assertIn("no_state_update", late.event["flags"])
        self.assertIn("no_trigger", late.event["flags"])

    def test_redelivered_batch_does_not_inflate_the_bot_window(self):
        g = IngestGate()
        views = [ev(event_id=f"v{i}", received_at=f"2026-06-15T14:00:00.{150 + 100 * i}Z") for i in range(3)]
        g.begin_batch()
        [g.process_line(v) for v in views]
        g.abort_batch()
        g.begin_batch()
        flags = [f for v in views for f in g.process_line(v).event["flags"]]
        self.assertNotIn("bot_rate", flags)


class PrivacyTests(unittest.TestCase):
    def test_quarantined_copy_is_redacted_too(self):
        o = IngestGate().process_line(ev(tenant_id=None, properties={"contact_email": "x@y.io"}))
        self.assertEqual(o.reason, "tenant_unresolved")
        self.assertNotIn("x@y.io", json.dumps(o.event))

    def test_pii_inside_arrays(self):
        o = IngestGate().process_line(ev(properties={"path": "/", "cc": ["a@b.io", "c@d.io"]}))
        self.assertNotIn("@", json.dumps(o.event["properties"]))

    def test_missing_received_at_partitions_on_kinesis_arrival(self):
        line = FIXTURE.read_text().splitlines()[9]  # evt-0009, legacy, no received_at
        live = IngestGate().process_line(line, arrival_ts="2026-06-16T00:00:01.000Z")
        offline = IngestGate().process_line(line)
        self.assertEqual(live.event["partition_date"], "2026-06-16")
        self.assertNotIn("partition_time_unverified", live.event["flags"])
        self.assertIn("partition_time_unverified", offline.event["flags"])

    def test_kinesis_arrival_stands_in_for_a_missing_collector_stamp(self):
        line = FIXTURE.read_text().splitlines()[9]  # evt-0009, sent 14:08:02 by the legacy SDK
        e = IngestGate().process_line(line, arrival_ts="2026-06-15T14:08:02.180Z").event
        self.assertEqual((e["received_at"][:23], e["event_time_source"]), ("2026-06-15T14:08:02.180", "client"))
        self.assertIn("received_at_from_kinesis", e["flags"])
        self.assertFalse({"no_state_update", "no_trigger"} & set(e["flags"]))  # legacy traffic moves counters

    def test_clickhouse_row_carries_the_columns_the_rollup_reads(self):
        g = IngestGate()
        outs = [g.process_line(line) for line in FIXTURE.read_text().splitlines()]
        rows = {o.event["event_id"]: clickhouse_row(o.event) for o in outs if o.bucket == "accepted"}
        self.assertEqual((rows["evt-0001"]["path"], rows["evt-0001"]["is_bot"]), ("/pricing", False))
        self.assertEqual((rows["evt-0009"]["type"], rows["evt-0009"]["path"], rows["evt-0009"]["referrer"]),
                         ("page_view", "/pricing", "https://bing.com"))
        self.assertTrue(all(rows[f"evt-00{n}"]["is_bot"] for n in (12, 13, 14, 15)))
        self.assertEqual(rows["evt-0004"]["referrer"], "")  # "referrer": null
        for r in rows.values():
            self.assertIsInstance(r["properties"], str)
            self.assertFalse(any(isinstance(v, (dict, list)) for k, v in r.items() if k != "flags"), r["event_id"])
        pricing = sorted(r["event_id"] for r in rows.values()
                         if r["tenant_id"] == "t-042" and r["type"] == "page_view" and r["path"] == "/pricing"
                         and not r["is_bot"])
        self.assertEqual(pricing, ["evt-0001", "evt-0009"])  # what page_views_1m_mv would count


if __name__ == "__main__":
    unittest.main()
