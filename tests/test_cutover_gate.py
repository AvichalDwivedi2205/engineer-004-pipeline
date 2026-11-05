"""Verdict logic of tools/cutover_gate.py on small hand-made count files, including the
counterexamples that passed the first, global-sum version of the gate."""
import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

GATE = Path(__file__).resolve().parent.parent / "tools" / "cutover_gate.py"
COLS = ["tenant_id", "hour", "edge", "new_accepted", "new_duplicate", "new_quarantined", "new_dead_letter",
        "new_control", "new_suppressed", "legacy_landed"]
OK_ROW = ["t-1", 0, 2000, 1980, 20, 0, 0, 0, 0, 1950]
FRESH = ["--canary-p95-ms", "2400"]


def verdict(rows, *extra):
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="") as fh:
        w = csv.writer(fh)
        w.writerow(COLS)
        w.writerows(rows)
    return subprocess.run([sys.executable, "-I", str(GATE), fh.name, *extra], capture_output=True, text=True).returncode


def approvals(*tenants):
    fh = tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False)
    fh.write("".join(f"{t}\treviewer\tsampled quarantine, SDK v1 payloads\n" for t in tenants))
    fh.close()
    return fh.name


class CutoverGateTests(unittest.TestCase):
    def test_fully_accounted_tenant_with_measured_freshness_passes(self):
        self.assertEqual(verdict([OK_ROW], *FRESH), 0)

    def test_unmeasured_freshness_holds(self):
        self.assertEqual(verdict([OK_ROW]), 3)

    def test_slow_freshness_holds(self):
        self.assertEqual(verdict([OK_ROW], "--canary-p95-ms", "6100"), 3)

    def test_one_missing_record_forces_rollback(self):
        self.assertEqual(verdict([["t-1", 0, 2000, 1979, 20, 0, 0, 0, 0, 1950]], *FRESH), 2)

    def test_missing_record_cannot_be_offset_by_another_tenants_surplus(self):
        rows = [["t-1", 0, 2000, 1979, 20, 0, 0, 0, 0, 1950], ["t-2", 0, 2000, 1981, 20, 0, 0, 0, 0, 1950]]
        self.assertEqual(verdict(rows, *FRESH), 2)

    def test_high_quarantine_rate_holds_for_human_review(self):
        # 2% quarantined: probably an SDK payload variant the gate does not know yet
        self.assertEqual(verdict([["t-1", 0, 2000, 1940, 20, 40, 0, 0, 0, 1950]], *FRESH), 3)

    def test_small_tenant_fully_quarantined_does_not_slip_through(self):
        rows = [OK_ROW, ["t-small", 0, 40, 0, 0, 40, 0, 0, 0, 40]]
        self.assertEqual(verdict(rows, *FRESH), 3)

    def test_small_tenant_with_a_bad_record_passes_only_after_recorded_review(self):
        rows = [OK_ROW, ["t-small", 0, 300, 299, 0, 1, 0, 0, 0, 295]]
        self.assertEqual(verdict(rows, *FRESH), 3)
        self.assertEqual(verdict(rows, *FRESH, "--approved", approvals("t-small")), 0)

    def test_legacy_rows_beyond_the_edge_count_mean_the_denominator_is_wrong(self):
        self.assertEqual(verdict([["t-1", 0, 2000, 1980, 20, 0, 0, 0, 0, 2010]], *FRESH), 3)

    def test_unresolved_records_must_still_be_accounted_for(self):
        rows = [OK_ROW, ["unresolved", 0, 12, 0, 0, 11, 0, 0, 0, 12]]
        self.assertEqual(verdict(rows, *FRESH), 2)
        rows[1] = ["unresolved", 0, 12, 0, 0, 12, 0, 0, 0, 12]
        self.assertEqual(verdict(rows, *FRESH), 0)


if __name__ == "__main__":
    unittest.main()
