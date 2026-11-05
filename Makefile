# Everything here is stdlib Python 3.10+; no installs.
PY ?= python3 -I

all: checksum test audit drill bugfix cutover cost ddl

checksum:
	shasum -a 256 fixtures/event_sample.jsonl

test:
	$(PY) -m unittest discover -s tests -v

audit:
	$(PY) tools/fixture_audit.py fixtures/event_sample.jsonl

drill:  # the before-sink run is expected to fail its checks: it is the first design, kept as the counterexample
	$(PY) tools/spike_drill.py --events 1000000 --seed 11 --tag clean > out/drill_clean.stdout
	$(PY) tools/spike_drill.py --events 1000000 --seed 11 --tag loss --inject-loss 0.0002 > out/drill_loss.stdout
	-$(PY) tools/spike_drill.py --events 1000000 --seed 11 --tag dedupe_before_sink --dedupe-commit before-sink > out/drill_dedupe_before_sink.stdout
	rm -f out/counts_dedupe_before_sink.csv

bugfix:
	$(PY) tools/bot_window_regression.py --events 200000 --seed 7 > /dev/null && cat out/bot_window_regression.json

cutover:  # clean run: HOLD, because the drill cannot measure A4 freshness; loss run: ROLLBACK
	-$(PY) tools/cutover_gate.py out/counts_clean.csv | tee out/cutover_clean.txt
	-$(PY) tools/cutover_gate.py out/counts_loss.csv | tee out/cutover_loss.txt

cost:
	$(PY) tools/cost_sheet.py > /dev/null && cat out/cost_sheet.md

ddl:  # needs a local clickhouse binary; prints a skip message otherwise
	sh tools/ddl_check.sh

.PHONY: all checksum test audit drill bugfix cutover cost ddl
