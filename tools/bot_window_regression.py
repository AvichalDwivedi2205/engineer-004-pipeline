#!/usr/bin/env python3
"""Reproduce the bot-window bug the spike drill found: first rule vs current rule, same inputs.

    python3 tools/bot_window_regression.py --events 200000 --seed 7

The first version of the rate rule windowed page views on client time. One event whose
client clock ran a year ahead entered a visitor's window and never aged out, so that
human's next page views tripped the 4-in-1-second rule. Both gates below see the identical
stream from spike_drill.generate(); only _bot_check differs (evidence/bot_window_fix.diff).
Writes out/bot_window_regression.json.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from collections import defaultdict, deque
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))
from ingest_gate.gate import (BOT_BURST_COUNT, BOT_BURST_WINDOW_MS, BOT_HOST_TOKENS,  # noqa: E402
                              IngestGate, host_tokens, ms_between)
from spike_drill import generate, iso  # noqa: E402


class ClientTimeWindowGate(IngestGate):
    """_bot_check as first written, before the fix."""

    def __init__(self):
        super().__init__()
        self._client_windows = defaultdict(deque)

    def _bot_check(self, ev, client, flags):
        visitor = (ev["tenant_id"], ev.get("anonymous_id"))
        if BOT_HOST_TOKENS & host_tokens(ev["properties"].get("referrer")):
            flags.append("bot_referrer")
            self.bot_visitors.add(visitor)
        if ev["type"] == "page_view":
            window = self._client_windows[visitor]
            window.append(client)
            while window and ms_between(client, window[0]) > BOT_BURST_WINDOW_MS:
                window.popleft()
            if len(window) >= BOT_BURST_COUNT:
                flags.append("bot_rate")
                self.bot_visitors.add(visitor)
        if visitor in self.bot_visitors and "bot_referrer" not in flags and "bot_rate" not in flags:
            flags.append("bot_visitor")


BOT_FLAGS = {"bot_referrer", "bot_rate", "bot_visitor"}


def count(gate, deliveries):
    clean, false_bot, true_bot, bots = 0, 0, 0, 0
    for recv, raw, tags, _ in deliveries:
        o = gate.process_line(raw, server_received_at=iso(recv))
        if o.bucket != "accepted":
            continue
        flagged = bool(BOT_FLAGS & set(o.event["flags"]))
        if not tags:
            clean += 1
            false_bot += flagged
        if "bot" in tags:
            bots += 1
            true_bot += flagged
    return {"clean_events": clean, "clean_events_flagged_as_bot": false_bot,
            "bot_events": bots, "bot_events_flagged_in_stream": true_bot}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=200_000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    deliveries, _ = generate(args.events, random.Random(args.seed))
    result = {"seed": args.seed, "events": args.events,
              "client_time_window_first_version": count(ClientTimeWindowGate(), deliveries),
              "server_time_window_current": count(IngestGate(), deliveries)}
    (ROOT / "out" / "bot_window_regression.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    return 0 if result["server_time_window_current"]["clean_events_flagged_as_bot"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
