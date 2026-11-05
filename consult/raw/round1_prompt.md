You are a senior data-infrastructure engineer acting as an independent design reviewer.
Files in the working directory: brief.md (the task), scoring_rubric.md and SCORING.md (how it is judged),
fixtures/event_sample.jsonl (25-line synthetic sample with deliberately planted issues).

Rules:
- Work only from these files and your own knowledge. Do NOT search for, open, or reuse other people's
  submissions to this challenge (there are public ones on GitHub; using them is plagiarism).
- Read the fixture line by line yourself. Parse it programmatically if you can run code.
- Be concrete and opinionated. No generic tutorial prose. Label every number as
  [Observed] (from the fixture), [Benchmarked] (named public source), [Estimated] (your reasoning shown), or [Assumed].
- Max ~1800 words.

Produce exactly these sections:

1. FIXTURE ANOMALIES - every anomaly class you find, with the exact event_ids, what is wrong, and how an
   ingest pipeline should handle it (drop / quarantine / flag / normalize / route elsewhere). Then list what
   you would NOT take at face value and why, and what you refuse to conclude because n=25.
2. ARCHITECTURE - one component per line: SDK edge/collector, durable buffer, stream processing,
   real-time store for dashboards, personalization/segment state, lake/warehouse export, identity stitching,
   GDPR deletion. For each: your pick, the strongest rejected alternative, and the deciding reason tied to
   <5s latency, 50M events/day, 10x spikes, $50K/month cap, a 12-person Python/Node/Postgres/Redis team
   with only 2 dedicated senior engineers, AWS-only, no SDK changes, 500+ tenants, SOC2/GDPR/CCPA.
3. SIZING + COST - back-of-envelope throughput, storage, and monthly cost (normal and Black-Friday 10x),
   inputs labeled.
4. DELIVERY GUARANTEES + FAILURE DESIGN - exact guarantee per hop, where backpressure/buffering lives,
   what degrades first under overload, how loss and duplication are detected (not assumed away).
5. MIGRATION - phased cutover with no SDK change and no broken integrations, parallel-run comparison,
   explicit rollback triggers, and a testable definition of "data accuracy verified".
6. THE 3 DECISIONS MOST LIKELY TO BE WRONG in your own design, and what evidence would change your mind.
