## 1. FIXTURE ANOMALIES

Brief identifier: `2026-07`. Python inspected every line: [Observed] 25 lines, 24 valid JSON objects, 23 distinct `event_id`s among parsed objects. Fixture SHA-256 [Observed]:

`1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`

| [Observed] anomaly / exact event_ids | Pipeline handling |
|---|---|
| Duplicate: `evt-0002`; only `received_at` differs. Its file position also violates receipt-time ordering. | **Drop duplicate analytical effect**, retain both receipts. Dedup using authenticated tenant/event ID and payload hash excluding receipt metadata; quarantine conflicting payloads. |
| Future timestamps: `evt-0005`, `evt-0006`, `evt-0016`; receipt precedes claimed event time by [Observed] 47.190s, 3,900s, 31,535,999.788s respectively. | **Flag and normalize** live processing to trusted collector time; preserve original timestamps. Client clocks cannot advance watermarks. |
| Schema drift: `evt-0009` uses `pageview`, `timestamp`, `page_path`, `ref`; lacks `ts` and `received_at`. | **Normalize** with a versioned adapter; stamp actual collector time. Historical replay must mark receipt time unknown. |
| Missing tenant: `evt-0011`. | Recover only from validated legacy credentials; otherwise **quarantine**. Never infer tenancy from neighboring records. |
| Malformed JSON: `evt-0020`, missing closing outer brace. | **Quarantine** exact bytes with authenticated envelope; never silently repair. |
| Contact PII: `evt-0007` contains email/phone. | **Redact** analytical properties and logs; **route** necessary contact data to restricted storage. Deletion must follow its later identify association, `evt-0022`. |
| Suspected automation: `evt-0012`, `evt-0013`, `evt-0014`, `evt-0015`; scanner referrer and [Observed] four page views spanning 50ms of client time. | **Flag**, preserve, expose filtered dashboards. Suppress personalization only under an approved classification policy. |
| Privacy control: `evt-0017`. | **Route elsewhere**, into verified deletion workflow; exclude from behavioral metrics. Payload alone cannot authorize deletion. |
| Unverified derived metric: `evt-0019`, [Observed] `count_today=3`. | Preserve as a client claim; derive counts independently. Only `evt-0001` corroborates pricing for this identity here; incomplete history neither proves nor disproves the claim. |

Do **not** take bot-looking referrers, client counters, or timestamp differences as trustworthy behavioral or processing-latency measurements. Anonymous identifiers are tenant-scoped; `anon-77a` connects `evt-0006`/`evt-0017` within their tenant. Anonymous-only events and null referrers are valid.

With [Observed] n=25, refuse conclusions about production loss, bot prevalence, payload distribution, tenant skew, peak duration, or latency percentiles.

Runnable audit excerpt; the table additionally reflects semantic inspection:

```python
import json, hashlib, datetime as dt
from pathlib import Path

raw = Path("fixtures/event_sample.jsonl").read_bytes()
print(hashlib.sha256(raw).hexdigest())
seen = set()
for line in raw.splitlines():
    try:
        e = json.loads(line)
    except json.JSONDecodeError:
        print("malformed JSON")
        continue
    key = e.get("tenant_id"), e["event_id"]
    if key in seen: print("duplicate", key)
    seen.add(key)
    if not e.get("tenant_id"): print("missing tenant", key)
    if "ts" not in e or "received_at" not in e:
        print("schema drift", key)
        continue
    parse = dt.datetime.fromisoformat
    lag = (parse(e["received_at"]) - parse(e["ts"])).total_seconds()
    if lag < 0: print("future timestamp", key, lag)
```

## 2. ARCHITECTURE

[Assumed; brief requirements] 50M events/day, 10× spikes, 500+ tenants, $50K/month infrastructure cap; 12 engineers with two dedicated seniors; MVP/full delivery in three/six months. Target [Assumed] p99 <5s from collector receipt to refreshed dashboard/profile availability; offline browser delay is unbounded.

**SDK edge/collector:** Existing domain/contracts → ALB → ECS Node collectors; SDK unchanged. Authenticate tenancy independently of payload; stamp receipt time/schema. Reject API Gateway/Lambda: preserve legacy batching, headers and responses while avoiding per-request charges.

**Durable buffer:** Provisioned Kinesis → independent raw archiver and processing consumers. Reject MSK: replay/order without Kafka operations; preprovision peak capacity before sales.

**Stream processing:** Managed Flink/PyFlink; checkpointed dedup and rolling windows, immediate updates without waiting for window closure. Reject Lambda: behavioral windows otherwise require custom durable-state coordination. Late corrections update analytics without retroactively firing personalization.

**Real-time store for dashboards:** Managed OpenSearch; bounded dimensions/query windows, pooled tenant indices, deterministic document IDs/routing. Reject self-managed ClickHouse: its operational burden exceeds senior staffing; accept worse analytical efficiency.

**Personalization/segment state:** ElastiCache Redis, versioned profile snapshots derived by Flink; existing personalization API reads them. Reject DynamoDB: existing Redis expertise and cache access fit better; Redis remains rebuildable.

**Lake/warehouse export:** Restricted raw S3 plus sanitized Iceberg; AWS-hosted queued exporters preserve Snowflake/BigQuery contracts. Reject direct warehouse writes: destination outages must not block ingestion.

**Identity stitching:** RDS PostgreSQL tenant-scoped alias edges, Redis cache; process `evt-0003`, `evt-0008`, `evt-0022`. Reject Neptune: arbitrary graph traversal is unnecessary. Client claims support provisional browser history; cross-device merges require authenticated corroboration. Review conflicts.

**GDPR deletion:** Verified workflow → durable tombstone → purge serving stores, rewrite lake/quarantine, expire snapshots/object versions, propagate export deletion manifests; suppress replay. Reject TTL-only deletion. [Assumed] online deletion within 24h; buffers/backups expire within seven days, subject to legal approval. Track destination acknowledgments.

Enforce tenant authorization on every read/write; private networking, TLS/KMS, least-privilege roles and access/deletion audits support SOC2/GDPR/CCPA. Deployment region follows residency contracts.

## 3. SIZING + COST

[Assumed] Average envelope 1KiB, month 30 days, lake compression 4:1. Fixture mean [Observed] 230 bytes/line is unsuitable for production sizing.

[Estimated] Throughput = 50M/86,400 ≈579 events/s normally; 5,787/s sustained peak; respectively 0.593/5.93 MB/s. [Assumed] Sixteen Kinesis shards, each supporting 1MB/s and 1,000 records/s, provide aggregate headroom; hot-key distribution still needs testing.

Normal/sustained-peak storage [Estimated], decimal units:

- Monthly incoming: 1.536/15.36TB.
- [Assumed] Seven-day buffer: 0.358/3.584TB.
- [Assumed] Three-day OpenSearch, indexing/replication multiplier 3: 0.461/4.608TB.
- [Assumed] Ninety-day compressed lake: 1.152/11.52TB.
- [Assumed] Daily dedup horizon, 96 bytes/key × overhead 3: 14.4/144GB checkpointed state.

Unverified rates are [Assumed]. Flink [Assumed] eight/thirty-two KPUs × $0.15/hour × 720 hours costs [Estimated] $0.864K/$3.456K before checkpoint/headroom allowances. OpenSearch [Assumed] six/eighteen nodes × $0.90/hour × 720 hours, plus storage/masters, yields approximately [Estimated] $5K/$15K.

| Service | [Estimated] normal / sustained peak, $K/month |
|---|---:|
| Kinesis | 0.7 / 1.3 |
| Flink/checkpoints | 1.8 / 5.5 |
| OpenSearch | 5 / 15 |
| Redis | 1.2 / 3 |
| PostgreSQL | 0.8 / 1.2 |
| S3/export/egress | 1 / 3 |
| Collectors/network/security/logging | 2.5 / 5 |
| Total | 13 / 34 |
| Including [Assumed] 25% reserve | 16.25 / 42.5 |

[Assumed] Export egress ≤1/10TB monthly; no payload logging; bounded queries from 100/500 concurrent dashboards. These are planning allowances, not quotes.

Peak cost funds sustained capacity all month. An isolated [Assumed] peak day raises monthly payload to [Estimated] 1.3×. Migration overlap requires the existing bill; headroom cannot be claimed without it.

## 4. DELIVERY GUARANTEES + FAILURE DESIGN

SDK→collector is **best effort** under existing retry behavior. Collector→Kinesis is **at least once**: acknowledge only after durable acceptance; retry individual partial failures. Timeouts can duplicate accepted records. Regional unavailability stops acknowledgments; unsent browser loss remains unobservable.

Kinesis→raw S3 is **at least once**, reconciled by stream coordinates/manifests. Kinesis→Flink provides checkpoint-consistent state; logical-event dedup is bounded by the stated horizon. OpenSearch/Redis sinks are **at least once**, using idempotent documents/versioned snapshots; writes are not atomic across stores. Older replays build isolated generations before swapping. Lake canonicalization merges by tenant/event ID; warehouse delivery uses manifests and destination merge keys.

[Assumed] Dashboard refresh/poll intervals each ≤1s. Verify visibility end to end; successful checkpoints do not establish sink freshness.

Backpressure accumulates in Kinesis; collector queues remain bounded. Degrade exports first, then expensive dashboard queries; display staleness and serve safe personalization defaults. Disable personalization during cache rebuild. Beyond capacity, reject before acknowledging; legacy SDK retries determine resulting loss.

Reconcile accepted receipts against raw manifests and canonical unique/duplicate/quarantine/control dispositions by tenant and interval. Compare ID/hash sets, not counts alone; alarm before backlog reaches retention. Poison records enter audited quarantine without blocking their shard.

## 5. MIGRATION

[Assumed] First month: compatible front door, durable log, raw capture and legacy bridge; both pipelines consume identical accepted input. Second month: shadow dashboards, basic segments, deletion and existing exports. MVP by third month; richer segments/export automation by sixth. Exclude probabilistic identity merging and arbitrary rule languages.

Assign the dedicated pair to ingestion/state and serving/migration respectively. [Assumed] Twelve/twenty-four working weeks provide [Estimated] 24/48 engineer-weeks.

Keep legacy APIs, credentials, payloads and integrations unchanged. Fence export ownership with durable cursors. Rollback switches dashboard/profile/export ownership to the caught-up legacy consumer; durable ingestion continues.

**Operating artifact—acceptance plan:** Replay the fixture with expected dispositions above; inject partial writes, accepted-write timeouts, processor restart, cache flush, poison records, destination outage, and deletion followed by replay. These system tests are proposed, not executed.

[Assumed] Shadow seven days including full peak replay; canary tenant cohorts at 1%, 10%, 50%, then 100%. **Accuracy verified** means every acknowledged receipt has a disposition; canonical ID/hash sets match the reference transformation; drained, closed-window integer aggregates match exactly; identity remains tenant-scoped; deleted subjects cannot reappear after replay. Document intentional normalization differences; legacy output is not ground truth.

Rollback for [Assumed] p99 >5s for five minutes, any unexplained missing accepted event, cross-tenant disclosure, failed deletion test, or monthly forecast >$50K. Privacy failures also disable the affected feature.

## 6. THE 3 DECISIONS MOST LIKELY TO BE WRONG

- **Flink fits the team.** Run an [Assumed] two-week peak/restart prototype against Lambda plus Redis with identical inputs/outputs. If Flink misses latency or operational feasibility, simplify MVP windows and choose Lambda.
- **OpenSearch fits dashboard economics.** Compare representative cardinalities/concurrency against ClickHouse under matched spend and operator time. Change if freshness/query targets fail or reserve disappears; retain S3 for backfill.
- **Sizing/retention assumptions fit reality.** Measure payload percentiles, active profiles, retry ages and egress; verify deletion obligations. Shorten hot retention and constrain exports before sacrificing acknowledged data. Immediate physical buffer erasure would require redesign.

Evidence log: fixture/checksum and executed Python assertions—**Logs or source records**; architecture, arithmetic and acceptance plan—**Demo artifact**; latency/reliability targets—**Claims only** pending load/fault tests.

AI disclosure: Codex authored this review; Python/json/hashlib checked fixture lines and sizing arithmetic. No production benchmark ran. Humans approve retention, disputed identity links, deletion authorization and cutover; no human validation is claimed.