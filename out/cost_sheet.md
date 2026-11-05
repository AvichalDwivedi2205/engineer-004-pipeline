# Cost sheet (USD / month, us-east-1 list prices 2026-10-29)

Throughput [Estimated]: avg 579 ev/s, normal peak 1,736 ev/s, 10x spike 17,361 ev/s. Kinesis sized for the spike permanently: 22 shards.

| line | normal month | Black Friday month (72 h at spike) | stress: all 730 h at spike rate |
|---|---:|---:|---:|
| collector (ALB + Fargate) | 171 | 190 | 372 |
| Kinesis (22 shards, 7-day retention) | 583 | 644 | 1,201 |
| Lambda ingest gate | 54 | 66 | 172 |
| Valkey (visitor state + dedupe) | 1,021 | 1,021 | 1,021 |
| ClickHouse Cloud Scale | 1,758 | 2,116 | 5,387 |
| PrivateLink to ClickHouse | 37 | 82 | 489 |
| Firehose raw archive (batched, to S3) | 45 | 174 | 1,354 |
| Firehose lake (one event per record, Iceberg) | 115 | 449 | 3,502 |
| S3 (13-mo lake + 30-day raw) | 67 | 105 | 457 |
| VPC endpoints (Kinesis, Firehose) | 90 | 224 | 1,445 |
| Athena (exports + DSR deletes) | 100 | 100 | 100 |
| RDS Postgres (control plane, Multi-AZ) | 372 | 372 | 372 |
| export egress (BigQuery pulls) | 2 | 6 | 47 |
| CloudWatch | 195 | 195 | 195 |
| staging/dev | 1,152 | 1,152 | 1,152 |
| reserve | 1,152 | 1,379 | 3,453 |
| **total** | **6,913** | **8,275** | **20,719** |
| headroom vs $50,000 | 43,087 | 41,725 | 29,281 |
| events this month (vs normal) | 1.50B (1.0x) | 5.85B (3.9x) | 45.60B (30.4x) |
| (not chosen) self-managed ClickHouse on EC2, infra only | 1,143 | 1,143 | 1,143 |
| (not chosen) Kinesis on-demand, same bytes, in place of the provisioned line | 244 (vs 583) | 866 (vs 644) | 6,554 (vs 1,201) |

Spike headroom [Estimated]: normal peak needs 3 shards; 10x of the daily average needs 8. Holding 22 costs $485/mo more than normal-peak sizing and $358/mo more than the 10x-average reading.

## Inputs

| input | value | label | note |
|---|---:|---|---|
| events_per_day | 50,000,000.0 | [Assumed] | brief figure |
| peak_to_avg | 3.0 | [Assumed] | diurnal peak hour vs daily average |
| spike_x | 10.0 | [Assumed] | brief's 10x, applied on top of the normal peak (conservative reading) |
| bytes_per_event | 1,024 | [Assumed] | fixture mean is 230 B [Observed]; production events carry UA/URL/context |
| spike_hours_bf_month | 72 | [Assumed] | Black Friday to Cyber Monday at full spike |
| collector_rps_per_vcpu | 2,000 | [Assumed] | Node/Go collector, JSON parse + PutRecords; Phase 0 benchmark replaces this |
| gate_ms_per_record | 0.2 | [Estimated] | gate CPU is ~0.020 ms/record (~50K records/s in the committed run, varies by run) on one Apple M4 core [Observed, out/drill_clean.json]; x3 for a Lambda vCPU, the rest is Redis/ClickHouse I/O |
| ch_units_normal | 8 | [Assumed] | 2 replicas x 32 GiB; Phase 1 load test replaces this |
| ch_units_spike | 24 | [Assumed] | 2 replicas x 96 GiB during spike hours |
| ch_hot_days | 90 | [Assumed] | hot retention in ClickHouse; lake keeps 13 months |
| compression_x | 8.0 | [Assumed] | columnar compression of event rows |
| valkey_nodes | 4 | [Estimated] | 2 shards x (primary+replica) r7g.xlarge: ~9 GB visitor state + up to ~6 GB dedupe keys at spike |
| dsr_athena_tb_month | 20 | [Estimated] | daily batched Iceberg deletes, each scanning the affected tenants' partitions |
| cw_logs_gb_month | 300 | [Assumed] | sampled request logs + Lambda logs |
| cw_metrics | 150 | [Assumed] | per-tenant counters go to ClickHouse, not CloudWatch |
| rds_storage_gb | 100 | [Assumed] | identity edges, DSR tickets, tombstones, audit rows |
| export_share | 0.3 | [Assumed] | share of event volume that tenants export to a warehouse |
| export_bigquery_share | 0.3 | [Assumed] | of exports, the share BigQuery pulls over the internet; Snowflake stages read S3 in-region |
| shard_headroom | 1.25 | [Assumed] | Kinesis shards provisioned above the computed record rate |
| staging_fraction | 0.25 | [Assumed] | staging/dev environment as a fraction of prod |
| reserve_fraction | 0.2 | [Assumed] | unplanned items and estimate error |

Prices: [Benchmarked] evidence/prices_2026-10-29.md (AWS Price List API skus, clickhouse.com/pricing).
