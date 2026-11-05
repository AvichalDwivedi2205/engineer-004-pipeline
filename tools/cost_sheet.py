#!/usr/bin/env python3
"""Sizing and monthly cost for the proposed pipeline. Every input has a label.

    python3 tools/cost_sheet.py            # prints the table, writes out/cost_sheet.md

Prices: AWS Price List API and clickhouse.com/pricing, us-east-1, retrieved 2026-10-29
(sku-level detail in evidence/prices_2026-10-29.md). Everything else is labelled
[Assumed] or [Estimated]; change an input and the table follows.

Not included: the legacy system's own bill during the parallel run (unknown to me,
see ANSWER.md), staff time, Support plans, taxes, reserved/savings-plan discounts,
cross-AZ transfer between our own services, and ClickHouse backup storage beyond the
default (one daily backup kept 24 h). The 20% reserve line is meant to absorb those.
"""
from __future__ import annotations

from pathlib import Path

H = 730  # hours per month

I = {  # value, label, note
    "events_per_day":        (50e6,   "Assumed", "brief figure"),
    "peak_to_avg":           (3.0,    "Assumed", "diurnal peak hour vs daily average"),
    "spike_x":               (10.0,   "Assumed", "brief's 10x, applied on top of the normal peak (conservative reading)"),
    "bytes_per_event":       (1024,   "Assumed", "fixture mean is 230 B [Observed]; production events carry UA/URL/context"),
    "spike_hours_bf_month":  (72,     "Assumed", "Black Friday to Cyber Monday at full spike"),
    "collector_rps_per_vcpu": (2000,  "Assumed", "Node/Go collector, JSON parse + PutRecords; Phase 0 benchmark replaces this"),
    "gate_ms_per_record":    (0.2,    "Estimated", "gate CPU is ~0.021 ms/record (~47K records/s in the committed run, varies by run) on one Apple M4 core [Observed, out/drill_clean.json]; x3 for a Lambda vCPU, the rest is Redis/ClickHouse I/O"),
    "ch_units_normal":       (8,      "Assumed", "2 replicas x 32 GiB; Phase 1 load test replaces this"),
    "ch_units_spike":        (24,     "Assumed", "2 replicas x 96 GiB during spike hours"),
    "ch_hot_days":           (90,     "Assumed", "hot retention in ClickHouse; lake keeps 13 months"),
    "compression_x":         (8.0,    "Assumed", "columnar compression of event rows"),
    "valkey_nodes":          (4,      "Estimated", "2 shards x (primary+replica) r7g.xlarge: ~9 GB visitor state + up to ~6 GB dedupe keys at spike"),
    "dsr_athena_tb_month":   (20,     "Estimated", "daily batched Iceberg deletes, each scanning the affected tenants' partitions"),
    "cw_logs_gb_month":      (300,    "Assumed", "sampled request logs + Lambda logs"),
    "cw_metrics":            (150,    "Assumed", "per-tenant counters go to ClickHouse, not CloudWatch"),
    "rds_storage_gb":        (100,    "Assumed", "identity edges, DSR tickets, tombstones, audit rows"),
    "export_share":          (0.30,   "Assumed", "share of event volume that tenants export to a warehouse"),
    "export_bigquery_share": (0.30,   "Assumed", "of exports, the share BigQuery pulls over the internet; Snowflake stages read S3 in-region"),
    "shard_headroom":        (1.25,   "Assumed", "Kinesis shards provisioned above the computed record rate"),
    "staging_fraction":      (0.25,   "Assumed", "staging/dev environment as a fraction of prod"),
    "reserve_fraction":      (0.20,   "Assumed", "unplanned items and estimate error"),
}
P = {  # [Benchmarked] list prices, us-east-1, 2026-10-29
    "fargate_arm_vcpu_h": 0.03238, "fargate_arm_gb_h": 0.00356,
    "alb_h": 0.0225, "alb_lcu_h": 0.008,
    "kds_shard_h": 0.015, "kds_ext_retention_shard_h": 0.020, "kds_put_unit_per_m": 0.014,
    "kds_od_stream_h": 0.04, "kds_od_in_gb": 0.08, "kds_od_out_gb": 0.04, "kds_od_ext_retention_gb_month": 0.10,
    "lambda_arm_gb_s": 0.0000133334, "lambda_req_per_m": 0.20,
    "valkey_r7g_xl_h": 0.3496,
    "ch_scale_unit_h": 0.2985, "ch_storage_tb_month": 25.30,
    "privatelink_endpoint_az_h": 0.01, "privatelink_gb": 0.01,
    "firehose_s3_batch_gb": 0.029, "firehose_iceberg_gb": 0.075,
    "s3_gb_month": 0.023, "athena_tb": 5.00,
    "rds_pg_r7g_large_maz_h": 0.478, "rds_gp3_maz_gb_month": 0.23, "egress_internet_gb": 0.09,
    "ec2_r7g_2xl_h": 0.4284, "ec2_m7g_xl_h": 0.1632, "gp3_gb_month": 0.08,
    "cw_logs_gb": 0.50, "cw_metric_month": 0.30,
}


def v(k):
    return I[k][0]


def scenario(name, spike_hours, staging_base=None):
    eps_avg = v("events_per_day") / 86400
    eps_peak = eps_avg * v("peak_to_avg")
    eps_spike = eps_peak * v("spike_x")
    normal_hours = H - spike_hours
    # Monthly volume: spike hours run at spike rate instead of the normal mix.
    events_month = v("events_per_day") * 30 + (eps_spike - eps_avg) * 3600 * spike_hours
    gb_month = events_month * v("bytes_per_event") / 1e9
    normal_day_gb = v("events_per_day") * v("bytes_per_event") / 1e9

    def retained_gb(days, compression):
        # A retention window holds this month's volume plus earlier days at the normal rate.
        return (gb_month + normal_day_gb * max(days - 30, 0)) / compression

    c = {}
    # Collector: ALB + Fargate ARM tasks (1 vCPU / 2 GB), N+1 across 3 AZs, min 3.
    def tasks(eps):
        return max(3, int(eps / v("collector_rps_per_vcpu")) + 2)
    task_h = 0.03238 * 1 + 0.00356 * 2
    c["collector (ALB + Fargate)"] = (tasks(eps_peak) * normal_hours + tasks(eps_spike) * spike_hours) * task_h \
        + P["alb_h"] * H + P["alb_lcu_h"] * (eps_avg * 0.5 / 25) * H  # LCU: half of requests open a new connection [Assumed]
    # Kinesis provisioned, sized for the full spike all month: 1,000 records/s/shard write limit [Benchmarked].
    shards = int(eps_spike * v("shard_headroom") / 1000) + 1
    c[f"Kinesis ({shards} shards, 7-day retention)"] = shards * (P["kds_shard_h"] + P["kds_ext_retention_shard_h"]) * H \
        + events_month / 1e6 * P["kds_put_unit_per_m"]
    # Lambda gate: compute plus a 50 ms floor for each 1/s poll per shard.
    gb_s = events_month * v("gate_ms_per_record") / 1000 * 1.0 + shards * 3600 * H * 0.05 * 1.0
    invocations = shards * 3600 * H
    c["Lambda ingest gate"] = gb_s * P["lambda_arm_gb_s"] + invocations / 1e6 * P["lambda_req_per_m"]
    c["Valkey (visitor state + dedupe)"] = v("valkey_nodes") * P["valkey_r7g_xl_h"] * H
    ch_tb = retained_gb(v("ch_hot_days"), v("compression_x")) / 1000
    c["ClickHouse Cloud Scale"] = (v("ch_units_normal") * normal_hours + v("ch_units_spike") * spike_hours) \
        * P["ch_scale_unit_h"] + ch_tb * P["ch_storage_tb_month"]
    c["PrivateLink to ClickHouse"] = 3 * P["privatelink_endpoint_az_h"] * H + gb_month * P["privatelink_gb"]
    # Raw archive: Direct PUT to an S3 destination, one newline-joined record per Kinesis batch, so the
    # 5 KB-per-record rounding is negligible. Lake: Direct PUT to Iceberg, which takes exactly one JSON
    # object per record (several in one record go to the error prefix) and bills bytes without rounding.
    c["Firehose raw archive (batched, to S3)"] = gb_month * P["firehose_s3_batch_gb"]
    c["Firehose lake (one event per record, Iceberg)"] = gb_month * P["firehose_iceberg_gb"]
    lake_gb = retained_gb(395, v("compression_x"))
    raw_gb = retained_gb(30, 4)  # gzip JSON, 30-day lifecycle
    c["S3 (13-mo lake + 30-day raw)"] = (lake_gb + raw_gb) * P["s3_gb_month"]
    # Interface endpoints for Kinesis (collectors) and Firehose (gate); bytes through both.
    c["VPC endpoints (Kinesis, Firehose)"] = 2 * 3 * P["privatelink_endpoint_az_h"] * H \
        + 3 * gb_month * P["privatelink_gb"]
    c["Athena (exports + DSR deletes)"] = v("dsr_athena_tb_month") * P["athena_tb"]
    c["RDS Postgres (control plane, Multi-AZ)"] = P["rds_pg_r7g_large_maz_h"] * H \
        + v("rds_storage_gb") * P["rds_gp3_maz_gb_month"]
    # Exports are compressed Parquet; only BigQuery's pull from S3 leaves the region.
    c["export egress (BigQuery pulls)"] = gb_month / v("compression_x") * v("export_share") \
        * v("export_bigquery_share") * P["egress_internet_gb"]
    c["CloudWatch"] = v("cw_logs_gb_month") * P["cw_logs_gb"] + v("cw_metrics") * P["cw_metric_month"]
    base = sum(c.values())
    # Staging carries test traffic, not production spikes: priced off the normal month in every scenario.
    c["staging/dev"] = (staging_base if staging_base is not None else base) * v("staging_fraction")
    c["reserve"] = (base + c["staging/dev"]) * v("reserve_fraction")
    # Not chosen: self-managed ClickHouse, 2 x r7g.2xlarge replicas + 3 x m7g.xlarge Keeper + 1 TB gp3 each.
    self_hosted_ch = (2 * P["ec2_r7g_2xl_h"] + 3 * P["ec2_m7g_xl_h"]) * H + 2 * 1000 * P["gp3_gb_month"]
    # Not chosen: Kinesis on-demand for the same bytes. One GetRecords consumer (the gate's Lambda);
    # retention past 24 h is billed per GB-month, each GB held 6 extra days.
    kinesis_on_demand = P["kds_od_stream_h"] * H + gb_month * (P["kds_od_in_gb"] + P["kds_od_out_gb"]) \
        + gb_month * 6 * 24 / H * P["kds_od_ext_retention_gb_month"]
    # Spike headroom: shards for 10x of peak vs shards for normal peak and for 10x of the daily average.
    def shards_for(eps):
        return int(eps * v("shard_headroom") / 1000) + 1
    shard_month = (P["kds_shard_h"] + P["kds_ext_retention_shard_h"]) * H
    headroom = {"normal_peak_shards": shards_for(eps_peak), "ten_x_avg_shards": shards_for(eps_avg * v("spike_x")),
                "extra_vs_normal_peak": (shards - shards_for(eps_peak)) * shard_month,
                "extra_vs_10x_avg": (shards - shards_for(eps_avg * v("spike_x"))) * shard_month}
    return {"self_hosted_ch": self_hosted_ch, "kinesis_on_demand": kinesis_on_demand, "headroom": headroom, "name": name, "eps": (eps_avg, eps_peak, eps_spike),
            "shards": shards, "base": base, "events_month": events_month, "gb_month": gb_month, "lines": c, "total": sum(c.values())}


def render(scs):
    out = ["# Cost sheet (USD / month, us-east-1 list prices 2026-10-29)", ""]
    a, p, s = scs[0]["eps"]
    out.append(f"Throughput [Estimated]: avg {a:,.0f} ev/s, normal peak {p:,.0f} ev/s, 10x spike {s:,.0f} ev/s. "
               f"Kinesis sized for the spike permanently: {scs[0]['shards']} shards.")
    out += ["", "| line | " + " | ".join(x["name"] for x in scs) + " |", "|---|" + "---:|" * len(scs)]
    for k in scs[0]["lines"]:
        out.append(f"| {k} | " + " | ".join(f"{x['lines'][k]:,.0f}" for x in scs) + " |")
    out.append("| **total** | " + " | ".join(f"**{x['total']:,.0f}**" for x in scs) + " |")
    out.append("| headroom vs $50,000 | " + " | ".join(f"{50000 - x['total']:,.0f}" for x in scs) + " |")
    out.append("| events this month (vs normal) | " + " | ".join(
        f"{x['events_month'] / 1e9:,.2f}B ({x['events_month'] / scs[0]['events_month']:.1f}x)" for x in scs) + " |")
    out.append("| (not chosen) self-managed ClickHouse on EC2, infra only | " + " | ".join(
        f"{x['self_hosted_ch']:,.0f}" for x in scs) + " |")
    kds = next(k for k in scs[0]["lines"] if k.startswith("Kinesis"))
    out.append("| (not chosen) Kinesis on-demand, same bytes, in place of the provisioned line | " + " | ".join(
        f"{x['kinesis_on_demand']:,.0f} (vs {x['lines'][kds]:,.0f})" for x in scs) + " |")
    hd = scs[0]["headroom"]
    out += ["", f"Spike headroom [Estimated]: normal peak needs {hd['normal_peak_shards']} shards; 10x of the daily "
            f"average needs {hd['ten_x_avg_shards']}. Holding {scs[0]['shards']} costs ${hd['extra_vs_normal_peak']:,.0f}/mo "
            f"more than normal-peak sizing and ${hd['extra_vs_10x_avg']:,.0f}/mo more than the 10x-average reading."]
    out += ["", "## Inputs", "", "| input | value | label | note |", "|---|---:|---|---|"]
    for k, (val, label, note) in I.items():
        out.append(f"| {k} | {val:,} | [{label}] | {note} |")
    out += ["", "Prices: [Benchmarked] evidence/prices_2026-10-29.md (AWS Price List API skus, clickhouse.com/pricing)."]
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    normal = scenario("normal month", 0)
    scs = [normal, scenario("Black Friday month (72 h at spike)", v("spike_hours_bf_month"), normal["base"]),
           scenario("stress: all 730 h at spike rate", H, normal["base"])]
    text = render(scs)
    Path(__file__).resolve().parent.parent.joinpath("out", "cost_sheet.md").write_text(text)
    print(text)
    assert all(x["total"] < 50_000 for x in scs), "over the $50K ceiling"
