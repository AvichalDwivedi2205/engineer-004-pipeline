# If the constraints change

These are answers to four stress questions GPT-6.1 raised in round 2 (`consult/raw/gpt61_round2.md`). Every figure is [Estimated], computed from the prices in `tools/cost_sheet.py` unless a line says otherwise. They didn't fit in the 4-page answer, so they live here.

## $10K a month in total

A normal month ($6,913) fits, and so does a Black Friday month ($8,275). That leaves $1.7-3.1K for the legacy system while both run in parallel.

If legacy costs more than that:
- staging moves to on-demand capacity;
- the parallel run gets shorter.

A sustained month at 30x volume ($20,719) does not fit.

## Everything inside our own AWS account

ClickHouse moves to EC2, at $1,143/mo for infrastructure on the sheet, or to BYOC. Running it adds roughly a quarter of an engineer's time [Assumed].

Snowflake and BigQuery exports leave the account by definition. They need an explicit exception, or tenants pull Parquet from an S3 bucket in our account.

## EU-only residency for EU tenants

Run a second cell in eu-west-1. The collectors route EU tenants to that cell's Kinesis stream. No EU record is written to US storage, and that includes the S3 spill.

GeoDNS alone isn't enough, because a visitor's location doesn't decide residency; the tenant does.

The cell's fixed lines come to about $3.7K/mo before regional price differences:
- Valkey
- ClickHouse at 8 units
- RDS
- collectors
- CloudWatch
- VPC endpoints
- 4 Kinesis shards

## Deletion within 72 hours, backups included

- **Kinesis:** retention drops from 7 days to 24 h, saving $321/mo.
- **Deletion jobs:** every deletion job in the ANSWER.md §5 table runs daily.
- **RDS:** backup retention drops to 1 day.
- **ClickHouse Cloud:** backups already expire after 24 h by default [Benchmarked] (ClickHouse docs, quoted in `evidence/prices_2026-10-29.md`).
- **Extra Athena and rewrite work:** not priced [Assumed].

What we give up is the 7-day replay window.
