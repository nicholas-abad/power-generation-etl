# Infrastructure Options

This document evaluates cloud hosting options for the power generation pipeline and recommends a deployment path.

## Current Architecture

Everything runs locally today. There is no cloud infrastructure.

```
Extractors (5 sources)
    |  JSONL files
    v
database_management.py (ETL loader)
    |
    v
PostgreSQL (local Docker)
    ^
    |
Streamlit Dashboard (local)
```

- **Extractors** output JSONL to local `output/` directories
- **ETL loader** (`database_management.py`) creates tables, loads JSONL, and manages materialized views
- **Dashboard** (`db_connector.py`) queries PostgreSQL with 1000+ lines of SQL
- **No cloud services** — no S3, RDS, Lambda, IaC, or deployment pipelines

## Requirements

| Dimension | Detail |
|-----------|--------|
| **Team size** | Small |
| **Refresh cadence** | Monthly or every few weeks |
| **Total rows** | ~60M across 5 sources |
| **Largest table** | ENTSOE (~47.5M rows) |
| **Existing DB layer** | PostgreSQL (materialized views, window functions, complex aggregations) |
| **Primary goal** | Ship quickly with minimal code changes |

## Options Evaluated

### 1. Neon (Serverless PostgreSQL)

Standard PostgreSQL — swap the connection string and everything works.

| | Detail |
|--|--------|
| **Free tier** | 0.5 GB storage, 190 compute hrs/month |
| **Paid tier** | ~$19/month |
| **Key feature** | Scales to zero when idle |
| **Code changes** | None — change `POSTGRES_*` env vars only |
| **Risk** | 60M rows may be tight in 0.5 GB free tier |

### 2. Supabase (Managed PostgreSQL)

Another hosted PostgreSQL option with a generous free tier.

| | Detail |
|--|--------|
| **Free tier** | 500 MB storage, 2 projects |
| **Paid tier** | $25/month |
| **Code changes** | None — change `POSTGRES_*` env vars only |
| **Downside** | More expensive than Neon on paid tier, fewer projects on free tier |

### 3. S3 + Parquet + DuckDB (No Database Server)

Eliminate the database entirely. Store data as Parquet files on S3, query with DuckDB.

| | Detail |
|--|--------|
| **Cost** | < $1/month (S3 storage only) |
| **Code changes** | Rewrite `db_connector.py` to use DuckDB instead of PostgreSQL |
| **Upside** | Cheapest long-term option, no running server |
| **Downside** | Significant work — not worth it if the goal is to ship quickly |

DuckDB speaks PostgreSQL-compatible SQL, so queries would be similar but not identical. Materialized views and some PostgreSQL-specific features would need reworking.

### 4. RDS PostgreSQL (AWS-Native)

AWS-managed PostgreSQL. Drop-in replacement for local Docker.

| | Detail |
|--|--------|
| **Free tier** | `db.t4g.micro` free for 12 months (new AWS accounts), 20 GB storage |
| **After free tier** | ~$12/month (`db.t4g.micro`) |
| **Production** | ~$25–50/month (`db.t3.small` or `db.t3.medium`) |
| **Code changes** | None — change `POSTGRES_*` env vars only |
| **Upside** | AWS-native, straightforward, handles 60M rows easily |
| **Downside** | Always running, no scale-to-zero |

## Cost Comparison

| Option | Monthly Cost | Code Changes | Scale to Zero |
|--------|-------------|--------------|---------------|
| **Neon** | $0–19 | None | Yes |
| **Supabase** | $0–25 | None | No |
| **S3 + Parquet + DuckDB** | < $1 | Rewrite db_connector.py | N/A (no server) |
| **RDS PostgreSQL** | $12–50 | None | No |

## Recommendation

**Start with Neon.**

- It's PostgreSQL, so zero code changes. `database_management.py`, `db_connector.py`, and all materialized views work as-is.
- The free tier may be enough. If not, the paid tier (~$19/month) scales to zero when idle — you only pay for compute when the dashboard is being queried.
- If you later want to move to AWS, it's just a connection string change to RDS.

The S3 + Parquet route is the cheapest long-term AWS option but requires rewriting the data access layer. Worth considering later if you want to eliminate ongoing database costs entirely, but not the right move if the goal is to ship quickly.

## Target Architecture

```
Extractors (run locally or on schedule)
    |  JSONL files
    v
S3 bucket (raw JSONL archive — cheap insurance)
    |
    v
database_management.py (existing ETL loader)
    |
    v
Neon / RDS PostgreSQL
    ^
    |
Streamlit Dashboard (EC2, ECS Fargate, or Streamlit Cloud)
```

### Monthly refresh workflow

A GitHub Actions workflow on a schedule (or a simple shell script):

1. Run each extractor
2. Upload JSONL to `s3://your-bucket/raw/{source}/{date}/`
3. Run `database_management.py load-data` against the cloud database
4. Refresh materialized views

That's it. This is a cron job, not a data platform.

## What to Avoid

Three services that sound relevant but are overkill for this workload:

**Glue / Athena** — The dashboard queries use window functions, materialized views, and complex aggregations that work great in PostgreSQL. Athena would be slower and more expensive for interactive queries over 60M rows.

**Lambda for ETL** — The extractors take minutes to hours (ENTSOE rate limiting, NPP scraping). Lambda's 15-minute timeout is a problem. A simple long-running script is more appropriate.

**Managed orchestration (Airflow, Step Functions)** — Overkill for 5 extractors running monthly. A single shell script or GitHub Actions workflow is enough.
