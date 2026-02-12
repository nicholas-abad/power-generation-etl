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

**How scale-to-zero works.** Neon separates storage from compute — unlike RDS, where the PostgreSQL process and the disk live on the same server that's always running.

```
Traditional (RDS, local Docker):

┌─────────────────────────┐
│  EC2 / Container        │  ← always running, always billing
│  ┌───────────────────┐  │
│  │  PostgreSQL process│  │
│  └───────────────────┘  │
│  ┌───────────────────┐  │
│  │  EBS disk (data)  │  │
│  └───────────────────┘  │
└─────────────────────────┘


Neon (separated):

┌───────────────────┐       ┌───────────────────┐
│  Compute          │       │  Storage           │
│  (PostgreSQL)     │◄─────►│  (always on, cheap)│
│                   │       │  just bytes on disk │
│  spins UP on      │       └───────────────────┘
│  first connection │
│  shuts DOWN after │
│  5 min idle       │
└───────────────────┘
    ▲
    │ ~500ms–2s cold start
    │
  connection
```

- **Storage** is always on, but it's just data at rest — fractions of a cent per GB.
- **Compute** (the actual PostgreSQL process) starts when someone connects and stops after a configurable idle timeout (default 5 minutes).
- The cold start (~500ms–2s) is the time for the compute to spin up on the first query. Subsequent queries while the compute is warm are normal PostgreSQL speed.
- This is why Neon is cheaper than RDS for intermittent workloads: you're not paying for a server that sits idle 23 hours a day.

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

**How S3 + DuckDB works in practice.** There are actually two sub-options here: DuckDB (embedded query engine) and Athena (AWS serverless query engine). Both read Parquet files from S3, but they work very differently.

**Option A: S3 + DuckDB (embedded)**

The ETL converts JSONL to Parquet and uploads to S3. DuckDB runs *inside* the Streamlit process — it's an embedded library, not a separate server.

```
Extractors (5 sources)
    |  JSONL files
    v
ETL (converts to Parquet)
    |
    v
S3 bucket
    |  reads Parquet directly from S3
    v
┌──────────────────────────────┐
│  Streamlit app (EC2/Fargate) │
│  ┌────────────────────────┐  │
│  │  DuckDB (in-process)   │  │  ← runs inside the Python process
│  │  import duckdb          │  │     no separate database server
│  │  duckdb.sql("SELECT…") │  │
│  └────────────────────────┘  │
└──────────────────────────────┘
```

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("SET s3_region='us-east-1';")

df = conn.execute("""
    SELECT fuel_type, SUM(generation_mwh) as total
    FROM read_parquet('s3://your-bucket/data/eia/*.parquet')
    WHERE timestamp >= '2024-01-01'
    GROUP BY fuel_type
""").fetchdf()
```

**Where compute runs:** DuckDB is an in-process library (like SQLite). It runs on the same server as Streamlit, using that server's CPU and RAM. This means your deploy host (EC2 instance, Fargate container) needs enough resources to query 60M rows. For a small team this is fine — DuckDB is very fast on columnar Parquet data. But it doesn't scale to many concurrent users because each query competes for the same CPU/RAM.

**Option B: S3 + Athena (serverless query engine)**

Athena is AWS's serverless SQL engine. You point it at Parquet files in S3 and run queries. No server to manage, but you pay per query.

| | S3 + DuckDB | S3 + Athena | Neon |
|--|-------------|-------------|------|
| **Monthly cost** | < $1 (S3 only) | $1–5 (S3 + query fees) | $0–19 |
| **Query speed** | Fast (in-process) | Slow (2–10s cold start) | Normal PostgreSQL |
| **Server needed** | Yes (runs inside Streamlit) | No (serverless) | No (managed) |
| **Code changes** | Rewrite db_connector.py | Rewrite db_connector.py + use boto3 | None |
| **Materialized views** | No (pre-aggregate in ETL) | No | Yes (existing ones work) |
| **Pricing model** | Fixed (server cost) | Pay-per-query ($5/TB scanned) | Compute hours |

Athena charges $5 per TB scanned. With ~60M rows of generation data in compressed Parquet (~1–2 GB), each full-table scan costs about $0.01. But Athena has 2–10 second cold starts on every query, no materialized views, and requires writing queries through boto3 or the Athena SDK rather than a standard PostgreSQL driver. For an interactive dashboard, the latency is noticeable.

**Bottom line:** S3 + DuckDB is the cheapest option if you're willing to rewrite `db_connector.py`. S3 + Athena adds complexity without enough upside for this workload.

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

**Why RDS costs more than S3.** RDS is a *server* — an EC2 instance running 24/7 with CPU, RAM, storage, automated backups, and managed patching. You're paying for all of that whether anyone is querying or not. S3 is *storage only* — just bytes on disk at ~$0.023/GB/month with no compute attached. The tradeoff: RDS gives you a real PostgreSQL server with zero code changes; S3 gives you the cheapest possible hosting but requires a query engine (DuckDB or Athena) and a rewrite of the data access layer.

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
