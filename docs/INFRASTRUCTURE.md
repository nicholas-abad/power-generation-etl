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

Neon's pricing is usage-based, not flat-tier. You pay separately for compute and storage.

| Plan | Compute | Storage | Minimum |
|------|---------|---------|---------|
| **Free** | 100 CU-hours/project (up to 2 CU) | 0.5 GB/project | $0 |
| **Launch** | $0.106/CU-hour (up to 16 CU) | $0.35/GB-month | None |
| **Scale** | $0.222/CU-hour (up to 56 CU) | $0.35/GB-month | None |

A CU (Compute Unit) = 1 vCPU + 4 GB RAM. With scale-to-zero, you only pay for compute while queries are running.

| | Detail |
|--|--------|
| **Free tier** | 0.5 GB storage, 100 CU-hours/project — too small for 60M rows |
| **Launch tier (60M rows)** | ~$2–4/month storage (5–10 GB) + ~$0.50–2/month compute (sporadic use) |
| **Key feature** | Scales to zero when idle, usage-based pricing |
| **Code changes** | None — change `POSTGRES_*` env vars only |

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
| **Upside** | No database server — but still needs a beefy app host for DuckDB |
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
| **Monthly cost** | < $1 (S3 only) | $1–5 (S3 + query fees) | ~$3–6 (Launch) |
| **Query speed** | Fast (in-process) | Slow (2–10s cold start) | Normal PostgreSQL |
| **Server needed** | Yes (runs inside Streamlit) | No (serverless) | No (managed) |
| **Code changes** | Rewrite db_connector.py | Rewrite db_connector.py + use boto3 | None |
| **Materialized views** | No (pre-aggregate in ETL) | No | Yes (existing ones work) |
| **Pricing model** | Fixed (server cost) | Pay-per-query ($5/TB scanned) | Compute hours |

Athena charges $5 per TB scanned. With ~60M rows of generation data in compressed Parquet (~1–2 GB), each full-table scan costs about $0.01. But Athena has 2–10 second cold starts on every query, no materialized views, and requires writing queries through boto3 or the Athena SDK rather than a standard PostgreSQL driver. For an interactive dashboard, the latency is noticeable.

**Bottom line:** S3 + DuckDB eliminates database costs, but you still need a beefier app host (EC2 `t3.medium` ~$30/month) since DuckDB runs in-process. This makes it **more expensive** than Neon (~$3–6/month) + a lightweight host. S3 + Athena adds even more complexity without enough upside for this workload.

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
| **Neon** | ~$3–6 (Launch, 60M rows) | None | Yes |
| **Supabase** | $0–25 | None | No |
| **S3 + Parquet + DuckDB** | < $1 | Rewrite db_connector.py | N/A (no server) |
| **RDS PostgreSQL** | $12–50 | None | No |

## Dashboard Hosting (Streamlit)

Where the Streamlit app runs depends on which database option you choose — specifically, where query compute happens.

### Why it matters

With **Neon or RDS**, all heavy query work (aggregations, window functions, materialized views over 60M rows) runs on the database server. The Streamlit app just sends SQL and renders the resulting DataFrames. This is lightweight — any host works.

With **S3 + DuckDB**, DuckDB runs *inside* the Streamlit process. The host needs enough CPU and RAM to query 60M rows of Parquet data in-process. This rules out resource-constrained options.

### Option 1: Streamlit Community Cloud (Free)

| | Detail |
|--|--------|
| **Cost** | $0 |
| **RAM** | ~1 GB |
| **Setup** | Connect GitHub repo, deploy from UI |
| **Sleep behavior** | App sleeps after inactivity, ~10–20s cold start on wake |
| **Visibility** | Public by default (free tier) |

**Works with Neon/RDS** — the app is just a thin frontend sending SQL queries and rendering charts. The 1 GB RAM limit is fine when you're receiving pre-aggregated DataFrames (hundreds/thousands of rows after aggregation).

**Does not work with S3 + DuckDB** — 1 GB RAM is not enough for in-process queries over 60M rows.

**Watch out for:** pulling very large result sets into memory (e.g., plant-level monthly data across all of ENTSOE). Worth testing against the 1 GB limit.

**Double cold start with Neon:** When nobody has visited in a while, both Streamlit (~10–20s) and Neon (~0.5–2s) need to wake up. First load feels slow, subsequent interactions are normal speed.

### Option 2: EC2

| Instance | vCPUs | RAM | Monthly Cost (on-demand) | Monthly Cost (reserved 1yr) |
|----------|-------|-----|--------------------------|----------------------------|
| `t3.micro` | 2 | 1 GB | ~$7 | ~$5 |
| `t3.small` | 2 | 2 GB | ~$15 | ~$10 |
| `t3.medium` | 2 | 4 GB | ~$30 | ~$20 |
| `t3.large` | 2 | 8 GB | ~$60 | ~$40 |

**With Neon/RDS:** A `t3.micro` or `t3.small` ($7–15/month) is plenty — the app is just serving a web UI.

**With S3 + DuckDB:** A `t3.medium` (4 GB, ~$30/month) is the sweet spot. DuckDB is efficient with columnar Parquet, but it needs room to work. Bump to `t3.large` if queries feel slow.

You manage the instance yourself (install Python, run Streamlit, optionally put nginx in front).

### Option 3: AWS App Runner

| | Detail |
|--|--------|
| **Cost** | ~$0–5/month (low traffic) |
| **Setup** | Push a container image, get a URL |
| **Key feature** | Scales to zero when idle |
| **Downside** | Cold starts when scaling from zero |

Push a container, get a URL. Scales to zero when nobody is using it. Simplest "upgrade" from Streamlit Community Cloud if you want to stay on AWS without managing a server.

### Option 4: ECS Fargate

| | Detail |
|--|--------|
| **Cost** | ~$10–15/month (small always-on container) |
| **Setup** | Dockerfile + task definition |
| **Key feature** | Containerized, no EC2 to manage |
| **Downside** | More setup upfront than App Runner |

More control than App Runner, but more configuration. Good for long-term production if you want container orchestration without managing EC2 instances.

### Option 5: Streamlit Teams/Enterprise

| | Detail |
|--|--------|
| **Cost** | ~$250/month per workspace |
| **Setup** | Same as Community Cloud |
| **Key feature** | Private apps, auth, more resources |
| **Downside** | Overkill price for a small team |

Same managed experience as Community Cloud with fewer restrictions. Only worth it if you specifically need their managed auth and private app features.

### Dashboard Hosting Summary

| Option | Monthly Cost | Effort | Works with Neon/RDS | Works with S3 + DuckDB |
|--------|-------------|--------|---------------------|------------------------|
| **Streamlit Community Cloud** | $0 | None | Yes | No (not enough RAM) |
| **EC2 `t3.micro`/`t3.small`** | $7–15 | Some (manage server) | Yes | No |
| **EC2 `t3.medium`** | ~$30 | Some (manage server) | Yes | Yes |
| **App Runner** | $0–5 | Low (push container) | Yes | Depends on config |
| **ECS Fargate** | $10–15 | Medium (container setup) | Yes | Depends on config |
| **Streamlit Teams** | $250 | None | Yes | No |

**Recommended path:** Start on Streamlit Community Cloud (free) with Neon. If you hit resource limits, move to EC2 `t3.small` (~$15/month) or App Runner (~$0–5/month) for the cheapest upgrade with scale-to-zero.

## Recommendation

**Start with Neon + Streamlit Community Cloud.**

- It's PostgreSQL, so zero code changes. `database_management.py`, `db_connector.py`, and all materialized views work as-is.
- The free tier (0.5 GB) is too small for 60M rows. The Launch tier is usage-based: ~$2–4/month for storage (5–10 GB) + ~$0.50–2/month for compute (sporadic use). **Total Neon cost: ~$3–6/month.**
- Neon scales to zero when idle — you only pay for compute while queries are running.
- Streamlit Community Cloud is free. Since Neon handles all query compute, the dashboard is just a thin frontend — 1 GB RAM is plenty for rendering pre-aggregated DataFrames.
- **Total cost: ~$3–6/month** for the full stack (database + dashboard).
- If you later want to move to AWS, it's just a connection string change to RDS.

### If Streamlit Community Cloud isn't enough

Two upgrade paths, depending on whether you want always-on or scale-to-zero:

**EC2 (`t3.small`, ~$15/month)** — Always on, no cold starts, but you manage the server (OS updates, nginx, systemd). Best if the dashboard is used daily and you want it to always feel snappy.

**App Runner (~$1–3/month)** — Scales to zero like Neon, no server to manage, push a container and get a URL. But first visit after idle has a double cold start: App Runner container (~10–30s) + Neon compute (~0.5–2s). Best if usage is sporadic and you don't want to manage infrastructure.

| | EC2 (`t3.small`) | App Runner |
|--|------------------|------------|
| **Monthly cost** | ~$15 (fixed) | ~$1–3 (usage-based) |
| **Idle cost** | Full price | $0 (scales to zero) |
| **Cold start** | None (always on) | ~10–30s (+ Neon's ~0.5–2s) |
| **Deploy process** | SSH in, git pull, restart | Push container image |
| **Server management** | You (OS, patches, nginx) | AWS |
| **HTTPS** | Manual (certbot/nginx) | Automatic |
| **Custom domain** | Manual (Route 53 + nginx) | Built-in |

### Total cost estimates by configuration

| Configuration | Monthly Cost | Code Changes | Server Management |
|--------------|-------------|--------------|-------------------|
| **Neon + Streamlit Community Cloud** | ~$3–6 | None | None |
| **Neon + App Runner** | ~$4–9 | None | None |
| **Neon + EC2 `t3.small`** | ~$18–21 | None | You |
| **S3 + DuckDB + EC2 `t3.medium`** | ~$30 | Rewrite db_connector.py | You |

The S3 + Parquet route eliminates database costs, but DuckDB runs in-process — so you need a beefier host (~$30/month for EC2 `t3.medium`). That's actually **more expensive** than Neon + Streamlit Community Cloud (~$3–6/month) or Neon + App Runner (~$4–9/month), and it requires rewriting `db_connector.py`. Only worth considering if you have a specific reason to avoid a managed database.

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

## Scheduling & Orchestration

The pipeline runs monthly: extract new data, load it into the database, refresh materialized views. This section compares ways to schedule and automate that.

### What needs orchestrating

The full pipeline is three sequential steps:

```bash
# 1. Extract — run each source's extractor
cd extractors/entsoe-power-generation && uv run src/main.py
cd extractors/npp-india-generation && uv run src/main.py
cd extractors/eia-usa-generation && uv run src/main.py
cd extractors/ons-brazil-generation && uv run src/main.py
cd extractors/australia-openelectricity-generation && uv run src/main.py

# 2. Load — insert JSONL into the database
cd etl/power-generation-etl
uv run src/database_management.py load-data entsoe ./data/entsoe_data.jsonl
uv run src/database_management.py load-data npp ./data/npp_data.jsonl
uv run src/database_management.py load-data eia ./data/eia_data_etl.jsonl
uv run src/database_management.py load-data ons ./data/ons_data_etl.jsonl
uv run src/database_management.py load-data oe ./data/oe_data_etl.jsonl

# 3. Refresh materialized views
psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_entsoe_monthly;"
psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_entsoe_plant_monthly;"
psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ons_monthly;"
psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ons_plant_monthly;"
psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_npp_monthly;"
psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_npp_plant_monthly;"
```

That's it. No DAG, no task dependencies, no branching logic. It's a linear script that runs once a month.

### Options comparison

| Option | Monthly Cost | Setup Effort | Best For |
|--------|-------------|--------------|----------|
| **GitHub Actions (cron)** | $0 (free tier: 2,000 min/month) | Low — YAML file in repo | This workload |
| **EventBridge + ECS task** | ~$1–3 (pay per run) | Medium — IaC + container | AWS-native shops |
| **Self-hosted Airflow** | ~$15–30 (EC2 `t3.small`–`t3.medium`) | High — install, manage, maintain | 10+ DAGs with complex dependencies |
| **MWAA (AWS managed Airflow)** | ~$50+ (minimum environment cost) | Medium — but expensive floor | Large teams, compliance requirements |
| **Prefect / Dagster Cloud** | $0–20 (free tiers available) | Medium — agent + flow code | Growing pipelines, observability |

### GitHub Actions example

A cron workflow that runs on the 2nd of each month:

```yaml
# .github/workflows/monthly-etl.yml
name: Monthly ETL Pipeline

on:
  schedule:
    - cron: '0 6 2 * *'  # 6:00 UTC on the 2nd of each month
  workflow_dispatch:       # manual trigger button

jobs:
  extract-and-load:
    runs-on: ubuntu-latest
    timeout-minutes: 120
    env:
      POSTGRES_HOST: ${{ secrets.POSTGRES_HOST }}
      POSTGRES_PORT: ${{ secrets.POSTGRES_PORT }}
      POSTGRES_DB: ${{ secrets.POSTGRES_DB }}
      POSTGRES_USER: ${{ secrets.POSTGRES_USER }}
      POSTGRES_PASSWORD: ${{ secrets.POSTGRES_PASSWORD }}

    steps:
      - uses: actions/checkout@v4
        with:
          submodules: true

      - uses: astral-sh/setup-uv@v4

      # Extract
      - name: Extract ENTSOE
        run: cd extractors/entsoe-power-generation && uv run src/main.py
        env:
          ENTSOE_API_KEY: ${{ secrets.ENTSOE_API_KEY }}

      - name: Extract EIA
        run: cd extractors/eia-usa-generation && uv run src/main.py
        env:
          EIA_API_KEY: ${{ secrets.EIA_API_KEY }}

      # ... other extractors ...

      # Load
      - name: Load ENTSOE
        run: cd etl/power-generation-etl && uv run src/database_management.py load-data entsoe ./data/entsoe_data.jsonl

      - name: Load EIA
        run: cd etl/power-generation-etl && uv run src/database_management.py load-data eia ./data/eia_data_etl.jsonl

      # ... other sources ...

      # Refresh views
      - name: Refresh materialized views
        run: |
          PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB \
            -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_entsoe_monthly;
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_entsoe_plant_monthly;
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ons_monthly;
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ons_plant_monthly;
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_npp_monthly;
                REFRESH MATERIALIZED VIEW CONCURRENTLY mv_npp_plant_monthly;"
```

**Notes:**
- `workflow_dispatch` adds a "Run workflow" button in the GitHub UI for manual runs
- Extractors can run in parallel using separate jobs if you want faster execution, but serial is simpler and stays within free tier minutes
- Free tier gives 2,000 minutes/month on public repos, 500 on private — more than enough for a monthly run

### Retry capabilities

The pipeline already has built-in retry logic for the most common failure mode (database connectivity). Here's what's covered and what isn't:

| Failure Scenario | GitHub Actions | Airflow | Already Built In? |
|-----------------|---------------|---------|-------------------|
| **DB connection drops mid-load** | Handled by `database.py` | Handled by `database.py` | Yes — exponential backoff, 3 attempts, 1–10s wait (`database.py:62–81`) |
| **Extractor HTTP error** | Re-run failed step manually or use `nick-fields/retry` action | Task-level retry with backoff | Depends on extractor |
| **Entire workflow fails** | Re-run from GitHub UI, or re-run failed jobs only | Automatic retry per task | No |
| **Partial load (some sources succeed, others fail)** | Re-run failed jobs (if split into separate jobs) | Retry only failed tasks | No |

The `database.py` retry decorator handles transient database errors (connection drops, timeouts) automatically — this is the most likely failure mode during loading. It uses the `tenacity` library with exponential backoff (1–10 seconds) and retries up to 3 times for `OperationalError`, `InterfaceError`, and `ConnectionError`.

For step-level retries in GitHub Actions (e.g., retrying an extractor that hit a rate limit), community actions like [`nick-fields/retry`](https://github.com/nick-fields/retry) add configurable retry with backoff to any step:

```yaml
- uses: nick-fields/retry@v3
  with:
    timeout_minutes: 30
    max_attempts: 3
    command: cd extractors/entsoe-power-generation && uv run src/main.py
```

### Recommendation

**Use GitHub Actions for this workload.** The pipeline is a linear sequence of CLI commands that runs monthly. GitHub Actions gives you cron scheduling, secrets management, run history, manual triggers, and email notifications on failure — all for free.

**When Airflow makes sense:** If the pipeline grows to 10+ sources with complex dependencies (e.g., source B depends on source A's output, conditional branching based on data quality checks, backfills across date ranges), then a proper orchestrator adds value. Until then, it's an operational burden — you'd be maintaining an Airflow server (or paying $50+/month for MWAA) to run what amounts to a shell script.

## What to Avoid

Three services that sound relevant but are overkill for this workload:

**Glue / Athena** — The dashboard queries use window functions, materialized views, and complex aggregations that work great in PostgreSQL. Athena would be slower and more expensive for interactive queries over 60M rows.

**Lambda for ETL** — The extractors take minutes to hours (ENTSOE rate limiting, NPP scraping). Lambda's 15-minute timeout is a problem. A simple long-running script is more appropriate.

**Managed orchestration (Airflow, Step Functions)** — Overkill for 5 extractors running monthly. See the [Scheduling & Orchestration](#scheduling--orchestration) section for a full comparison — GitHub Actions covers this workload for free.
