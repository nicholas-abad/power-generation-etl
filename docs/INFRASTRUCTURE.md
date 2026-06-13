# Infrastructure

This document describes the deployed infrastructure for the power generation
pipeline and the reasoning behind each choice. The stack is intentionally
small: a serverless database, a scheduled CI job for the ETL, and a static-edge
frontend.

## Current Architecture

```
Extractors (7 sources: EIA, ENTSOE, NPP, ONS, OE, OCCTO, Chile)
    |  JSONL files
    v
GitHub Actions (weekly cron)  ── runs extractors, then the loader
    |
    v
database_management.py (ETL loader)  ── validate → stage → upsert
    |
    v
Neon (serverless PostgreSQL)  ── source of truth; materialized views
    ^
    |  @neondatabase/serverless HTTP driver
    |
Next.js dashboard (Cloudflare Pages, Edge runtime)
```

- **Extractors** output JSONL; a unified `energy-extract` CLI covers all 7 sources.
- **ETL loader** (`database_management.py`) validates each JSONL, loads it via a
  staging-table upsert (`ON CONFLICT DO NOTHING`), and the run refreshes the
  materialized views.
- **Orchestration** is a GitHub Actions weekly cron (`weekly-extraction.yml`) —
  no Airflow, no managed orchestrator.
- **Database** is Neon serverless PostgreSQL. All heavy query work
  (aggregations, window functions, materialized views over ~55M+ rows) runs on
  the database.
- **Dashboard** is a Next.js 15 app on Cloudflare Pages (Edge runtime), reading
  Neon over the `@neondatabase/serverless` HTTP driver. It is a thin renderer:
  it sends SQL and draws the resulting (already-aggregated) rows.

## Requirements

| Dimension | Detail |
|-----------|--------|
| **Team size** | Small |
| **Refresh cadence** | Weekly |
| **Total rows** | ~55M+ across 7 sources |
| **Largest table** | ENTSOE (~55M rows) |
| **DB layer** | PostgreSQL (materialized views, window functions, complex aggregations) |
| **Primary goal** | Cheap to run, near-zero ops, no servers to manage |

## Database: why Neon

The database options below were evaluated before settling on **Neon**. The
deciding factors: it is standard PostgreSQL (so the existing schema,
materialized views, and SQL work unchanged), it scales to zero between the
weekly load and sporadic dashboard visits, and its usage-based pricing suits an
intermittent workload far better than an always-on server.

### Neon (chosen) — Serverless PostgreSQL

Standard PostgreSQL — the connection string is the only configuration.

Neon's pricing is usage-based, not flat-tier. You pay separately for compute and storage.

| Plan | Compute | Storage | Minimum |
|------|---------|---------|---------|
| **Free** | 100 CU-hours/project (up to 2 CU) | 0.5 GB/project | $0 |
| **Launch** | $0.106/CU-hour (up to 16 CU) | $0.35/GB-month | None |
| **Scale** | $0.222/CU-hour (up to 56 CU) | $0.35/GB-month | None |

A CU (Compute Unit) = 1 vCPU + 4 GB RAM. With scale-to-zero, you only pay for compute while queries are running.

| | Detail |
|--|--------|
| **Free tier** | 0.5 GB storage, 100 CU-hours/project — too small for the dataset |
| **Launch tier (~55M rows)** | ~$2–4/month storage (5–10 GB) + ~$0.50–2/month compute (sporadic use) |
| **Key feature** | Scales to zero when idle, usage-based pricing |
| **Migration cost** | None — it's PostgreSQL; the schema/views/SQL are unchanged |

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

### Alternatives considered

**Supabase (managed PostgreSQL).** Also drop-in PostgreSQL, generous free tier (500 MB, 2 projects), $25/month paid. Rejected only because it is more expensive than Neon on the paid tier and lacks scale-to-zero.

**S3 + Parquet + DuckDB (no database server).** Store data as Parquet on S3 and query with an embedded engine (DuckDB) or a serverless one (Athena). Cost is near-zero for storage, but DuckDB runs *in-process* on whatever host queries it — so it needs a beefy always-on query host (~$30/month for an EC2 `t3.medium`), which ends up **more expensive** than Neon plus a free frontend, and it would require rewriting the entire data-access layer away from PostgreSQL (losing the materialized views). Athena adds 2–10s cold starts and pay-per-scan billing without enough upside. Not worth it.

**RDS PostgreSQL (AWS-native).** Drop-in PostgreSQL, ~$12/month after the 12-month free tier, ~$25–50/month for production sizes. Rejected because it is always running (no scale-to-zero) and costs more than Neon for an intermittent workload. It remains the easy fallback if an AWS-native database is ever required — it's a connection-string change.

### Database cost comparison

| Option | Monthly Cost | Migration | Scale to Zero |
|--------|-------------|-----------|---------------|
| **Neon (chosen)** | ~$3–6 (Launch) | None | Yes |
| **Supabase** | $0–25 | None | No |
| **S3 + Parquet + DuckDB** | < $1 storage, but needs a ~$30 query host | Rewrite data layer | N/A (no server) |
| **RDS PostgreSQL** | $12–50 | None | No |

## Dashboard Hosting: Next.js on Cloudflare Pages

The dashboard (`dashboard/energy-generation-dashboard/nextjs`) is a Next.js 15
App Router application built for the Cloudflare Pages **Edge runtime** via
`@cloudflare/next-on-pages`, with the `nodejs_compat` flag enabled. It reads
Neon directly through the `@neondatabase/serverless` HTTP driver (not a TCP
connection pool), which is what makes it Edge-safe.

### Why this fits

Because **Neon does all the query compute**, the frontend only needs to issue
SQL and render already-aggregated results (hundreds to a few thousand rows per
view). There is no in-process query engine and no large dataset held in memory,
so the dashboard has effectively no compute or RAM requirements of its own — it
is a static/edge-rendered frontend plus thin data-fetching. That is exactly the
profile Cloudflare Pages serves best:

- **Cost:** the Cloudflare Pages free tier (generous request/bandwidth limits,
  500 builds/month) covers this workload — effectively $0.
- **No servers:** no instance to patch, no container to size, no scaling config.
- **Global edge + automatic HTTPS + custom domains** out of the box.
- **Git-driven deploys:** every push to `main` of the dashboard submodule builds
  and deploys automatically; no manual release step.

### The Edge-runtime constraint

The Edge runtime is not full Node.js, which drives two non-negotiable rules in
the dashboard code:

- **Use the Neon HTTP driver** (`@neondatabase/serverless`), never a `pg` TCP
  pool — Edge has no long-lived TCP sockets.
- **No Node-only APIs** in route handlers (no `fs`, no native modules). The
  `nodejs_compat` flag covers the shims the build needs.

### Cold-start behavior

When nobody has visited in a while, the only cold start is Neon compute waking
on the first query (~0.5–2s); the Pages edge itself does not cold-start the way
a container does. First load after idle feels slightly slow; subsequent
interactions are normal speed. Keeping Neon's idle timeout at its default is the
right trade for a low-traffic dashboard.

### Total cost

| Component | Monthly Cost |
|-----------|-------------|
| **Neon (Launch, ~55M rows)** | ~$3–6 |
| **Cloudflare Pages (frontend)** | ~$0 (free tier) |
| **GitHub Actions (weekly ETL)** | ~$0 (within free minutes) |
| **Total** | **~$3–6** |

If the dashboard ever needs to move off Cloudflare (e.g. for a Node-only
dependency), Vercel or a small container host are drop-in alternatives — but the
HTTP-driver + thin-renderer design means almost any host works, and none of them
change the database side.

## ETL Orchestration: GitHub Actions

The pipeline runs weekly: extract new data for each source, load it into Neon,
refresh the materialized views, rebuild the plant crosswalk, and check for
drift. This is a linear sequence of CLI steps — not a DAG — so a cron-triggered
CI workflow is the right tool, not a managed orchestrator.

### The deployed workflow

`weekly-extraction.yml` runs on a weekly schedule (plus a manual
`workflow_dispatch` trigger). Its shape:

```
1. Extract — one job per source (energy-extract <source>), in parallel,
             each with its own concurrency group and failure artifact
2. Load    — database_management.py load-data <source> <jsonl>
             (validate → stage → ON CONFLICT DO NOTHING upsert)
3. Rebuild — rebuild the NPP plant crosswalk (guarded against degraded refdata)
4. Refresh — refresh_views.py (REFRESH MATERIALIZED VIEW … CONCURRENTLY)
5. Drift   — check_crosswalk_drift.py
6. Notify  — open a GitHub issue if any job failed
```

Secrets (`POSTGRES_*`, per-source API keys) come from GitHub Actions secrets;
`POSTGRES_SSLMODE=require`. Per-source jobs mean a single source failing doesn't
block the others, and the notify-failure job surfaces any failure as an issue.

### Why GitHub Actions over an orchestrator

| Option | Monthly Cost | Setup Effort | Best For |
|--------|-------------|--------------|----------|
| **GitHub Actions (cron, chosen)** | $0 (free minutes) | Low — YAML in repo | This workload |
| **EventBridge + ECS task** | ~$1–3 (pay per run) | Medium — IaC + container | AWS-native shops |
| **Self-hosted Airflow** | ~$15–30 (EC2 `t3.small`–`t3.medium`) | High — install, manage, maintain | 10+ DAGs with complex dependencies |
| **MWAA (AWS managed Airflow)** | ~$50+ (minimum environment cost) | Medium — but expensive floor | Large teams, compliance requirements |
| **Prefect / Dagster Cloud** | $0–20 (free tiers available) | Medium — agent + flow code | Growing pipelines, observability |

GitHub Actions gives cron scheduling, secrets, run history, manual triggers, and
failure notifications for free. An orchestrator would add value only if the
pipeline grew to many sources with real cross-source dependencies or conditional
branching; until then it's pure operational overhead.

### Retry behavior

The loader already handles the most common failure mode — transient database
errors — automatically: `database.py`'s retry decorator uses `tenacity` with
exponential backoff (1–10s, up to 3 attempts) for `OperationalError`,
`InterfaceError`, and `ConnectionError`. For step-level retries (e.g. an
extractor hitting a rate limit), a community action such as
[`nick-fields/retry`](https://github.com/nick-fields/retry) wraps any step:

```yaml
- uses: nick-fields/retry@v3
  with:
    timeout_minutes: 30
    max_attempts: 3
    command: uv run energy-extract entsoe --output ./output --yes
```

A whole-run or partial-load failure is recovered by re-running the failed jobs
from the GitHub UI (each source is a separate job, so only the failed ones need
re-running), and every load is idempotent (`ON CONFLICT DO NOTHING`), so
re-running is always safe.

## What to Avoid

Services that sound relevant but are overkill for this workload:

**Glue / Athena** — the dashboard relies on window functions, materialized
views, and complex aggregations that PostgreSQL does natively. Athena would be
slower and more expensive for interactive queries over tens of millions of rows.

**Lambda for ETL** — extractors take minutes to hours (ENTSOE rate limiting, NPP
scraping). Lambda's 15-minute timeout is a poor fit; a long-running CI job is
appropriate.

**Managed orchestration (Airflow, Step Functions)** — overkill for 7 extractors
running weekly. GitHub Actions covers this workload for free; see
[ETL Orchestration](#etl-orchestration-github-actions).

**An always-on server for the dashboard** — the frontend holds no data and runs
no query engine, so paying for an idle EC2/container would be wasted; an
edge/static host (Cloudflare Pages) is both cheaper and lower-ops.
