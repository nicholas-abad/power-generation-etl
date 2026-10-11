"""Checks for the Chile all-fuel staging pilot (migration 019). Read-only.

  fingerprint OUT.json        Coal-table fingerprint: row count, MWh, and an md5
                              over the measurement fields (timestamp_ms, plant,
                              chile_plant_id, fuel_type, generation_mwh) in key
                              order, plus monthly totals. Run metadata
                              (extraction_run_id, created_at_ms) is excluded.
  compare BEFORE.json AFTER.json
                              Exit 1 unless the two fingerprints are identical.
  reconcile MANIFEST START END
                              Per local day: the rows and MWh the extractor
                              manifest says it wrote (one line per plant-year)
                              against the stored rows, then the monthly view
                              against the table. Exit 1 on any failed
                              plant-year or mismatched day.
  coal-report START END OUT.csv
                              Per plant-month over local days START..END: coal-table MWh against unit-level
                              'Carbón' MWh and the non-coal units of coal-table
                              plants, labelled unchanged / revised / new_coal_plant
                              / absent_from_all_fuels. Input for the methodology
                              decision, not an automatic change.

Exit codes: 0 ok, 1 check failed, 2 configuration/connection error.
Usage: run_in_environment.py --environment staging --role writer -- \
           uv run python src/check_chile_all_fuels.py <command> ...
"""

import csv
import json
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from loguru import logger
from sqlalchemy import create_engine, text

SANTIAGO = ZoneInfo("America/Santiago")
TOLERANCE_MWH = 1e-6


def engine():
    url = (
        f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ.get('POSTGRES_PORT', '5432')}"
        f"/{os.environ.get('POSTGRES_DB', 'power_generation')}"
    )
    if os.environ.get("POSTGRES_SSLMODE"):
        url += f"?sslmode={os.environ['POSTGRES_SSLMODE']}"
    return create_engine(url, connect_args={"options": "-c default_transaction_read_only=on"})


def _utc(conn) -> None:
    """Pin the session to UTC: month buckets (DATE_TRUNC of TO_TIMESTAMP)
    follow the session TimeZone, and the views were built under UTC."""
    conn.execute(text("SET TIME ZONE 'UTC'"))


def fingerprint(conn) -> dict:
    _utc(conn)
    row = conn.execute(text(
        """
        SELECT COUNT(*), COALESCE(SUM(generation_mwh), 0),
               md5(COALESCE(string_agg(
                   concat_ws('|', timestamp_ms, plant, COALESCE(chile_plant_id, ''),
                             COALESCE(fuel_type, ''), generation_mwh::text),
                   E'\\n' ORDER BY timestamp_ms, plant, COALESCE(chile_plant_id, '')), ''))
        FROM ingestion.chile_generation_data
        """
    )).one()
    months = conn.execute(text(
        """
        SELECT to_char(DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)), 'YYYY-MM'),
               COUNT(*), SUM(generation_mwh)
        FROM ingestion.chile_generation_data GROUP BY 1 ORDER BY 1
        """
    )).all()
    return {
        "taken_at": datetime.now().isoformat(),
        "rows": row[0],
        "generation_mwh": float(row[1]),
        "md5": row[2],
        "months": {m: {"rows": n, "generation_mwh": float(s)} for m, n, s in months},
    }


def day_bounds_ms(day: date) -> tuple[int, int]:
    """UTC epoch-ms bounds of a local America/Santiago day [00:00, next 00:00).

    A midnight that does not exist (spring-forward) resolves to the first
    instant after the gap, matching the extractor's `shift_forward`.
    """
    def local_midnight(d: date) -> int:
        naive = datetime(d.year, d.month, d.day)
        aware = naive.replace(tzinfo=SANTIAGO)
        # zoneinfo maps a nonexistent wall time forward by the gap (fold=0).
        return int(aware.timestamp() * 1000)

    return local_midnight(day), local_midnight(day + timedelta(days=1))


def reconcile(conn, manifest_path: str, start: date, end: date) -> list[str]:
    # The latest line per plant-year (`<id_central>_<year>`) wins: a resumed
    # run appends an ok line after a failed one.
    latest: dict[str, dict] = {}
    with open(manifest_path) as fh:
        for line in fh:
            rec = json.loads(line)
            latest[rec["part_key"]] = rec

    problems: list[str] = []
    for key, rec in sorted(latest.items()):
        if rec.get("status") != "ok" and start.year <= int(key.rsplit("_", 1)[1]) <= end.year:
            problems.append(f"{key}: manifest status {rec.get('status')}: {rec.get('error', '')[:200]}")

    # Expected rows/MWh per LOCAL day of the stored instants, as the
    # extractor recorded them.
    expected: dict[str, list[float]] = {}
    for rec in latest.values():
        if rec.get("status") != "ok":
            continue
        for d, (n, m) in rec["by_local_day"].items():
            e = expected.setdefault(d, [0, 0.0])
            e[0] += n
            e[1] += m

    _utc(conn)
    total_rows, total_mwh = 0, 0.0
    d = start
    while d <= end:
        key = d.isoformat()
        lo, hi = day_bounds_ms(d)
        n, s = conn.execute(
            text(
                "SELECT COUNT(*), COALESCE(SUM(generation_mwh), 0) FROM ingestion.chile_generation_all_fuels "
                "WHERE timestamp_ms >= :lo AND timestamp_ms < :hi"
            ),
            {"lo": lo, "hi": hi},
        ).one()
        total_rows += n
        total_mwh += float(s)
        want_n, want_m = expected.get(key, [0, 0.0])
        if n != want_n or abs(float(s) - want_m) > TOLERANCE_MWH * max(1.0, abs(float(s))):
            problems.append(f"{key}: stored {n} rows / {float(s):.3f} MWh, manifest {want_n} rows / {want_m:.3f} MWh")
        d += timedelta(days=1)

    lo, _ = day_bounds_ms(start)
    _, hi = day_bounds_ms(end)
    table_total = float(conn.execute(
        text(
            "SELECT COALESCE(SUM(generation_mwh), 0) FROM ingestion.chile_generation_all_fuels "
            "WHERE timestamp_ms >= :lo AND timestamp_ms < :hi"
        ),
        {"lo": lo, "hi": hi},
    ).scalar())
    # The monthly view must match the whole table month by month.
    view_vs_table = conn.execute(text(
        """
        SELECT month, v.mwh, t.mwh FROM
          (SELECT month, SUM(generation_mwh) mwh FROM public.mv_chile_all_fuels_plant_monthly GROUP BY 1) v
          FULL JOIN
          (SELECT DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month, SUM(generation_mwh) mwh
             FROM ingestion.chile_generation_all_fuels GROUP BY 1) t USING (month)
        WHERE v.mwh IS DISTINCT FROM t.mwh
        """
    )).all()
    for month, v, t in view_vs_table:
        if v is None or t is None or abs(float(v) - float(t)) > TOLERANCE_MWH * max(1.0, abs(float(t))):
            problems.append(f"view {month:%Y-%m}: view {v} MWh, table {t} MWh (refresh the view?)")
    logger.info(f"{start}..{end}: {total_rows:,} stored rows, {table_total:,.1f} MWh")
    return problems


COAL_REPORT_SQL = """
WITH coal AS (
    SELECT DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month,
           chile_plant_id AS plant_key, MAX(plant) AS plant, SUM(generation_mwh) AS coal_table_mwh
    FROM ingestion.chile_generation_data
    WHERE timestamp_ms >= :lo AND timestamp_ms < :hi
    GROUP BY 1, 2
), af AS (
    SELECT DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)) AS month, plant_key, MAX(plant) AS plant,
           SUM(generation_mwh) FILTER (WHERE fuel_type = 'Carbón') AS unit_coal_mwh,
           SUM(generation_mwh) FILTER (WHERE fuel_type <> 'Carbón') AS other_units_mwh,
           string_agg(DISTINCT fuel_type, ', ') FILTER (WHERE fuel_type <> 'Carbón') AS other_fuels
    FROM ingestion.chile_generation_all_fuels
    WHERE timestamp_ms >= :lo AND timestamp_ms < :hi
    GROUP BY 1, 2
)
SELECT to_char(COALESCE(c.month, a.month), 'YYYY-MM') AS month,
       COALESCE(c.plant_key, a.plant_key) AS plant_key,
       COALESCE(c.plant, a.plant) AS plant,
       c.coal_table_mwh, a.unit_coal_mwh,
       CASE WHEN c.plant_key IS NOT NULL THEN a.other_units_mwh END AS coal_plant_other_units_mwh,
       CASE WHEN c.plant_key IS NOT NULL THEN a.other_fuels END AS coal_plant_other_fuels
FROM coal c
FULL JOIN af a ON a.month = c.month AND a.plant_key = c.plant_key
WHERE c.plant_key IS NOT NULL OR a.unit_coal_mwh IS NOT NULL
ORDER BY 1, 2
"""


def coal_report(conn, start: date, end: date, out_path: str) -> dict:
    lo, _ = day_bounds_ms(start)
    _, hi = day_bounds_ms(end)
    _utc(conn)
    rows = conn.execute(text(COAL_REPORT_SQL), {"lo": lo, "hi": hi}).mappings().all()
    counts: dict[str, int] = {}
    with open(out_path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["month", "plant_key", "plant", "coal_table_mwh", "unit_coal_mwh",
                    "coal_plant_other_units_mwh", "coal_plant_other_fuels", "status"])
        for r in rows:
            coal_t, unit_c = r["coal_table_mwh"], r["unit_coal_mwh"]
            if coal_t is None:
                status = "new_coal_plant"
            elif unit_c is None and not r["coal_plant_other_units_mwh"]:
                status = "absent_from_all_fuels"
            elif abs(float(coal_t) - float(unit_c or 0)) <= 1e-6 * max(1.0, float(coal_t)):
                status = "unchanged"
            else:
                status = "revised"
            counts[status] = counts.get(status, 0) + 1
            w.writerow([r["month"], r["plant_key"], r["plant"], coal_t, unit_c,
                        r["coal_plant_other_units_mwh"], r["coal_plant_other_fuels"], status])
    return counts


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    cmd, args = argv[0], argv[1:]
    if cmd == "compare":
        a, b = (json.load(open(p)) for p in args[:2])
        # md5 (per-row text of every measurement field) and row counts are the
        # exact test; float sums only to tolerance, since SUM order can vary.
        close = lambda x, y: abs(x - y) <= 1e-9 * max(1.0, abs(x))  # noqa: E731
        same = (
            a["rows"] == b["rows"]
            and a["md5"] == b["md5"]
            and close(a["generation_mwh"], b["generation_mwh"])
            and a["months"].keys() == b["months"].keys()
            and all(
                a["months"][m]["rows"] == b["months"][m]["rows"]
                and close(a["months"][m]["generation_mwh"], b["months"][m]["generation_mwh"])
                for m in a["months"]
            )
        )
        (logger.success if same else logger.error)(
            f"coal fingerprint {'unchanged' if same else 'CHANGED'}: {a['rows']} → {b['rows']} rows, md5 {a['md5']} → {b['md5']}"
        )
        return 0 if same else 1
    try:
        eng = engine()
        with eng.connect() as conn:
            if cmd == "fingerprint":
                fp = fingerprint(conn)
                with open(args[0], "w") as fh:
                    json.dump(fp, fh, indent=2)
                logger.info(f"coal fingerprint: {fp['rows']} rows, {fp['generation_mwh']:,.1f} MWh, md5 {fp['md5']}")
                return 0
            if cmd == "reconcile":
                problems = reconcile(conn, args[0], date.fromisoformat(args[1]), date.fromisoformat(args[2]))
                for p in problems:
                    logger.error(p)
                if not problems:
                    logger.success("reconciled: every day matches its manifest and the view matches the table")
                return 1 if problems else 0
            if cmd == "coal-report":
                counts = coal_report(conn, date.fromisoformat(args[0]), date.fromisoformat(args[1]), args[2])
                logger.info(f"coal report {args[0]}..{args[1]} → {args[2]}: {counts}")
                return 0
    except KeyError as e:
        logger.error(f"missing environment variable {e}")
        return 2
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
