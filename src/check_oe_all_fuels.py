"""Checks for the OpenElectricity all-fuel staging pilot (migration 022). Read-only.

  fingerprint OUT.json        Fingerprint of the existing OE facility table
                              (ingestion.oe_facility_generation_data): row count,
                              MWh, md5 over the measurement fields in key order,
                              monthly totals. Run metadata is excluded.
  compare BEFORE.json AFTER.json
                              Exit 1 unless the two fingerprints are identical.
  reconcile START END MANIFEST [MANIFEST ...]
                              Every network-month part in START..END (for each
                              network with a manifest) needs an ok manifest line
                              covering its days; per network-local day, the rows
                              and MWh the manifests say were written are compared
                              with the stored rows; then the monthly view against
                              the table at its full key. Exit 1 on any gap.
  fuel-report START END OUT.csv
                              Per network, local month, fueltech group and
                              direction: MWh and unit counts; plus coal and gas
                              injection against the existing OE facility table
                              (a plumbing check: same source).

Exit codes: 0 ok, 1 check failed, 2 configuration/connection error.
Usage: run_in_environment.py --environment staging --role writer -- \
           uv run python src/check_oe_all_fuels.py <command> ...
"""

import csv
import json
import math
import os
import sys
from datetime import date, datetime, timedelta, timezone

from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

OFFSET_HOURS = {"NEM": 10, "WEM": 8}
TZ_NAME = {"NEM": "Australia/Brisbane", "WEM": "Australia/Perth"}
TOLERANCE_MWH = 1e-6
LOCAL_TS = (
    "TO_TIMESTAMP(timestamp_ms / 1000) AT TIME ZONE "
    "CASE network_code WHEN 'WEM' THEN 'Australia/Perth' ELSE 'Australia/Brisbane' END"
)


def engine():
    url = (
        f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ.get('POSTGRES_PORT', '5432')}"
        f"/{os.environ.get('POSTGRES_DB', 'power_generation')}"
    )
    if os.environ.get("POSTGRES_SSLMODE"):
        url += f"?sslmode={os.environ['POSTGRES_SSLMODE']}"
    return create_engine(
        url, connect_args={"options": "-c default_transaction_read_only=on"}
    )


def _utc(conn) -> None:
    conn.execute(text("SET TIME ZONE 'UTC'"))


def fingerprint(conn) -> dict:
    _utc(conn)
    row = conn.execute(
        text(
            """
        SELECT COUNT(*), COALESCE(SUM(generation_mwh), 0),
               md5(COALESCE(string_agg(
                   concat_ws('|', timestamp_ms, facility_code, fueltech, network_code, generation_mwh::text),
                   E'\\n' ORDER BY timestamp_ms, facility_code, fueltech), ''))
        FROM ingestion.oe_facility_generation_data
        """
        )
    ).one()
    months = conn.execute(
        text(
            """
        SELECT to_char(DATE_TRUNC('month', TO_TIMESTAMP(timestamp_ms / 1000)), 'YYYY-MM'),
               COUNT(*), SUM(generation_mwh)
        FROM ingestion.oe_facility_generation_data GROUP BY 1 ORDER BY 1
        """
        )
    ).all()
    return {
        "taken_at": datetime.now().isoformat(),
        "rows": row[0],
        "generation_mwh": float(row[1]),
        "md5": row[2],
        "months": {m: {"rows": n, "generation_mwh": float(s)} for m, n, s in months},
    }


def day_bounds_ms(network: str, day: date) -> tuple[int, int]:
    tz = timezone(timedelta(hours=OFFSET_HOURS[network]))
    lo = datetime(day.year, day.month, day.day, tzinfo=tz)
    return int(lo.timestamp() * 1000), int((lo + timedelta(days=1)).timestamp() * 1000)


def _months(start: date, end: date):
    cur = date(start.year, start.month, 1)
    while cur <= end:
        nxt = date(cur.year + (cur.month == 12), cur.month % 12 + 1, 1)
        yield (
            max(cur, start),
            min(nxt - timedelta(days=1), end),
            f"{cur.year}-{cur.month:02d}",
        )
        cur = nxt


def reconcile(conn, start: date, end: date, manifest_paths: list[str]) -> list[str]:
    if not manifest_paths:
        raise ValueError("reconcile needs at least one manifest")
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    problems: list[str] = []
    expected: dict[tuple[str, str], list[float]] = {}
    networks = []
    for path in manifest_paths:
        latest: dict[str, dict] = {}
        with open(path) as fh:
            for line in fh:
                if line.strip():
                    rec = json.loads(line)
                    latest[rec["part_key"]] = rec
        network = next(iter(latest)).split("_")[0] if latest else None
        if network not in OFFSET_HOURS:
            problems.append(f"{path}: no network parts")
            continue
        if network in networks:
            raise ValueError(
                f"two manifests for {network}: pass one manifest per network"
            )
        networks.append(network)
        for lo, hi, label in _months(start, end):
            key = f"{network}_{label}"
            rec = latest.get(key)
            if rec is None:
                problems.append(f"{key}: not in manifest (never extracted?)")
            elif rec.get("status") != "ok":
                problems.append(
                    f"{key}: manifest status {rec.get('status')}: {str(rec.get('error', ''))[:200]}"
                )
            elif not rec.get("start") or not rec.get("end"):
                problems.append(f"{key}: manifest line has no start/end")
            elif rec["start"] > lo.isoformat() or rec["end"] < hi.isoformat():
                problems.append(
                    f"{key}: covers {rec.get('start')}..{rec.get('end')}, needed {lo}..{hi}"
                )
        for rec in latest.values():
            if rec.get("status") == "ok":
                for d, (n, m) in rec["by_local_day"].items():
                    e = expected.setdefault((network, d), [0, 0.0])
                    e[0] += n
                    e[1] += m

    _utc(conn)
    for network in networks:
        d = start
        while d <= end:
            lo, hi = day_bounds_ms(network, d)
            n, s = conn.execute(
                text(
                    "SELECT COUNT(*), COALESCE(SUM(energy_mwh), 0) FROM ingestion.oe_unit_generation_all_fuels "
                    "WHERE network_code = :net AND timestamp_ms >= :lo AND timestamp_ms < :hi"
                ),
                {"net": network, "lo": lo, "hi": hi},
            ).one()
            want_n, want_m = expected.get((network, d.isoformat()), [0, 0.0])
            if not (math.isfinite(float(s)) and math.isfinite(want_m)):
                problems.append(
                    f"{network} {d}: non-finite energy (stored {s}, manifest {want_m})"
                )
            elif n != want_n or abs(float(s) - want_m) > TOLERANCE_MWH * max(
                1.0, abs(float(s))
            ):
                problems.append(
                    f"{network} {d}: stored {n} rows / {float(s):.3f} MWh, "
                    f"manifest {want_n} rows / {want_m:.3f} MWh"
                )
            d += timedelta(days=1)

    stale = conn.execute(text(VIEW_VS_TABLE_SQL), {"tol": TOLERANCE_MWH}).all()
    for r in stale[:5]:
        problems.append(
            f"view {r.month} {r.network_code} {r.unit_code} {r.direction}: view and table differ (refresh the view?)"
        )
    if len(stale) > 5:
        problems.append(f"view: {len(stale) - 5} more groups differ from the table")
    return problems


VIEW_VS_TABLE_SQL = f"""
WITH t AS (
    SELECT DATE_TRUNC('month', {LOCAL_TS})::date AS month, network_code, unit_code, direction,
           MAX(fueltech) AS fueltech, MAX(fueltech_group) AS fueltech_group, MAX(network_region) AS network_region,
           MAX(facility_code) AS facility_code, MAX(facility_name) AS facility_name,
           SUM(energy_mwh) AS mwh, COUNT(*) AS days, COUNT(*) FILTER (WHERE basis = 'derived') AS derived_days
    FROM ingestion.oe_unit_generation_all_fuels
    GROUP BY 1, 2, 3, 4
)
SELECT COALESCE(t.month, v.month) AS month, COALESCE(t.network_code, v.network_code) AS network_code,
       COALESCE(t.unit_code, v.unit_code) AS unit_code, COALESCE(t.direction, v.direction) AS direction
FROM t
FULL JOIN public.mv_oe_all_fuels_unit_monthly v
  ON v.month = t.month AND v.network_code = t.network_code AND v.unit_code = t.unit_code AND v.direction = t.direction
WHERE t.month IS NULL OR v.month IS NULL
   OR v.fueltech IS DISTINCT FROM t.fueltech OR v.fueltech_group IS DISTINCT FROM t.fueltech_group
   OR v.network_region IS DISTINCT FROM t.network_region OR v.facility_code IS DISTINCT FROM t.facility_code
   OR v.facility_name IS DISTINCT FROM t.facility_name OR v.days <> t.days OR v.derived_days <> t.derived_days
   OR ABS(v.energy_mwh - t.mwh) > :tol * GREATEST(1.0, ABS(t.mwh))
ORDER BY 1, 2, 3, 4
"""


FUEL_REPORT_SQL = f"""
WITH af AS (
    SELECT to_char({LOCAL_TS}, 'YYYY-MM') AS month, network_code, fueltech_group, direction,
           SUM(energy_mwh) AS mwh, COUNT(DISTINCT unit_code) AS units
    FROM ingestion.oe_unit_generation_all_fuels
    WHERE ({LOCAL_TS})::date BETWEEN :start AND :end
    GROUP BY 1, 2, 3, 4
), fossil AS (
    SELECT to_char({LOCAL_TS}, 'YYYY-MM') AS month, network_code,
           CASE WHEN fueltech LIKE 'coal%' THEN 'coal' WHEN fueltech LIKE 'gas%' THEN 'gas' ELSE fueltech END AS fueltech_group,
           SUM(generation_mwh) AS facility_table_mwh
    FROM ingestion.oe_facility_generation_data
    WHERE ({LOCAL_TS})::date BETWEEN :start AND :end
    GROUP BY 1, 2, 3
)
SELECT af.month, af.network_code, af.fueltech_group, af.direction, af.mwh, af.units,
       CASE WHEN af.direction = 'injection' THEN f.facility_table_mwh END AS facility_table_mwh
FROM af LEFT JOIN fossil f
  ON f.month = af.month AND f.network_code = af.network_code AND f.fueltech_group = af.fueltech_group
ORDER BY 1, 2, 3, 4
"""


def fuel_report(conn, start: date, end: date, out_path: str) -> int:
    _utc(conn)
    rows = (
        conn.execute(text(FUEL_REPORT_SQL), {"start": start, "end": end})
        .mappings()
        .all()
    )
    with open(out_path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "month",
                "network_code",
                "fueltech_group",
                "direction",
                "mwh",
                "units",
                "facility_table_mwh",
            ]
        )
        for r in rows:
            w.writerow(
                [
                    r["month"],
                    r["network_code"],
                    r["fueltech_group"],
                    r["direction"],
                    r["mwh"],
                    r["units"],
                    r["facility_table_mwh"],
                ]
            )
    return len(rows)


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    cmd, args = argv[0], argv[1:]
    try:
        if cmd == "compare":
            a, b = (json.load(open(p)) for p in args[:2])
            close = lambda x, y: abs(x - y) <= 1e-9 * max(1.0, abs(x))  # noqa: E731
            same = (
                a["rows"] == b["rows"]
                and a["md5"] == b["md5"]
                and close(a["generation_mwh"], b["generation_mwh"])
                and a["months"].keys() == b["months"].keys()
                and all(
                    a["months"][m]["rows"] == b["months"][m]["rows"]
                    and close(
                        a["months"][m]["generation_mwh"],
                        b["months"][m]["generation_mwh"],
                    )
                    for m in a["months"]
                )
            )
            (logger.success if same else logger.error)(
                f"OE facility table fingerprint {'unchanged' if same else 'CHANGED'}: "
                f"{a['rows']} → {b['rows']} rows, md5 {a['md5']} → {b['md5']}"
            )
            return 0 if same else 1
        with engine().connect() as conn:
            if cmd == "fingerprint":
                fp = fingerprint(conn)
                with open(args[0], "w") as fh:
                    json.dump(fp, fh, indent=2)
                logger.info(
                    f"OE facility fingerprint: {fp['rows']} rows, {fp['generation_mwh']:,.1f} MWh, md5 {fp['md5']}"
                )
                return 0
            if cmd == "reconcile":
                problems = reconcile(
                    conn,
                    date.fromisoformat(args[0]),
                    date.fromisoformat(args[1]),
                    args[2:],
                )
                for p in problems:
                    logger.error(p)
                if not problems:
                    logger.success(
                        "reconciled: every planned part ok, every day matches, view matches table"
                    )
                return 1 if problems else 0
            if cmd == "fuel-report":
                n = fuel_report(
                    conn,
                    date.fromisoformat(args[0]),
                    date.fromisoformat(args[1]),
                    args[2],
                )
                logger.info(f"fuel report {args[0]}..{args[1]} → {args[2]}: {n} rows")
                return 0
    except KeyError as e:
        logger.error(f"missing environment variable {e}")
        return 2
    except (IndexError, ValueError, StopIteration) as e:
        logger.error(f"bad arguments: {e}")
        print(__doc__)
        return 2
    except SQLAlchemyError as e:
        logger.error(f"database error: {type(e).__name__}")
        return 2
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
