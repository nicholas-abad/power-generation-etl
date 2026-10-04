"""Reconcile the CZ 2019/2024 identified-unit pilot, on staging only.

Run a source preflight BEFORE loading. It checks archived response hashes,
request coverage, input identities, existing fossil values and coal coverage.
The verification phase compares every observation and monthly unit group.
"""

import argparse
from collections import Counter
import csv
from datetime import UTC, datetime, timedelta
import hashlib
import io
import json
import math
import os
from pathlib import Path

import psycopg2

from validator import DataValidator
from entsoe_identity import prepare_record

ROOT = Path(__file__).resolve().parents[1]
SOURCE_COLUMNS = (
    "timestamp_ms",
    "country_code",
    "unit_eic",
    "production_unit_eic",
    "plant_name",
    "psr_type",
    "fuel_type",
    "data_type",
    "generation_mw",
    "resolution_minutes",
)
COLUMNS = SOURCE_COLUMNS + ("source_unit_name",)
COAL = "psr_type IN ('B02','B03','B05')"
MEASUREMENTS = "timestamp_ms,country_code,psr_type,plant_name,fuel_type,generation_mw,resolution_minutes"
PSRS = {f"B{i:02d}" for i in range(1, 21)} | {"B25"}


def bounds(year, month=None):
    if year not in (2019, 2024) or (month is not None and month not in range(1, 13)):
        raise ValueError("Only the CZ 2019/2024 pilot is audited")
    start = datetime(year, month or 1, 1, tzinfo=UTC)
    end = (
        datetime(year + 1, 1, 1, tzinfo=UTC)
        if month is None or month == 12
        else datetime(year, month + 1, 1, tzinfo=UTC)
    )
    return start, end


def predicate(year, month=None):
    start, end = bounds(year, month)
    return (
        f"country_code='CZ' AND timestamp_ms >= {int(start.timestamp() * 1000)} "
        f"AND timestamp_ms < {int(end.timestamp() * 1000)}"
    )


def digest(rows):
    result = hashlib.sha256()
    for row in sorted(
        rows, key=lambda r: (r["timestamp_ms"], r["country_code"], r["unit_eic"])
    ):
        result.update(
            (
                json.dumps(
                    [row.get(c) for c in SOURCE_COLUMNS],
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                + "\n"
            ).encode()
        )
    return result.hexdigest()


def check_benchmark(report, year):
    """Reject a coherently regenerated manifest if the audited source changed."""
    audited = json.loads((ROOT / "config/entsoe-benchmarks.json").read_text())
    expected = audited["benchmarks"][str(year)]
    for field in ("content_sha256", "observations", "unit_count", "days"):
        if report[field] != expected[field]:
            raise ValueError(f"Audited CZ {year} benchmark changed: {field}")


def check_source(path, manifest_path, year, month=None):
    start, end = bounds(year, month)
    manifest = json.loads(manifest_path.read_text())
    expected_months = [month] if month else list(range(1, 13))
    if (
        manifest.get("identified_units") is not True
        or manifest.get("timezone") != "UTC"
        or manifest.get("country_codes") != ["CZ"]
        or manifest.get("months") != expected_months
        or manifest.get("start_year") != year
        or manifest.get("end_year") != year
        or set(manifest.get("psr_types", [])) != PSRS
    ):
        raise ValueError("Input is not the requested complete UTC CZ all-fuel scope")
    with path.open() as handle:
        rows = [json.loads(line) for line in handle if line.strip()]
    validator = DataValidator()
    keys, legacy_keys, days, by_day = set(), set(), set(), {}
    for row in rows:
        result = validator.validate_entsoe_record(row)
        if not result.valid:
            raise ValueError(f"Invalid observation: {result.errors}")
        timestamp = datetime.fromtimestamp(row["timestamp_ms"] / 1000, UTC)
        if not start <= timestamp < end or row["country_code"] != "CZ":
            raise ValueError("Observation outside the requested scope")
        if row.get("extraction_run_id") != manifest["extraction_run_id"]:
            raise ValueError("Observation run ID differs from the source manifest")
        key = (row["timestamp_ms"], row["unit_eic"])
        legacy = (row["timestamp_ms"], row["plant_name"], row["psr_type"])
        if key in keys or legacy in legacy_keys:
            raise ValueError("Duplicate unit key or conflicting legacy name key")
        keys.add(key)
        legacy_keys.add(legacy)
        day = timestamp.date().isoformat()
        days.add(day)
        by_day.setdefault(day, []).append(row)
    expected_days = {
        (start + timedelta(days=n)).date().isoformat()
        for n in range((end - start).days)
    }
    if days != expected_days:
        raise ValueError("Some calendar days have no generation observations")
    requests = manifest.get("source_requests", [])
    request_days, labels = {}, set()
    for request in requests:
        first = datetime.fromisoformat(request["start"])
        last = datetime.fromisoformat(request["end"])
        day = first.date().isoformat()
        if (
            request.get("country") != "CZ"
            or first.hour != 0
            or first.minute != 0
            or first.utcoffset() != timedelta(0)
            or last - first != timedelta(days=1)
            or first < start
            or last > end
            or request["label"] in labels
        ):
            raise ValueError("Invalid or repeated source request window")
        labels.add(request["label"])
        request_days.setdefault(day, []).append(request.get("psr_type"))
        selected = [
            r
            for r in by_day[day]
            if request.get("psr_type") is None or r["psr_type"] == request["psr_type"]
        ]
        if request.get("status") == "no_data":
            if selected:
                raise ValueError("No-data request has extracted observations")
            continue
        if request.get("status") != "success":
            raise ValueError("Failed source request")
        raw_path = (manifest_path.parent / request["path"]).resolve()
        if not raw_path.is_relative_to(manifest_path.parent.resolve()):
            raise ValueError("Source XML path escapes the archive")
        if hashlib.sha256(raw_path.read_bytes()).hexdigest() != request["sha256"]:
            raise ValueError("Archived XML differs from its recorded source hash")
        if (
            len(selected) != request["observations"]
            or digest(selected) != request["content_sha256"]
        ):
            raise ValueError(
                "Input differs from the per-request source observation digest"
            )
    if set(request_days) != expected_days:
        raise ValueError("Missing source request days")
    for requests_for_day in request_days.values():
        if requests_for_day != [None] and (
            len(requests_for_day) != len(PSRS) or set(requests_for_day) != PSRS
        ):
            raise ValueError("Incomplete all-fuel request or per-PSR fallback")
    total_mwh = math.fsum(
        r["generation_mw"] * r["resolution_minutes"] / 60 for r in rows
    )
    if (
        len(rows) != manifest["total_records"]
        or digest(rows) != manifest["content_sha256"]
        or dict(Counter(r["fuel_type"] for r in rows)) != manifest["fuel_counts"]
        or len({r["unit_eic"] for r in rows}) != manifest["unit_count"]
        or not math.isclose(
            total_mwh, manifest["generation_mwh"], rel_tol=1e-12, abs_tol=1e-6
        )
    ):
        raise ValueError("Extracted observations do not match the source manifest")
    return rows, {
        "observations": len(rows),
        "unit_count": manifest["unit_count"],
        "fuel_counts": manifest["fuel_counts"],
        "generation_mwh": total_mwh,
        "days": len(days),
        "content_sha256": manifest["content_sha256"],
        "source_requests": len(requests),
        "extraction_run_id": manifest["extraction_run_id"],
    }


def load_expected(cursor, rows):
    cursor.execute("""CREATE TEMP TABLE entsoe_expected (
        timestamp_ms bigint, country_code text, unit_eic text, production_unit_eic text,
        plant_name text, psr_type text, fuel_type text, data_type text,
        generation_mw double precision, resolution_minutes integer, source_unit_name text) ON COMMIT DROP""")
    for offset in range(0, len(rows), 50000):
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerows(
            [r.get(c) for c in COLUMNS] for r in rows[offset : offset + 50000]
        )
        buffer.seek(0)
        cursor.copy_expert(
            f"COPY entsoe_expected ({','.join(COLUMNS)}) FROM STDIN WITH CSV", buffer
        )
    cursor.execute(
        "CREATE UNIQUE INDEX ON entsoe_expected (timestamp_ms,country_code,psr_type,plant_name)"
    )
    cursor.execute("ANALYZE entsoe_expected")
    cursor.execute("""SELECT count(*) FROM (
        SELECT timestamp_ms, lag(timestamp_ms + resolution_minutes*60000)
        OVER (PARTITION BY country_code,unit_eic ORDER BY timestamp_ms) AS previous_end
        FROM entsoe_expected) e WHERE timestamp_ms < previous_end""")
    if cursor.fetchone()[0]:
        raise ValueError("Overlapping source intervals")


def capture(cursor, year, month=None):
    buffer = io.StringIO()
    cursor.copy_expert(
        f"COPY (SELECT {MEASUREMENTS} FROM ingestion.entsoe_generation_data WHERE {predicate(year, month)} "
        f"AND {COAL} ORDER BY timestamp_ms,country_code,psr_type,plant_name) TO STDOUT WITH CSV",
        buffer,
    )
    cursor.execute(
        f"SELECT count(*) FROM ingestion.entsoe_generation_data WHERE {predicate(year, month)} AND {COAL}"
    )
    count = cursor.fetchone()[0]
    if not count:
        raise ValueError("No existing coal observations to benchmark")
    cursor.execute(f"""SELECT date_trunc('month',to_timestamp(timestamp_ms/1000.0))::text,
        sum(generation_mw*resolution_minutes/60.0) FROM ingestion.entsoe_generation_data
        WHERE {predicate(year, month)} AND {COAL} GROUP BY 1 ORDER BY 1""")
    return {
        "coal_rows": count,
        "coal_sha256": hashlib.sha256(buffer.getvalue().encode()).hexdigest(),
        "coal_monthly": cursor.fetchall(),
    }


JOIN = "e.timestamp_ms=a.timestamp_ms AND e.country_code=a.country_code AND e.psr_type=a.psr_type AND e.plant_name=a.plant_name"


def preflight(cursor, year, month=None):
    scope = (
        predicate(year, month)
        .replace("country_code=", "a.country_code=")
        .replace("timestamp_ms", "a.timestamp_ms")
    )
    cursor.execute(f"""SELECT count(*) FROM ingestion.entsoe_generation_data a
        LEFT JOIN entsoe_expected e ON {JOIN}
        WHERE {predicate(year, month).replace("country_code=", "a.country_code=").replace("timestamp_ms", "a.timestamp_ms")}
        AND a.{COAL} AND e.timestamp_ms IS NULL""")
    missing_coal = cursor.fetchone()[0]
    cursor.execute(f"""SELECT count(*) FROM ingestion.entsoe_generation_data a
        LEFT JOIN entsoe_expected e ON {JOIN}
        WHERE {predicate(year, month).replace("country_code=", "a.country_code=").replace("timestamp_ms", "a.timestamp_ms")}
        AND e.timestamp_ms IS NULL""")
    missing_any = cursor.fetchone()[0]
    cursor.execute(f"""SELECT count(*) FROM entsoe_expected e JOIN ingestion.entsoe_generation_data a ON {JOIN}
        WHERE {scope} AND (abs(e.generation_mw-a.generation_mw)>1e-9 OR e.resolution_minutes<>a.resolution_minutes
        OR e.fuel_type<>a.fuel_type)""")
    differences = cursor.fetchone()[0]
    cursor.execute(f"""SELECT count(*) FROM entsoe_expected e JOIN ingestion.entsoe_generation_data a ON {JOIN}
        WHERE {scope} AND ((a.unit_eic IS NOT NULL AND a.unit_eic IS DISTINCT FROM e.unit_eic)
        OR (a.production_unit_eic IS NOT NULL AND a.production_unit_eic IS DISTINCT FROM e.production_unit_eic))""")
    changed_identity = cursor.fetchone()[0]
    cursor.execute(f"""SELECT count(*) FROM entsoe_expected e JOIN ingestion.entsoe_generation_data a
        ON e.country_code=a.country_code AND e.unit_eic=a.unit_eic AND e.timestamp_ms=a.timestamp_ms
        WHERE {scope} AND (e.plant_name IS DISTINCT FROM a.plant_name OR e.psr_type IS DISTINCT FROM a.psr_type)""")
    duplicate_identity = cursor.fetchone()[0]
    result = {
        "missing_existing_coal_observations": missing_coal,
        "missing_existing_observations": missing_any,
        "changed_existing_observations": differences,
        "changed_existing_identities": changed_identity,
        "duplicate_unit_identities": duplicate_identity,
    }
    if any(result.values()):
        if missing_any:
            cursor.execute(f"""SELECT a.timestamp_ms,a.plant_name,a.psr_type,a.generation_mw,a.resolution_minutes
                FROM ingestion.entsoe_generation_data a LEFT JOIN entsoe_expected e ON {JOIN}
                WHERE {scope} AND e.timestamp_ms IS NULL
                ORDER BY a.timestamp_ms,a.plant_name LIMIT 25""")
            result["missing_examples"] = cursor.fetchall()
        if differences:
            cursor.execute(f"""SELECT a.timestamp_ms,a.plant_name,a.psr_type,
                a.generation_mw,e.generation_mw,a.resolution_minutes,e.resolution_minutes
                FROM entsoe_expected e JOIN ingestion.entsoe_generation_data a ON {JOIN}
                WHERE {scope} AND (abs(e.generation_mw-a.generation_mw)>1e-9
                OR e.resolution_minutes<>a.resolution_minutes OR e.fuel_type<>a.fuel_type)
                ORDER BY a.timestamp_ms,a.plant_name LIMIT 25""")
            result["changed_examples"] = cursor.fetchall()
        raise ValueError(f"Source comparison failed before loading: {result}")
    return result


def reconcile(cursor, year, month=None):
    scope = (
        predicate(year, month)
        .replace("country_code=", "a.country_code=")
        .replace("timestamp_ms", "a.timestamp_ms")
    )
    fields = [f"e.{c} IS DISTINCT FROM a.{c}" for c in COLUMNS if c != "generation_mw"]
    cursor.execute(
        f"SELECT count(*) FROM entsoe_expected e LEFT JOIN ingestion.entsoe_generation_data a ON {JOIN} AND {scope} WHERE "
        + " OR ".join(
            fields + ["a.id IS NULL", "abs(e.generation_mw-a.generation_mw)>1e-9"]
        )
    )
    if cursor.fetchone()[0]:
        raise ValueError("Stored unit observations differ from source")
    cursor.execute(
        f"SELECT count(*) FROM ingestion.entsoe_generation_data WHERE {predicate(year, month)} AND unit_eic IS NOT NULL"
    )
    actual_count = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM entsoe_expected")
    if actual_count != cursor.fetchone()[0]:
        raise ValueError(
            "Unexpected identified observations outside the extracted population"
        )
    grain = "month,country_code,unit_eic,production_unit_eic,plant_name,source_unit_name,psr_type,fuel_type"
    cursor.execute(
        f"SELECT {grain},sum(generation_mw*resolution_minutes/60.0),count(*),sum(resolution_minutes) "
        "FROM (SELECT date_trunc('month',to_timestamp(timestamp_ms/1000.0)) AS month,* FROM entsoe_expected) e "
        f"GROUP BY {grain}"
    )
    expected = {tuple(r[:8]): r[8:] for r in cursor.fetchall()}
    start, end = bounds(year, month)
    cursor.execute(
        f"SELECT {grain},generation_mwh,observation_count,observed_minutes FROM public.mv_entsoe_unit_monthly "
        "WHERE country_code='CZ' AND month >= %s AND month < %s",
        (start, end),
    )
    actual = {tuple(r[:8]): r[8:] for r in cursor.fetchall()}
    if set(actual) != set(expected):
        raise ValueError("Monthly unit groups differ from source")
    for key, values in expected.items():
        if values[1:] != actual[key][1:] or not math.isclose(
            values[0], actual[key][0], rel_tol=1e-12, abs_tol=1e-6
        ):
            raise ValueError(f"Monthly unit values differ for {key}")
    cursor.execute(f"""SELECT date_trunc('month',to_timestamp(timestamp_ms/1000.0)),
        plant_name,country_code,fuel_type,sum(generation_mw*resolution_minutes/60.0)
        FROM entsoe_expected WHERE {COAL} GROUP BY 1,2,3,4""")
    expected_coal = {tuple(r[:4]): r[4] for r in cursor.fetchall()}
    cursor.execute(
        """SELECT month,plant_name,country_code,fuel_type,generation_mwh
        FROM public.mv_entsoe_plant_monthly WHERE country_code='CZ' AND month >= %s AND month < %s
        AND fuel_type IN ('Fossil Brown coal/Lignite','Fossil Coal-derived gas','Fossil Hard coal')""",
        (start, end),
    )
    actual_coal = {tuple(r[:4]): r[4] for r in cursor.fetchall()}
    if set(actual_coal) != set(expected_coal) or any(
        not math.isclose(value, actual_coal[key], rel_tol=1e-12, abs_tol=1e-6)
        for key, value in expected_coal.items()
    ):
        raise ValueError("Legacy coal monthly view differs from source")
    cursor.execute((ROOT / "schema/checks/dashboard_ro_surface.sql").read_text())
    return {
        "matched_observations": actual_count,
        "matched_monthly_groups": len(actual),
        "matched_legacy_coal_monthly_groups": len(actual_coal),
        "months": sorted({r[0].isoformat() for r in actual}),
        "dashboard_permissions": "unchanged",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True, choices=[2019, 2024])
    parser.add_argument("--month", type=int, choices=range(1, 13))
    parser.add_argument(
        "--phase", required=True, choices=["baseline", "preflight", "verify"]
    )
    parser.add_argument("--input", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--require-audited", action="store_true")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if os.getenv("ETL_ENVIRONMENT") != "staging":
        raise ValueError(
            "This pilot can only run through the staging environment wrapper"
        )
    report = {
        "country": "CZ",
        "year": args.year,
        "month": args.month,
        "phase": args.phase,
    }
    try:
        rows = None
        if args.phase != "baseline":
            if not args.input or not args.manifest:
                raise ValueError("An input file and source manifest are required")
            rows, report["source"] = check_source(
                args.input, args.manifest, args.year, args.month
            )
            if args.require_audited:
                if args.month is not None:
                    raise ValueError("Fixed benchmarks require the full year")
                check_benchmark(report["source"], args.year)
            rows = [prepare_record(row) for row in rows]
        with (
            psycopg2.connect(os.environ["DATABASE_URL"]) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SET TIME ZONE 'UTC'")
            report["coal"] = capture(cursor, args.year, args.month)
            if rows is not None:
                load_expected(cursor, rows)
                report["preflight"] = preflight(cursor, args.year, args.month)
            if args.phase == "verify":
                if not args.baseline:
                    raise ValueError("A pre-load coal baseline is required")
                before = json.loads(args.baseline.read_text())
                if any(
                    before.get(k) != report[k] for k in ("country", "year", "month")
                ):
                    raise ValueError("Coal baseline covers another scope")
                if (
                    before["coal"]["coal_sha256"] != report["coal"]["coal_sha256"]
                    or before["coal"]["coal_rows"] != report["coal"]["coal_rows"]
                ):
                    raise ValueError("Coal measurements changed")
                report["reconciliation"] = reconcile(cursor, args.year, args.month)
        report["status"] = "passed"
    except Exception as exc:
        report.update(status="failed", error=str(exc))
        raise
    finally:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps(report, default=str))


if __name__ == "__main__":
    main()
