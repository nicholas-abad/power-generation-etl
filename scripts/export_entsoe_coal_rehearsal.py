"""Export only pinned staging, read-only, for the offline coal repair rehearsal.

Run through the existing staging owner environment wrapper. No credentials are
stored in the export. This is a reconstruction source, not a production backup.
"""

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import sys

import psycopg2
from psycopg2.extensions import parse_dsn

ROOT = Path(__file__).resolve().parents[1]
COLUMNS = (
    "id,extraction_run_id,created_at_ms,country_code,psr_type,plant_name,"
    "fuel_type,data_type,timestamp_ms,generation_mw,resolution_minutes"
)
COAL = "psr_type IN ('B02','B03','B05')"
RAW = "ingestion.entsoe_generation_data"
QUERIES = {
    "current2019coal": f"SELECT {COLUMNS} FROM {RAW} WHERE country_code='CZ' AND {COAL} AND timestamp_ms>=1546300800000 AND timestamp_ms<1577836800000 ORDER BY id",
    "current2024fossil": f"SELECT {COLUMNS} FROM {RAW} WHERE country_code='CZ' AND psr_type IN ('B02','B03','B04','B05') AND timestamp_ms>=1704067200000 AND timestamp_ms<1735689600000 ORDER BY id",
    "original2024fossil": f"SELECT {COLUMNS} FROM ingestion.entsoe_cz_2024_repair_backup ORDER BY id",
    "boundary2023": f"SELECT {COLUMNS} FROM {RAW} WHERE country_code='CZ' AND {COAL} AND timestamp_ms=1704063600000 ORDER BY id",
    "controls2025coal": f"SELECT {COLUMNS} FROM {RAW} WHERE country_code='CZ' AND {COAL} AND timestamp_ms>=1735689600000 AND timestamp_ms<1767225600000 ORDER BY id",
    "controls2024pl": f"SELECT {COLUMNS} FROM {RAW} WHERE country_code='PL' AND timestamp_ms>=1704067200000 AND timestamp_ms<1704153600000 ORDER BY id",
    "frontend_crosswalk": f"""SELECT c.plant_name,c.plant_code,c.gem_location_id,
        EXISTS(SELECT 1 FROM gem_units u WHERE u.gem_location_id=c.gem_location_id AND u.tracker='GCPT') AS gcpt_included
        FROM plant_crosswalk c WHERE c.source_system='ENTSOE' AND
        coalesce(c.plant_code,c.plant_name) IN (
            SELECT DISTINCT plant_name FROM {RAW} WHERE country_code='CZ' AND {COAL}
            AND timestamp_ms>=1704067200000 AND timestamp_ms<1735689600000)
        ORDER BY c.plant_name,c.plant_code,c.gem_location_id""",
}


def staging_dsn():
    config = json.loads((ROOT / "config/environments.json").read_text())
    expected = config["environments"]["staging"]
    dsn = os.environ.get("DATABASE_URL", "")
    parts = parse_dsn(dsn)
    if (
        os.environ.get("ETL_ENVIRONMENT") != "staging"
        or os.environ.get("NEON_BRANCH_ID") != expected["branch_id"]
        or os.environ.get("NEON_ENDPOINT_ID") != expected["endpoint_id"]
        or parts.get("host") != f"{expected['endpoint_id']}.{config['proxy_host']}"
        or parts.get("user") != config["roles"]["owner"]
        or parts.get("dbname") != config["database"]
        or parts.get("sslmode") not in {"require", "verify-full"}
        or any(k in parts for k in ("hostaddr", "service"))
        or any(os.environ.get(k) for k in ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE"))
    ):
        raise ValueError("Export requires the pinned staging direct owner wrapper")
    return dsn, expected


def export(destination):
    dsn, target = staging_dsn()
    destination.mkdir(parents=True, exist_ok=False)
    report = {
        "scope": "read-only staging; old-schema column projection",
        **target,
        "files": {},
    }
    with psycopg2.connect(dsn, connect_timeout=15) as connection:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL TIME ZONE 'UTC'")
            cursor.execute("SET LOCAL statement_timeout='300s'")
            cursor.execute(
                "SELECT current_timestamp,version(),current_setting('transaction_read_only')"
            )
            timestamp, version, readonly = cursor.fetchone()
            if readonly != "on":
                raise ValueError("Export transaction must be read-only")
            report.update(
                snapshot_at=timestamp.isoformat(),
                postgres=version,
                transaction_read_only=readonly,
            )
            for label, query in QUERIES.items():
                path = destination / f"{label}.csv.gz"
                with path.open("wb") as raw:
                    with gzip.GzipFile(
                        fileobj=raw, mode="wb", filename="", mtime=0
                    ) as compressed:
                        with io.TextIOWrapper(
                            compressed, encoding="utf-8", newline=""
                        ) as output:
                            cursor.copy_expert(
                                f"COPY ({query}) TO STDOUT WITH CSV HEADER", output
                            )
                with gzip.open(path, "rt", newline="") as source:
                    count = sum(1 for _ in csv.DictReader(source))
                report["files"][label] = {
                    "path": path.name,
                    "rows": count,
                    "query": query,
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
                print(f"Exported {label}: {count:,} rows", flush=True)
    (destination / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        export(args.output)
    except psycopg2.Error as error:
        # Connection failures can contain credentials; do not echo their text.
        print(f"Staging export failed: {type(error).__name__}", file=sys.stderr)
        sys.exit(1)
