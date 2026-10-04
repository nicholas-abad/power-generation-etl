"""Czech coal compatibility guards for the existing name-keyed schema."""

import json
from pathlib import Path
import re

from psycopg2.extras import execute_values

COAL = frozenset({"B02", "B03", "B05"})
ALIASES = json.loads(
    (
        Path(__file__).resolve().parents[1] / "config/entsoe-coal-aliases.json"
    ).read_text()
)["aliases"]


def source_coal(row):
    return (
        row.get("country_code") == "CZ"
        and row.get("psr_type") in COAL
        and row.get("resolution_source") == "xml_period"
    )


def check_coal_records(records):
    """Reject conflicting copies before the generic validator deduplicates them.

    The optional source marker is provenance, not an authentication mechanism.
    Old files remain loadable, but cannot change an existing interval duration.
    """
    seen = {}
    for row in records:
        if row.get("country_code") != "CZ" or row.get("psr_type") not in COAL:
            continue
        marker = row.get("resolution_source")
        if marker not in {None, "xml_period"}:
            raise ValueError("Unknown Czech coal interval provenance")
        if marker == "xml_period" and not row.get("unit_eic"):
            raise ValueError("Source-period coal records require their source unit EIC")
        if marker == "xml_period" and (
            not re.fullmatch(r"[A-Z0-9-]{16}", row["unit_eic"])
            or row.get("data_type") != "Actual Aggregated"
        ):
            raise ValueError("Invalid source-period coal identity or metric")
        for alias in ALIASES:
            if row.get("unit_eic") == alias["unit_eic"] and (
                row.get("psr_type") != alias["psr_type"]
                or row.get("plant_name")
                not in {alias["source_name"], alias["legacy_name"]}
            ):
                raise ValueError(
                    "Known Czech coal EIC changed name or fuel; review required"
                )
            if row.get("psr_type") == alias["psr_type"] and row.get("plant_name") in {
                alias["source_name"],
                alias["legacy_name"],
            }:
                if row.get("unit_eic") and row["unit_eic"] != alias["unit_eic"]:
                    raise ValueError("Czech coal name does not match its reviewed EIC")
        key = (row.get("timestamp_ms"), row.get("psr_type"), row.get("plant_name"))
        value = (row.get("generation_mw"), row.get("resolution_minutes"))
        if key in seen and seen[key] != value:
            raise ValueError("Conflicting Czech coal measurements in input")
        seen[key] = value


def prepare_coal_upsert(cursor):
    """Normalize reviewed aliases and protect durations in the write transaction.

    The table lock serializes name resolution with insertion, including writers
    that do not use an advisory lock. Both existing names is an error requiring
    an audited repair; this function never deletes historical observations.
    """
    cursor.execute("""SELECT EXISTS (SELECT 1 FROM _staging_entsoe_generation_data
        WHERE country_code='CZ' AND psr_type IN ('B02','B03','B05'))""")
    if not cursor.fetchone()[0]:
        return
    cursor.execute("SET LOCAL lock_timeout = '10s'")
    cursor.execute("LOCK TABLE entsoe_generation_data IN SHARE ROW EXCLUSIVE MODE")
    cursor.execute("""CREATE TEMP TABLE _coal_aliases (
        psr text, legacy text, source text
    ) ON COMMIT DROP""")
    execute_values(
        cursor,
        "INSERT INTO _coal_aliases VALUES %s",
        [(a["psr_type"], a["legacy_name"], a["source_name"]) for a in ALIASES],
    )
    cursor.execute("""CREATE TEMP TABLE _coal_names ON COMMIT DROP AS
        SELECT DISTINCT s.timestamp_ms, s.psr_type, s.plant_name AS incoming,
            aliases.legacy, a.plant_name AS existing
        FROM _staging_entsoe_generation_data s
        JOIN _coal_aliases aliases ON s.country_code='CZ' AND s.psr_type=aliases.psr
            AND s.plant_name IN (aliases.legacy, aliases.source)
        LEFT JOIN entsoe_generation_data a ON a.country_code='CZ'
            AND a.timestamp_ms=s.timestamp_ms AND a.psr_type=s.psr_type
            AND a.plant_name IN (aliases.legacy, aliases.source)""")
    cursor.execute("""SELECT 1 FROM _coal_names
        GROUP BY timestamp_ms, psr_type, incoming HAVING count(*)>1 LIMIT 1""")
    if cursor.fetchone():
        raise ValueError(
            "Existing Czech coal aliases overlap; an audited repair is required"
        )
    cursor.execute("""UPDATE _staging_entsoe_generation_data s
        SET plant_name=COALESCE(n.existing,n.legacy)
        FROM _coal_names n WHERE s.country_code='CZ' AND s.timestamp_ms=n.timestamp_ms
          AND s.psr_type=n.psr_type AND s.plant_name=n.incoming""")
    cursor.execute("""SELECT 1 FROM _staging_entsoe_generation_data
        WHERE country_code='CZ' AND psr_type IN ('B02','B03','B05')
        GROUP BY timestamp_ms, psr_type, plant_name
        HAVING count(DISTINCT (generation_mw,resolution_minutes,fuel_type))>1 LIMIT 1""")
    if cursor.fetchone():
        raise ValueError("Conflicting Czech coal aliases in input")
    cursor.execute("""SELECT 1 FROM _staging_entsoe_generation_data s
        JOIN entsoe_generation_data a USING (timestamp_ms,country_code,psr_type,plant_name)
        WHERE s.country_code='CZ' AND s.psr_type IN ('B02','B03','B05')
          AND NOT s._source_resolution_verified
          AND a.resolution_minutes IS DISTINCT FROM s.resolution_minutes LIMIT 1""")
    if cursor.fetchone():
        raise ValueError(
            "Legacy Czech coal interval mismatch; regenerate from source XML"
        )
