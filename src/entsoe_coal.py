"""ENTSO-E coal compatibility guards for the existing name-keyed schema.

They apply to the countries in config/entsoe-coal-scopes.json, whose coal the
extractor emits as per-Period source records.
"""

import json
from pathlib import Path
import re

from psycopg2.extras import execute_values

CONFIG = Path(__file__).resolve().parents[1] / "config"
_SCOPES = json.loads((CONFIG / "entsoe-coal-scopes.json").read_text())
COAL = frozenset(_SCOPES["psr_types"])
COUNTRIES = frozenset(_SCOPES["countries"])
ALIASES = json.loads((CONFIG / "entsoe-coal-aliases.json").read_text())["aliases"]
_ALIASES_BY_COUNTRY: dict = {}
for _alias in ALIASES:
    _ALIASES_BY_COUNTRY.setdefault(_alias["country_code"], []).append(_alias)


def aliases_for(country):
    return _ALIASES_BY_COUNTRY.get(country, [])


def in_scope(row):
    return row.get("country_code") in COUNTRIES and row.get("psr_type") in COAL


def source_coal(row):
    return in_scope(row) and row.get("resolution_source") == "xml_period"


def check_source_scope(row):
    """Reject source-period coal from a country outside the configured scopes.

    Called on each record as soon as it is read, before legacy name cleanup
    can drop it, so an extractor/ETL scope mismatch fails the load instead of
    losing rows.
    """
    if (
        row.get("psr_type") in COAL
        and row.get("country_code") not in COUNTRIES
        and row.get("resolution_source") is not None
    ):
        raise ValueError(
            f"Source-period coal for {row.get('country_code')} is outside the "
            "configured scopes (config/entsoe-coal-scopes.json)"
        )


def check_coal_records(records):
    """Reject conflicting copies before the generic validator deduplicates them.

    The optional source marker is provenance, not an authentication mechanism.
    Old files remain loadable, but cannot change an existing interval duration.
    Source-period coal outside the configured scopes is rejected.
    """
    seen = {}
    for row in records:
        check_source_scope(row)
        if not in_scope(row):
            continue
        country = row["country_code"]
        marker = row.get("resolution_source")
        if marker not in {None, "xml_period"}:
            raise ValueError(f"Unknown {country} coal interval provenance")
        if marker == "xml_period" and not row.get("unit_eic"):
            raise ValueError("Source-period coal records require their source unit EIC")
        if marker == "xml_period" and (
            not re.fullmatch(r"[A-Z0-9-]{16}", row["unit_eic"])
            or row.get("data_type") != "Actual Aggregated"
        ):
            raise ValueError("Invalid source-period coal identity or metric")
        for alias in aliases_for(country):
            if row.get("unit_eic") == alias["unit_eic"] and (
                row.get("psr_type") != alias["psr_type"]
                or row.get("plant_name")
                not in {alias["source_name"], alias["legacy_name"]}
            ):
                raise ValueError(
                    f"Known {country} coal EIC changed name or fuel; review required"
                )
            if row.get("psr_type") == alias["psr_type"] and row.get("plant_name") in {
                alias["source_name"],
                alias["legacy_name"],
            }:
                if row.get("unit_eic") and row["unit_eic"] != alias["unit_eic"]:
                    raise ValueError(
                        f"{country} coal name does not match its reviewed EIC"
                    )
        key = (
            row.get("timestamp_ms"),
            country,
            row.get("psr_type"),
            row.get("plant_name"),
        )
        value = (row.get("generation_mw"), row.get("resolution_minutes"))
        if key in seen and seen[key] != value:
            raise ValueError(f"Conflicting {country} coal measurements in input")
        seen[key] = value


def prepare_coal_upsert(cursor):
    """Normalize reviewed aliases and protect durations in the write transaction.

    The table lock serializes name resolution with insertion, including writers
    that do not use an advisory lock. Both existing names is an error requiring
    an audited repair; this function never deletes historical observations.
    """
    scope = (sorted(COUNTRIES), sorted(COAL))
    cursor.execute(
        """SELECT EXISTS (SELECT 1 FROM _staging_entsoe_generation_data
        WHERE country_code = ANY(%s) AND psr_type = ANY(%s))""",
        scope,
    )
    if not cursor.fetchone()[0]:
        return
    cursor.execute("SET LOCAL lock_timeout = '10s'")
    cursor.execute("LOCK TABLE entsoe_generation_data IN SHARE ROW EXCLUSIVE MODE")
    cursor.execute("""CREATE TEMP TABLE _coal_aliases (
        country text, psr text, legacy text, source text
    ) ON COMMIT DROP""")
    rows = [
        (a["country_code"], a["psr_type"], a["legacy_name"], a["source_name"])
        for a in ALIASES
        if a["country_code"] in COUNTRIES
    ]
    if rows:
        execute_values(cursor, "INSERT INTO _coal_aliases VALUES %s", rows)
    cursor.execute("""CREATE TEMP TABLE _coal_names ON COMMIT DROP AS
        SELECT DISTINCT s.timestamp_ms, s.country_code, s.psr_type,
            s.plant_name AS incoming, aliases.legacy, a.plant_name AS existing
        FROM _staging_entsoe_generation_data s
        JOIN _coal_aliases aliases ON s.country_code=aliases.country
            AND s.psr_type=aliases.psr
            AND s.plant_name IN (aliases.legacy, aliases.source)
        LEFT JOIN entsoe_generation_data a ON a.country_code=s.country_code
            AND a.timestamp_ms=s.timestamp_ms AND a.psr_type=s.psr_type
            AND a.plant_name IN (aliases.legacy, aliases.source)""")
    cursor.execute("""SELECT country_code FROM _coal_names
        GROUP BY timestamp_ms, country_code, psr_type, incoming
        HAVING count(*)>1 LIMIT 1""")
    found = cursor.fetchone()
    if found:
        raise ValueError(
            f"Existing {found[0]} coal aliases overlap; an audited repair is required"
        )
    # An unverified row renamed onto the other alias's stored row must agree on
    # MW; otherwise a conflicting copy split across batches would silently
    # overwrite the stored value. Source-period rows may revise MW.
    cursor.execute("""SELECT s.country_code FROM _staging_entsoe_generation_data s
        JOIN _coal_names n ON s.country_code=n.country_code
          AND s.timestamp_ms=n.timestamp_ms
          AND s.psr_type=n.psr_type AND s.plant_name=n.incoming
        JOIN entsoe_generation_data a ON a.country_code=s.country_code
          AND a.timestamp_ms=s.timestamp_ms AND a.psr_type=s.psr_type
          AND a.plant_name=n.existing
        WHERE n.existing IS NOT NULL AND n.existing<>n.incoming
          AND NOT s._source_resolution_verified
          AND a.generation_mw IS DISTINCT FROM s.generation_mw LIMIT 1""")
    found = cursor.fetchone()
    if found:
        raise ValueError(
            f"Conflicting {found[0]} coal alias measurement; regenerate from source XML"
        )
    cursor.execute("""UPDATE _staging_entsoe_generation_data s
        SET plant_name=COALESCE(n.existing,n.legacy)
        FROM _coal_names n WHERE s.country_code=n.country_code
          AND s.timestamp_ms=n.timestamp_ms
          AND s.psr_type=n.psr_type AND s.plant_name=n.incoming""")
    cursor.execute(
        """SELECT country_code FROM _staging_entsoe_generation_data
        WHERE country_code = ANY(%s) AND psr_type = ANY(%s)
        GROUP BY timestamp_ms, country_code, psr_type, plant_name
        HAVING count(DISTINCT (generation_mw,resolution_minutes,fuel_type))>1
        LIMIT 1""",
        scope,
    )
    found = cursor.fetchone()
    if found:
        raise ValueError(f"Conflicting {found[0]} coal aliases in input")
    # Rows already carrying their unit EIC (migration 018) keep their stored
    # duration on a legacy replay (the upsert's identified-metadata guard), so
    # only rows without an EIC are checked; to_jsonb works with or without 018.
    cursor.execute(
        """SELECT s.country_code FROM _staging_entsoe_generation_data s
        JOIN entsoe_generation_data a USING (timestamp_ms,country_code,psr_type,plant_name)
        WHERE s.country_code = ANY(%s) AND s.psr_type = ANY(%s)
          AND NOT s._source_resolution_verified
          AND to_jsonb(a)->>'unit_eic' IS NULL
          AND a.resolution_minutes IS DISTINCT FROM s.resolution_minutes LIMIT 1""",
        scope,
    )
    found = cursor.fetchone()
    if found:
        raise ValueError(
            f"Legacy {found[0]} coal interval mismatch; regenerate from source XML"
        )
