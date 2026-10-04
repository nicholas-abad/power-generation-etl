"""Explicit, reviewed bridges from source unit identities to legacy DB names."""

from functools import lru_cache
import json
from pathlib import Path


@lru_cache(maxsize=1)
def aliases():
    path = Path(__file__).resolve().parents[1] / "config/entsoe-unit-aliases.json"
    entries = json.loads(path.read_text())["aliases"]
    result = {}
    for entry in entries:
        key = (entry["country_code"], entry["unit_eic"])
        if key in result:
            raise ValueError("Repeated ENTSO-E alias identity")
        result[key] = entry
    return result


def prepare_record(record):
    """Retain exact source names, while preserving existing dashboard join keys."""
    row = dict(record)
    source_name = row.get("source_unit_name", row["plant_name"])
    entry = aliases().get((row["country_code"], row["unit_eic"]))
    if entry:
        if source_name != entry["source_name"] or row["psr_type"] != entry["psr_type"]:
            raise ValueError("Known EIC has an unreviewed source name or PSR type")
        if row["plant_name"] not in {entry["source_name"], entry["legacy_name"]}:
            raise ValueError("Unit name does not match its explicit alias")
        row["plant_name"] = entry["legacy_name"]
    elif row["plant_name"] != source_name:
        raise ValueError("Unreviewed unit-name alias")
    row["source_unit_name"] = source_name
    row.setdefault("production_unit_eic", None)
    return row
