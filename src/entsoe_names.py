"""The single reviewed ENTSO-E unit identity table (config/entsoe-unit-names.json).

One entry per (country, unit EIC): the canonical `plant_name` written to the
database, the reviewed `source_names` the source has used for that EIC, and the
`legacy_names` its rows replace in a rebuild. The loader, the coal guards, the
repair and the crosswalk all read identities from here.
"""

from functools import lru_cache
import json
from pathlib import Path

PATH = Path(__file__).resolve().parents[1] / "config/entsoe-unit-names.json"


@lru_cache(maxsize=1)
def units():
    """{(country, unit_eic): entry}, validated."""
    result, names = {}, {}
    for entry in json.loads(PATH.read_text())["units"]:
        key = (entry["country_code"], entry["unit_eic"])
        if key in result:
            raise ValueError(f"Repeated ENTSO-E unit identity {key}")
        if entry["plant_name"] not in entry["legacy_names"] + entry["source_names"]:
            raise ValueError(
                f"Canonical name of {key} is neither a source nor a legacy name"
            )
        name_key = (entry["country_code"], entry["psr_type"], entry["plant_name"])
        if name_key in names:
            raise ValueError(
                f"{key} and {names[name_key]} share the canonical name {name_key}"
            )
        names[name_key] = key
        result[key] = entry
    return result


def aliases():
    """Source→canonical name pairs, the shape the coal guards and repair use."""
    return [
        {
            "country_code": e["country_code"],
            "unit_eic": e["unit_eic"],
            "psr_type": e["psr_type"],
            "source_name": source,
            "legacy_name": e["plant_name"],
        }
        for e in units().values()
        for source in e["source_names"]
        if source != e["plant_name"]
    ]
