"""Source unit identities to database names, via the single reviewed table."""

from entsoe_names import units


def prepare_record(record):
    """Keep the exact source name; write the reviewed canonical name.

    A unit in config/entsoe-unit-names.json must arrive under one of its
    reviewed source names and PSR type; it is stored under its canonical name.
    Any other unit keeps its literal source name.
    """
    row = dict(record)
    source_name = row.get("source_unit_name", row["plant_name"])
    entry = units().get((row["country_code"], row["unit_eic"]))
    if entry:
        if (
            source_name not in entry["source_names"]
            or row["psr_type"] != entry["psr_type"]
        ):
            raise ValueError("Known EIC has an unreviewed source name or PSR type")
        if row["plant_name"] not in set(entry["source_names"]) | {entry["plant_name"]}:
            raise ValueError("Unit name does not match its reviewed identity")
        row["plant_name"] = entry["plant_name"]
    elif row["plant_name"] != source_name:
        raise ValueError("Unreviewed unit-name alias")
    row["source_unit_name"] = source_name
    row.setdefault("production_unit_eic", None)
    return row
