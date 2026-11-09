"""The reviewed fuel-group list (config/fuel-groups.json) and its database copy.

Each source labels fuels its own way: Chile's Coordinador writes 'Carbón',
'Gas Natural', 'PetCoke'; OpenElectricity writes 'coal_black', 'distillate'.
The extractors keep those labels. This list maps every label to one reporting
group (Coal, Gas, Oil, Other fossil, Hydro, ...) or excludes it from
generation with a reason (batteries for now, pumping load). Example:
('CHILE', 'PetCoke') -> 'other_fossil'; ('OE', 'pumps') -> excluded.

Views read the copy in ingestion.fuel_groups (migration 026), which
``sync`` rewrites from the file before the views that use it are refreshed;
``unmapped`` lists labels in the data that the file does not know, which the
refresh treats as an error rather than letting a join drop the energy.
"""

from functools import lru_cache
import json
from pathlib import Path

from sqlalchemy import text

PATH = Path(__file__).resolve().parents[1] / "config/fuel-groups.json"
TABLE = "ingestion.fuel_groups"
# Source label column of each all-fuel table with a fuel-group view, for
# ``unmapped`` (Australia's is added with its view).
LABEL_COLUMNS = {"CHILE": ("ingestion.chile_generation_all_fuels", "fuel_type")}


def parse(document: dict) -> list[tuple[str, str, str | None, str | None]]:
    """(source, label, group or None, exclusion reason or None) rows; raises on
    an unknown group, a label both grouped and excluded, or an empty reason."""
    groups = document["groups"]
    rows = []
    for source, entry in document["sources"].items():
        grouped, excluded = entry.get("grouped", {}), entry.get("excluded", {})
        both = set(grouped) & set(excluded)
        if both:
            raise ValueError(
                f"{source}: labels both grouped and excluded: {sorted(both)}"
            )
        for label, group in grouped.items():
            if group not in groups:
                raise ValueError(f"{source} {label!r}: unknown group {group!r}")
            rows.append((source, label, group, None))
        for label, reason in excluded.items():
            if not reason:
                raise ValueError(f"{source} {label!r}: an exclusion needs a reason")
            rows.append((source, label, None, reason))
    return rows


@lru_cache(maxsize=1)
def rows() -> tuple[tuple[str, str, str | None, str | None], ...]:
    """The validated list, read once per process."""
    return tuple(parse(json.loads(PATH.read_text())))


def sync(conn) -> int:
    """Rewrite ingestion.fuel_groups from the file in the caller's transaction
    (a SQLAlchemy connection)."""
    conn.execute(text(f"DELETE FROM {TABLE}"))
    conn.execute(
        text(
            f"INSERT INTO {TABLE} (source_system, source_label, fuel_group, excluded_reason)"
            " VALUES (:source, :label, :fuel_group, :reason)"
        ),
        [
            {"source": s, "label": label, "fuel_group": group, "reason": reason}
            for s, label, group, reason in rows()
        ],
    )
    return len(rows())


def unmapped(conn, source: str) -> list[str]:
    """Labels of ``source`` in its all-fuel table that the list neither groups
    nor excludes."""
    table, column = LABEL_COLUMNS[source]
    known = {label for s, label, _, _ in rows() if s == source}
    found = conn.execute(text(f"SELECT DISTINCT {column} FROM {table}")).scalars()
    return sorted(label for label in found if label not in known)
