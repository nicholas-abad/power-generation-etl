"""The reviewed list of ENTSO-E fuel-label corrections (config/entsoe-fuel-exceptions.json).

ENTSO-E's per-unit fuel label (A73 psrType) is taken as given everywhere else.
A record here overrides it for one unit and a range of UTC months, after review
(Christine has the final say, tracker #48 and #10):

- ``exclude``: rows labelled coal that are not coal. Example: a coal unit
  converted to biomass that ENTSO-E still reports as hard coal.
- ``count_as_coal``: a non-coal series that is coal for the tracker. Example:
  München Nord 2 ("Nord 2 T20", EIC 11WD2NOR20001055), relabelled B04 (gas)
  for 2019–2024 although it burned coal until 2024. The certified rebuild
  writes those months as B05; every load drops the unit's B04 rows in the
  window, so the months come only from the rebuild.
- ``count_as_other_fuel``: coal rows that are the unit's real other fuel,
  kept as that fuel instead of dropped (tracker #10: a converted unit's output
  stays visible). Example: Studstrup 3 (DK_CA), labelled B05 hard coal by
  ENTSO-E after its conversion to biomass, is loaded as B01 "Biomass". Every
  load moves the row; rows already stored are moved by
  src/repair_entsoe_fuel_moves.py. The certified rebuild refuses an area with
  such a record (its replacement scope is coal-only).

Matching is on the row's UTC month (YYYY-MM from timestamp_ms), never on today's
date, so a historical reload is filtered exactly like the weekly load. A row
matches by unit EIC, or, when it has no EIC (legacy rows), by one of the
record's ``reviewed_names``; names are never matched outside a listed record.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re

from entsoe_fuels import PSR_TO_FUEL_TYPE

PATH = Path(__file__).resolve().parents[1] / "config/entsoe-fuel-exceptions.json"
COAL = frozenset({"B02", "B03", "B05"})
TREATMENTS = frozenset({"exclude", "count_as_coal", "count_as_other_fuel"})
# Treatments whose matching rows a load drops; count_as_other_fuel moves them.
DROPPED_ON_LOAD = frozenset({"exclude", "count_as_coal"})
_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
# Bidding zones and control areas: DE_TENNET, DK_CA, IT_SACO_AC, DE_AT_LU.
# A mistyped area or EIC matches nothing; certification and the payload then
# fail on a record without observations (scripts/certify_entsoe_coal_source.py,
# scripts/build_entsoe_coal_payload.py).
_AREA = re.compile(r"^[A-Z]{2}(_[A-Z0-9]+)*$")
_EIC = re.compile(r"^[A-Z0-9-]{16}$")
# ENTSO-E generation production types (B21-B24 are links and substations).
GENERATION_PSRS = frozenset(f"B{n:02d}" for n in (*range(1, 21), 25))


@dataclass(frozen=True)
class FuelException:
    """One reviewed correction of a unit's fuel label over a range of months."""

    id: str
    area: str
    unit_eic: str
    unit_name: str
    reviewed_names: tuple[str, ...]
    from_month: str
    to_month: str | None
    source_psr: str
    treatment: str
    counted_psr: str | None
    reason: str
    decided_by: str
    decided_on: str
    ref: str

    def covers(self, month: str) -> bool:
        """True if ``month`` (YYYY-MM) lies in [from_month, to_month]."""
        return self.from_month <= month and (
            self.to_month is None or month <= self.to_month
        )


def _check(entry: dict) -> FuelException:
    label = entry.get("id") or "<no id>"
    missing = [
        key
        for key in (
            "id",
            "area",
            "unit_eic",
            "unit_name",
            "from_month",
            "source_psr",
            "treatment",
            "reason",
            "decided_by",
            "decided_on",
            "ref",
        )
        if not entry.get(key)
    ]
    if missing:
        raise ValueError(f"Fuel exception {label}: missing {missing}")
    if not _AREA.fullmatch(entry["area"]):
        raise ValueError(f"Fuel exception {label}: unknown area {entry['area']!r}")
    if not _EIC.fullmatch(entry["unit_eic"]):
        raise ValueError(f"Fuel exception {label}: invalid unit EIC")
    to_month = entry.get("to_month")
    for month in (entry["from_month"], to_month):
        if month is not None and not _MONTH.fullmatch(month):
            raise ValueError(f"Fuel exception {label}: invalid month {month!r}")
    if to_month is not None and to_month < entry["from_month"]:
        raise ValueError(f"Fuel exception {label}: to_month before from_month")
    treatment, source = entry["treatment"], entry["source_psr"]
    counted = entry.get("counted_psr")
    if treatment not in TREATMENTS:
        raise ValueError(f"Fuel exception {label}: unknown treatment {treatment!r}")
    if source not in GENERATION_PSRS:
        raise ValueError(f"Fuel exception {label}: invalid source_psr {source!r}")
    if treatment == "exclude" and (source not in COAL or counted is not None):
        raise ValueError(
            f"Fuel exception {label}: exclude needs a coal source_psr and no counted_psr"
        )
    if treatment == "count_as_coal" and (source in COAL or counted not in COAL):
        raise ValueError(
            f"Fuel exception {label}: count_as_coal needs a non-coal source_psr "
            "and a coal counted_psr"
        )
    if treatment == "count_as_other_fuel" and (
        source not in COAL or counted in COAL or counted not in PSR_TO_FUEL_TYPE
    ):
        raise ValueError(
            f"Fuel exception {label}: count_as_other_fuel needs a coal source_psr "
            "and a non-coal generation counted_psr"
        )
    names = entry.get("reviewed_names", [])
    if not isinstance(names, list) or not all(isinstance(n, str) and n for n in names):
        raise ValueError(f"Fuel exception {label}: reviewed_names must be names")
    return FuelException(
        id=entry["id"],
        area=entry["area"],
        unit_eic=entry["unit_eic"],
        unit_name=entry["unit_name"],
        reviewed_names=tuple(names),
        from_month=entry["from_month"],
        to_month=to_month,
        source_psr=source,
        treatment=treatment,
        counted_psr=counted,
        reason=entry["reason"],
        decided_by=entry["decided_by"],
        decided_on=entry["decided_on"],
        ref=entry["ref"],
    )


def parse(document: dict) -> tuple[FuelException, ...]:
    """Validated records of a fuel-exceptions document; raises on any error."""
    records = tuple(_check(entry) for entry in document["exceptions"])
    ids = [r.id for r in records]
    if len(ids) != len(set(ids)):
        raise ValueError("Repeated fuel exception id")
    for i, a in enumerate(records):
        for b in records[i + 1 :]:
            if (a.area, a.unit_eic) != (b.area, b.unit_eic):
                continue
            a_end, b_end = a.to_month or "9999-12", b.to_month or "9999-12"
            if a.from_month <= b_end and b.from_month <= a_end:
                raise ValueError(f"Fuel exceptions {a.id} and {b.id} overlap")
    return records


@lru_cache(maxsize=1)
def exceptions() -> tuple[FuelException, ...]:
    """The reviewed list, validated once per process."""
    return parse(json.loads(PATH.read_text()))


def sha256() -> str:
    """Hash of the list file, recorded in rebuild manifests."""
    return hashlib.sha256(PATH.read_bytes()).hexdigest()


def utc_month(timestamp_ms: int) -> str:
    """YYYY-MM of a UTC timestamp in milliseconds."""
    return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).strftime("%Y-%m")


def matching(
    country: str,
    psr: str,
    month: str,
    unit_eic: str | None,
    plant_name: str | None = None,
    records: tuple[FuelException, ...] | None = None,
) -> FuelException | None:
    """The record that applies to a row, or None."""
    for record in exceptions() if records is None else records:
        if (
            record.area == country
            and record.source_psr == psr
            and record.covers(month)
            and (
                unit_eic == record.unit_eic
                if unit_eic is not None
                else plant_name in record.reviewed_names
            )
        ):
            return record
    return None


def count_as_coal_pairs(
    country: str, month: str, records=None
) -> frozenset[tuple[str, str]]:
    """(source psr, unit EIC) of the country's count_as_coal records covering
    ``month``: the non-coal series a certification and payload read beside
    coal in that month."""
    return frozenset(
        (r.source_psr, r.unit_eic)
        for r in (exceptions() if records is None else records)
        if r.area == country and r.treatment == "count_as_coal" and r.covers(month)
    )


def overlapping(country: str, start: str, end: str, records=None):
    """The country's records covering any month in [start, end] (YYYY-MM)."""
    return [
        r
        for r in (exceptions() if records is None else records)
        if r.area == country
        and r.from_month <= end
        and (r.to_month is None or start <= r.to_month)
    ]


def areas_requiring_unit_eic(records=None) -> set[str]:
    """Areas with an exclude or count_as_other_fuel record: their coal rows must carry a unit EIC."""
    return {
        r.area
        for r in (exceptions() if records is None else records)
        if r.treatment in ("exclude", "count_as_other_fuel")
    }


def other_fuel_areas(records=None) -> set[str]:
    """Areas with a count_as_other_fuel record: the certified rebuild refuses them."""
    return {
        r.area
        for r in (exceptions() if records is None else records)
        if r.treatment == "count_as_other_fuel"
    }


def moved_record(record: dict, exception: FuelException) -> dict:
    """The row as its real fuel: psr_type and fuel_type of the record's counted PSR."""
    if exception.treatment != "count_as_other_fuel":
        raise ValueError(f"Fuel exception {exception.id} does not move rows")
    return {
        **record,
        "psr_type": exception.counted_psr,
        "fuel_type": PSR_TO_FUEL_TYPE[exception.counted_psr],
    }


def exception_on_load(record: dict, records=None) -> FuelException | None:
    """The record that applies to this row in an ENTSO-E load, or None.

    Every load path calls this (weekly, START_OVERRIDE replays, prepared
    files), after timestamps are parsed and legacy names cleaned. The caller
    drops the row for a DROPPED_ON_LOAD treatment (``exclude``: coal that is
    not coal; ``count_as_coal``: source-PSR rows whose months come only from
    the certified rebuild) and moves it for ``count_as_other_fuel``
    (moved_record). A coal row without an EIC from an area with an exclude or
    count_as_other_fuel record fails the load: it cannot be checked, which
    happens only with a stale extractor pin or an area not on the
    source-period path.
    """
    country, psr, eic = (
        record.get("country_code"),
        record.get("psr_type"),
        record.get("unit_eic"),
    )
    if eic is None and psr in COAL and country in areas_requiring_unit_eic(records):
        raise ValueError(
            f"{country} coal row without a unit EIC cannot be checked against the "
            "fuel exceptions (config/entsoe-fuel-exceptions.json)"
        )
    timestamp = record.get("timestamp_ms")
    return matching(
        country,
        psr,
        utc_month(timestamp) if timestamp is not None else "",
        eic,
        record.get("plant_name"),
        records,
    )
