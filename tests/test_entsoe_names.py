"""The single reviewed ENTSO-E identity table and its readers."""

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import entsoe_coal  # noqa: E402
import entsoe_names  # noqa: E402
from entsoe_identity import prepare_record  # noqa: E402


def key(alias):
    return tuple(
        alias[k]
        for k in ("country_code", "unit_eic", "psr_type", "source_name", "legacy_name")
    )


def test_czech_coal_entries_reproduce_the_frozen_release_aliases():
    frozen = json.loads((ROOT / "config/entsoe-coal-aliases.json").read_text())[
        "aliases"
    ]
    derived = [a for a in entsoe_coal.ALIASES if a["country_code"] == "CZ"]
    assert sorted(map(key, derived)) == sorted(map(key, frozen))
    assert entsoe_coal.aliases_for("CZ") and len(entsoe_coal.aliases_for("CZ")) == 25


def row(**changes):
    base = {
        "country_code": "PL",
        "unit_eic": "19W0000000000709",
        "psr_type": "B05",
        "plant_name": "EC Karolin G2",
    }
    base.update(changes)
    return base


def test_renamed_unit_is_stored_under_its_canonical_name():
    prepared = prepare_record(row())
    assert prepared["plant_name"] == "Karolin B2"
    assert prepared["source_unit_name"] == "EC Karolin G2"
    assert prepare_record(row(plant_name="Karolin B2"))["plant_name"] == "Karolin B2"


@pytest.mark.parametrize(
    "changes", [{"plant_name": "Karolin Unit 2"}, {"psr_type": "B02"}]
)
def test_known_eic_with_unreviewed_name_or_fuel_fails(changes):
    with pytest.raises(ValueError, match="unreviewed source name or PSR"):
        prepare_record(row(**changes))


def test_unlisted_unit_keeps_its_literal_source_name():
    prepared = prepare_record(
        row(unit_eic="19W0000000000458", plant_name="Dolna Odra B1")
    )
    assert prepared["plant_name"] == prepared["source_unit_name"] == "Dolna Odra B1"


def write_table(tmp_path, monkeypatch, units):
    path = tmp_path / "names.json"
    path.write_text(json.dumps({"units": units}))
    monkeypatch.setattr(entsoe_names, "PATH", path)
    entsoe_names.units.cache_clear()
    return path


def unit(**changes):
    base = {
        "country_code": "PL",
        "unit_eic": "19W0000000000709",
        "psr_type": "B05",
        "plant_name": "Karolin B2",
        "source_names": ["Karolin B2"],
        "legacy_names": ["Karolin B2"],
    }
    base.update(changes)
    return base


@pytest.mark.parametrize(
    "units, message",
    [
        ([unit(), unit()], "Repeated"),
        ([unit(), unit(unit_eic="19W0000000000717")], "share the canonical name"),
        ([unit(plant_name="Something else")], "neither a source nor a legacy name"),
    ],
    ids=["duplicate-eic", "shared-canonical", "unknown-canonical"],
)
def test_inconsistent_table_is_rejected(tmp_path, monkeypatch, units, message):
    write_table(tmp_path, monkeypatch, units)
    try:
        with pytest.raises(ValueError, match=message):
            entsoe_names.units()
    finally:
        entsoe_names.units.cache_clear()
