"""The reviewed ENTSO-E fuel exceptions: the list, matching on the row's month,
the load filter, certification of listed non-coal series and the payload."""

# ruff: noqa: F811  (pytest fixtures shared from the certification tests)

import copy
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "src"))
from test_build_entsoe_coal_payload import (  # noqa: E402
    PAYLOAD,
    identity,
    names,  # noqa: F401
    rows_of,
)
from test_certify_entsoe_coal_source import (  # noqa: E402
    CERTIFY,
    DOMAIN,
    UNIT,
    archive,
    entries,
    extractor,  # noqa: F401
)
import entsoe_fuel_exceptions as fx  # noqa: E402
import audit_entsoe_coal_history as AUDIT  # noqa: E402
from audit_entsoe_coal_history import field  # noqa: E402
from xml.etree import ElementTree  # noqa: E402

GAS_UNIT = "19W0000000000GAS"
MARCH_1 = 1709251200000  # 2024-03-01T00:00Z

NORD2 = {
    "id": "PL-gas-unit-coal",
    "area": "PL",
    "unit_eic": GAS_UNIT,
    "unit_name": "Gas Unit 1",
    "reviewed_names": ["Gas Unit 1"],
    "from_month": "2024-01",
    "to_month": "2024-12",
    "source_psr": "B04",
    "treatment": "count_as_coal",
    "counted_psr": "B05",
    "reason": "test",
    "decided_by": "test",
    "decided_on": "2026-11-09",
    "ref": "test",
}
EXCLUDE = {
    **NORD2,
    "id": "PL-belchatow-2-not-coal",
    "unit_eic": UNIT,
    "unit_name": "Belchatow 2",
    "reviewed_names": [],
    "source_psr": "B02",
    "treatment": "exclude",
    "counted_psr": None,
}


def records(*entries):
    return fx.parse({"exceptions": list(entries)})


# --- the list ---------------------------------------------------------------


def test_the_committed_list_is_valid_and_holds_munich_nord_2():
    (nord2,) = [r for r in fx.exceptions() if r.unit_eic == "11WD2NOR20001055"]
    assert (nord2.treatment, nord2.source_psr, nord2.counted_psr) == (
        "count_as_coal",
        "B04",
        "B05",
    )
    assert (nord2.from_month, nord2.to_month) == ("2019-01", "2024-12")


@pytest.mark.parametrize(
    "change, message",
    [
        ({"treatment": "relabel"}, "unknown treatment"),
        ({"to_month": "2023-12"}, "to_month before from_month"),
        ({"source_psr": "B05"}, "count_as_coal needs a non-coal source_psr"),
        ({"counted_psr": "B04"}, "count_as_coal needs a non-coal source_psr"),
        ({"decided_by": ""}, "missing"),
        ({"unit_eic": "SHORT"}, "invalid unit EIC"),
        ({"from_month": "2024-13"}, "invalid month"),
    ],
)
def test_an_invalid_record_fails_loudly(change, message):
    with pytest.raises(ValueError, match=message):
        records({**NORD2, **change})


def test_exclude_needs_a_coal_source_psr():
    with pytest.raises(ValueError, match="exclude needs a coal source_psr"):
        records({**EXCLUDE, "source_psr": "B04"})


def test_overlapping_records_for_one_unit_are_rejected():
    later = {**NORD2, "id": "second", "from_month": "2024-06", "to_month": None}
    with pytest.raises(ValueError, match="overlap"):
        records(NORD2, later)
    records(NORD2, {**later, "from_month": "2025-01"})  # adjacent is fine


def test_matching_uses_the_rows_month_and_eic_or_a_reviewed_name():
    listed = records(NORD2)
    assert fx.matching("PL", "B04", "2024-03", GAS_UNIT, records=listed)
    assert fx.matching("PL", "B04", "2024-03", None, "Gas Unit 1", records=listed)
    # Outside the window, another PSR, another name without EIC, or another
    # unit under the reviewed name: no match.
    assert not fx.matching("PL", "B04", "2025-01", GAS_UNIT, records=listed)
    assert not fx.matching("PL", "B05", "2024-03", GAS_UNIT, records=listed)
    assert not fx.matching("PL", "B04", "2024-03", None, "Other", records=listed)
    assert not fx.matching(
        "PL", "B04", "2024-03", "19W000000000OTHR", "Gas Unit 1", records=listed
    )


def test_an_open_ended_record_covers_every_later_month():
    (record,) = records({**EXCLUDE, "to_month": None})
    assert record.covers("2031-07") and not record.covers("2023-12")


# --- the load filter --------------------------------------------------------


def row(**changes):
    return {
        "country_code": "PL",
        "psr_type": "B04",
        "timestamp_ms": MARCH_1,
        "unit_eic": None,
        "plant_name": "Gas Unit 1",
        **changes,
    }


def test_a_historical_reload_drops_the_relabelled_months_and_keeps_later_gas():
    listed = records(NORD2)
    assert fx.filtered_on_load(row(), listed).id == "PL-gas-unit-coal"
    after = row(timestamp_ms=1735689600000)  # 2025-01-01: gas after conversion
    assert fx.filtered_on_load(after, listed) is None


def test_an_excluded_coal_row_is_dropped_and_others_kept():
    listed = records(EXCLUDE)
    coal = row(psr_type="B02", unit_eic=UNIT, plant_name="Belchatow 2")
    assert fx.filtered_on_load(coal, listed).id == "PL-belchatow-2-not-coal"
    other = dict(coal, unit_eic="19W000000000OTHR")
    assert fx.filtered_on_load(other, listed) is None


def test_coal_without_an_eic_from_an_area_with_an_exclude_record_fails():
    listed = records(EXCLUDE)
    with pytest.raises(ValueError, match="without a unit EIC"):
        fx.filtered_on_load(row(psr_type="B02", plant_name="Belchatow 2"), listed)
    # Gas rows without an EIC are fine: they are not coal.
    assert fx.filtered_on_load(row(plant_name="Other gas"), listed) is None


# --- certification ----------------------------------------------------------


def series(psr, eic, name, points, day="2024-03-01"):
    pts = "".join(
        f"<Point><position>{p}</position><quantity>{q}</quantity></Point>"
        for p, q in points
    )
    return f"""<TimeSeries>
    <businessType>A01</businessType><objectAggregation>A06</objectAggregation>
    <inBiddingZone_Domain.mRID>{DOMAIN}</inBiddingZone_Domain.mRID>
    <registeredResource.mRID codingScheme="A01">19W0000000000PRD</registeredResource.mRID>
    <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name><curveType>A01</curveType>
    <MktPSRType><psrType>{psr}</psrType><PowerSystemResources>
    <mRID codingScheme="A01">{eic}</mRID><name>{name}</name>
    </PowerSystemResources></MktPSRType><Period><timeInterval>
    <start>{day}T00:00Z</start><end>2024-03-02T00:00Z</end></timeInterval>
    <resolution>PT60M</resolution>{pts}</Period></TimeSeries>"""


def doc(*parts):
    return (
        '<GL_MarketDocument xmlns="urn:test"><type>A73</type>'
        "<process.processType>A16</process.processType>"
        + "".join(parts)
        + "</GL_MarketDocument>"
    ).encode()


COAL_SERIES = series("B02", UNIT, "Belchatow 2", ((1, "100"), (2, "200")))
GAS_SERIES = series("B04", GAS_UNIT, "Gas Unit 1", ((1, "50"),))
OTHER_GAS = series("B04", "19W000000000OTHR", "Other gas", ((1, "7"),))


def test_a_listed_gas_series_is_certified_beside_coal_and_others_are_reported(
    tmp_path, extractor
):
    root = archive(tmp_path, body=doc(COAL_SERIES, GAS_SERIES, OTHER_GAS))
    listed = records(NORD2)
    result = CERTIFY.certify_request(
        (str(root), entries(root)[0], "PL", DOMAIN, listed)
    )
    assert {(k[1], v[0]) for k, v in result[2].items()} == {
        (UNIT, "B02"),
        (GAS_UNIT, "B04"),
    }
    rows, _, _, _, skipped = CERTIFY.summarize([result], "PL", listed)
    gas = [r for r in rows if r["unit_eic"] == GAS_UNIT]
    assert gas[0]["exception_ids"] == ["PL-gas-unit-coal"]
    assert (gas[0]["mwh"], gas[0]["relabelled_mwh"]) == ("50", "50")
    assert "exception_ids" not in [r for r in rows if r["unit_eic"] == UNIT][0]
    assert skipped == [
        {
            "unit_eic": "19W000000000OTHR",
            "psr": "B04",
            "months": ["2024-03"],
            "series_days": 1,
        }
    ]


def test_a_listed_series_outside_its_months_is_reported_not_certified(
    tmp_path, extractor
):
    root = archive(tmp_path, body=doc(COAL_SERIES, GAS_SERIES))
    listed = records({**NORD2, "from_month": "2025-01", "to_month": None})
    result = CERTIFY.certify_request(
        (str(root), entries(root)[0], "PL", DOMAIN, listed)
    )
    assert {k[1] for k in result[2]} == {UNIT}
    assert result[4] == {(GAS_UNIT, "B04")}


def test_identical_copies_of_a_listed_series_count_once_conflicting_ones_fail():
    gas = {(MARCH_1, GAS_UNIT): ("B04", "Gas Unit 1", Decimal("50"), 60, None)}
    coal = {(MARCH_1, UNIT): ("B02", "Belchatow 2", Decimal("100"), 60, None)}
    listed = records(NORD2)
    rows, _, counters, count, _ = CERTIFY.summarize(
        [
            ("2024-03-01", "B02", {**coal, **gas}, {}, set()),
            ("2024-03-01", "B05", copy.deepcopy(gas), {}, set()),
        ],
        "PL",
        listed,
    )
    assert count == 2 and counters["listed_identical_copies"] == 1
    changed = {k: ("B04", "Gas Unit 1", Decimal("51"), 60, None) for k in gas}
    with pytest.raises(ValueError, match="repeated across requests"):
        CERTIFY.summarize(
            [
                ("2024-03-01", "B02", gas, {}, set()),
                ("2024-03-01", "B05", changed, {}, set()),
            ],
            "PL",
            listed,
        )
    # A coal observation in two requests still fails.
    with pytest.raises(ValueError, match="repeated across requests"):
        CERTIFY.summarize(
            [
                ("2024-03-01", "B02", coal, {}, set()),
                ("2024-03-01", "B05", coal, {}, set()),
            ]
        )


# --- the payload ------------------------------------------------------------


@pytest.fixture
def listed(monkeypatch):
    """Install a fuel-exceptions list for the payload builder."""

    def install(*entries):
        parsed = records(*entries)
        monkeypatch.setattr(fx, "exceptions", lambda: parsed)
        monkeypatch.setattr(fx, "sha256", lambda: "test-list")
        return parsed

    return install


def certify_with(root, path, windows):
    results = [
        CERTIFY.certify_request((str(root), e, "PL", DOMAIN, windows))
        for e in entries(root)
        if e["status"] == "ok"
    ]
    rows, _, _, _, _ = CERTIFY.summarize(results, "PL", windows)
    path.write_text(
        json.dumps(
            {
                "country": "PL",
                "domain": DOMAIN,
                "psrs": ["B02"],
                "start": "2024-03-01",
                "end": "2024-03-02",
                "extractor_commit": "test",
                "manifest_sha256": hashlib.sha256(
                    (root / "manifest.jsonl").read_bytes()
                ).hexdigest(),
                "fuel_exceptions_sha256": "test-list",
                "unit_months": rows,
            }
        )
    )
    return path


def build_with(tmp_path, root, windows):
    return PAYLOAD.build(
        root,
        certify_with(root, tmp_path / "cert.json", windows),
        identity(tmp_path),
        "test-repair",
        1700000000000,
        tmp_path / "payload",
        1,
    )


def test_a_relabelled_series_is_written_as_its_counted_coal_psr(
    tmp_path, extractor, names, listed
):
    root = archive(tmp_path, body=doc(COAL_SERIES, GAS_SERIES))
    manifest = build_with(tmp_path, root, listed(NORD2))
    month = manifest["months"][0]
    gas = [r for r in rows_of(tmp_path, manifest) if r["unit_eic"] == GAS_UNIT]
    assert [(r["psr_type"], r["fuel_type"], r["plant_name"]) for r in gas] == [
        ("B05", "Fossil Hard coal", "Gas Unit 1")
    ]
    assert month["relabelled"] == [
        {
            "exception_id": "PL-gas-unit-coal",
            "unit_eic": GAS_UNIT,
            "observations": 1,
            "mwh": "50",
        }
    ]
    # MWh identity: payload + excluded = certified (coal 300 + relabelled 50).
    assert (month["mwh"], month["excluded"]) == ("350", [])
    assert manifest["inputs"]["fuel_exceptions_sha256"] == "test-list"


def test_an_excluded_reviewed_unit_lands_in_excluded_not_in_the_file(
    tmp_path, extractor, names, listed
):
    # Exceptions are tested before the reviewed-unit rule: a named reviewed
    # unit (here with a canonical name) is still excluded.
    names(
        [
            {
                "country_code": "PL",
                "unit_eic": UNIT,
                "psr_type": "B02",
                "plant_name": "Belchatow B2",
                "source_names": ["Belchatow 2"],
                "legacy_names": ["Belchatow B2"],
            }
        ]
    )
    root = archive(tmp_path)
    manifest = build_with(tmp_path, root, listed(EXCLUDE))
    month = manifest["months"][0]
    assert month["rows"] == 0 and rows_of(tmp_path, manifest) == []
    assert month["excluded"] == [
        {
            "unit_eic": UNIT,
            "psr": "B02",
            "day": "2024-03-01",
            "observations": 2,
            "mwh": "300",
            "exception_id": "PL-belchatow-2-not-coal",
        }
    ]


def test_a_changed_list_requires_a_new_certification(
    tmp_path, extractor, names, listed, monkeypatch
):
    root = archive(tmp_path, body=doc(COAL_SERIES, GAS_SERIES))
    windows = listed(NORD2)
    cert = certify_with(root, tmp_path / "cert.json", windows)
    monkeypatch.setattr(fx, "sha256", lambda: "edited-list")
    with pytest.raises(ValueError, match="changed since certification"):
        PAYLOAD.build(root, cert, identity(tmp_path), "t", 1, tmp_path / "p", 1)


def test_payload_files_stay_byte_identical_with_exceptions(
    tmp_path, extractor, names, listed
):
    root = archive(tmp_path, body=doc(COAL_SERIES, GAS_SERIES))
    windows = listed(NORD2)
    first = build_with(tmp_path, root, windows)["months"][0]["sha256"]
    data = gzip.decompress((tmp_path / "payload" / "2024-03.tsv.gz").read_bytes())
    assert data.count(b"\n") == 4  # header + 2 coal + 1 relabelled
    assert build_with(tmp_path, root, windows)["months"][0]["sha256"] == first


# --- review fixes (PR #147) -------------------------------------------------


def test_only_coal_and_listed_series_reach_the_strict_parser():
    # An unrelated gas unit with a shape the strict parser rejects must not stop
    # a coal certification just because B04 is requested for one listed unit.
    odd = OTHER_GAS.replace("<curveType>A01</curveType>", "<curveType>A02</curveType>")
    narrowed = AUDIT.coal_and_listed_series(
        doc(COAL_SERIES, GAS_SERIES, odd), {("B04", GAS_UNIT)}
    )
    kept = {
        (
            field(s, "MktPSRType/psrType"),
            field(s, "MktPSRType/PowerSystemResources/mRID"),
        )
        for s in ElementTree.fromstring(narrowed).findall("{*}TimeSeries")
    }
    assert kept == {("B02", UNIT), ("B04", GAS_UNIT)}


def test_a_unit_switching_label_within_a_month_keeps_both_ledgers():
    b05 = {(MARCH_1, GAS_UNIT): ("B05", "Gas Unit 1", Decimal("80"), 60, None)}
    b04 = {
        (MARCH_1 + 3600000, GAS_UNIT): ("B04", "Gas Unit 1", Decimal("50"), 60, None)
    }
    (row,), _, _, _, _ = CERTIFY.summarize(
        [("2024-03-01", "B05", {**b05, **b04}, {}, set())], "PL", records(NORD2)
    )
    assert (row["psr"], row["mwh"], row["relabelled_mwh"]) == (
        ["B04", "B05"],
        "130",
        "50",
    )


def test_two_periods_of_one_unit_are_both_certified():
    first = {**NORD2, "to_month": "2024-02"}
    second = {**NORD2, "id": "second", "from_month": "2024-03", "to_month": None}
    listed = records(first, second)
    assert fx.count_as_coal_pairs("PL", "2024-01", listed) == {("B04", GAS_UNIT)}
    assert fx.count_as_coal_pairs("PL", "2024-03", listed) == {("B04", GAS_UNIT)}
    jan = {(1704067200000, GAS_UNIT): ("B04", "Gas Unit 1", Decimal("5"), 60, None)}
    mar = {(MARCH_1, GAS_UNIT): ("B04", "Gas Unit 1", Decimal("7"), 60, None)}
    rows, _, _, _, _ = CERTIFY.summarize(
        [("2024-01-01", "B05", jan, {}, set()), ("2024-03-01", "B05", mar, {}, set())],
        "PL",
        listed,
    )
    assert [r["exception_ids"] for r in rows] == [["PL-gas-unit-coal"], ["second"]]


def test_a_record_without_observations_fails_certification():
    from datetime import date

    listed = records({**NORD2, "unit_eic": "19W0000000TYPO00"})
    with pytest.raises(ValueError, match="has no observations"):
        CERTIFY.require_records_observed(
            [], "PL", date(2024, 3, 1), date(2024, 4, 1), listed
        )
    # A record outside the certified window is not required.
    CERTIFY.require_records_observed(
        [], "PL", date(2025, 3, 1), date(2025, 4, 1), records(NORD2)
    )


def test_a_payload_record_that_matched_nothing_fails(
    tmp_path, extractor, names, listed
):
    root = archive(tmp_path)
    windows = listed({**EXCLUDE, "unit_eic": "19W0000000TYPO00"})
    with pytest.raises(ValueError, match="matched no row"):
        build_with(tmp_path, root, windows)


def test_an_unnamed_listed_series_left_out_is_not_counted_as_relabelled(
    tmp_path, extractor, names, listed
):
    unnamed = series("B04", GAS_UNIT, "", ((1, "50"),))
    root = archive(tmp_path, body=doc(COAL_SERIES, unnamed))
    month = build_with(tmp_path, root, listed(NORD2))["months"][0]
    assert month["relabelled"] == []
    assert [(x["unit_eic"], x["mwh"]) for x in month["excluded"]] == [(GAS_UNIT, "50")]
