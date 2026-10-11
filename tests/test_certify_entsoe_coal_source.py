"""Certification must fail closed: incomplete archives, tampered bodies and a
parser that disagrees with the independent Decimal parser are all rejected."""

from datetime import date, timedelta
from decimal import Decimal
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import types

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location(
    "certify", ROOT / "scripts/certify_entsoe_coal_source.py"
)
CERTIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CERTIFY)
from audit_entsoe_coal_history import FUEL, independent_day, ms  # noqa: E402

DOMAIN = "10YPL-AREA-----S"
UNIT = "19W000000000095U"


def document(points=((1, "100"), (2, "200"))):
    pts = "".join(
        f"<Point><position>{p}</position><quantity>{q}</quantity></Point>"
        for p, q in points
    )
    return f"""<GL_MarketDocument xmlns="urn:test"><type>A73</type>
    <process.processType>A16</process.processType><TimeSeries>
    <businessType>A01</businessType><objectAggregation>A06</objectAggregation>
    <inBiddingZone_Domain.mRID>{DOMAIN}</inBiddingZone_Domain.mRID>
    <registeredResource.mRID codingScheme="A01">19W0000000000PRD</registeredResource.mRID>
    <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name><curveType>A01</curveType>
    <MktPSRType><psrType>B02</psrType><PowerSystemResources>
    <mRID codingScheme="A01">{UNIT}</mRID><name>Belchatow 2</name>
    </PowerSystemResources></MktPSRType><Period><timeInterval>
    <start>2024-03-01T00:00Z</start><end>2024-03-02T00:00Z</end></timeInterval>
    <resolution>PT60M</resolution>{pts}</Period></TimeSeries></GL_MarketDocument>""".encode()


def archive(tmp_path, statuses=(("2024-03-01", "ok"),), body=None):
    root = tmp_path / "PL"
    (root / "xml").mkdir(parents=True)
    lines = []
    for day, status in statuses:
        entry = {
            "country": "PL",
            "domain": DOMAIN,
            "day": day,
            "psr": "B02",
            "start": f"{day}T00:00:00+00:00",
            "end": f"{date.fromisoformat(day) + timedelta(days=1)}T00:00:00+00:00",
            "status": status,
        }
        if status == "ok":
            data = body or document()
            entry["sha256"] = hashlib.sha256(data).hexdigest()
            (root / "xml" / f"{day}_B02.xml.gz").write_bytes(gzip.compress(data))
        lines.append(json.dumps(entry))
    (root / "manifest.jsonl").write_text("\n".join(lines) + "\n")
    return root


@pytest.fixture
def extractor(monkeypatch):
    """A stand-in extractor parser derived from the independent parser.

    `mutate` lets a test make it disagree, as a parser defect would."""
    state = {"mutate": None}

    def parse_coal_periods(xml, country, domain, start, end, psrs):
        observations, _, _ = independent_day(xml, ms(start), ms(end), domain)
        rows = [
            {
                "timestamp_ms": ts,
                "unit_eic": eic,
                "psr_type": psr,
                "plant_name": name,
                "generation_mw": float(power),
                "resolution_minutes": minutes,
                "production_unit_eic": parent,
                "data_type": "Actual Aggregated",
                "resolution_source": "xml_period",
                "fuel_type": FUEL[psr],
                "country_code": country,
            }
            for (ts, eic), (psr, name, power, minutes, parent) in observations.items()
        ]
        if state["mutate"]:
            rows = state["mutate"](rows)
        return rows, {}

    periods = types.ModuleType("energy_extractors.entsoe.periods")
    periods.parse_coal_periods = parse_coal_periods
    for name, module in [
        ("energy_extractors", types.ModuleType("energy_extractors")),
        ("energy_extractors.entsoe", types.ModuleType("energy_extractors.entsoe")),
        ("energy_extractors.entsoe.periods", periods),
    ]:
        monkeypatch.setitem(sys.modules, name, module)
    return state


def entries(root):
    return [
        json.loads(line) for line in (root / "manifest.jsonl").read_text().splitlines()
    ]


def test_matching_parsers_certify_with_decimal_energy(tmp_path, extractor):
    root = archive(tmp_path)
    day, psr, observations, _ = CERTIFY.certify_request(
        (str(root), entries(root)[0], "PL", DOMAIN)
    )
    assert len(observations) == 2
    assert sum(v[2] * v[3] / 60 for v in observations.values()) == Decimal("300")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda rows: rows[1:],  # a dropped observation
        lambda rows: [dict(rows[0], generation_mw=101.0)] + rows[1:],  # a changed MW
        lambda rows: [dict(rows[0], resolution_minutes=15)]
        + rows[1:],  # wrong interval
    ],
    ids=["dropped", "changed-mw", "wrong-interval"],
)
def test_parser_disagreement_fails_certification(tmp_path, extractor, mutate):
    root = archive(tmp_path)
    extractor["mutate"] = mutate
    with pytest.raises(ValueError, match="disagrees with extractor"):
        CERTIFY.certify_request((str(root), entries(root)[0], "PL", DOMAIN))


def test_tampered_archive_body_fails(tmp_path, extractor):
    root = archive(tmp_path)
    (root / "xml" / "2024-03-01_B02.xml.gz").write_bytes(
        gzip.compress(document(((1, "100"), (2, "999"))))
    )
    with pytest.raises(ValueError, match="hash mismatch"):
        CERTIFY.certify_request((str(root), entries(root)[0], "PL", DOMAIN))


def test_incomplete_or_failed_inventory_fails(tmp_path):
    root = archive(tmp_path, (("2024-03-01", "ok"), ("2024-03-02", "failed")))
    with pytest.raises(ValueError, match="failed requests"):
        CERTIFY.load_inventory(root, ["B02"], date(2024, 3, 1), date(2024, 3, 3))
    with pytest.raises(ValueError, match="lacks 1 requests"):
        CERTIFY.load_inventory(root, ["B02"], date(2024, 3, 1), date(2024, 3, 4))


def test_verified_no_data_is_complete(tmp_path):
    root = archive(tmp_path, (("2024-03-01", "ok"), ("2024-03-02", "no_data")))
    inventory, domain = CERTIFY.load_inventory(
        root, ["B02"], date(2024, 3, 1), date(2024, 3, 3)
    )
    assert domain == DOMAIN and [e["status"] for e in inventory] == ["ok", "no_data"]
