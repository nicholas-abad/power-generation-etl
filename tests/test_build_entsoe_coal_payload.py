"""The rebuild payload: only the reviewed name change, certified observations,
named or listed unnamed series, unique keys, byte-identical output."""

# ruff: noqa: F811  (pytest fixture shared from the certification tests)

import gzip
import importlib.util
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "src"))
from test_certify_entsoe_coal_source import (  # noqa: E402
    CERTIFY,
    DOMAIN,
    UNIT,
    archive,
    document,
    entries,
    extractor,  # noqa: F401
)
import entsoe_names  # noqa: E402

SPEC = importlib.util.spec_from_file_location(
    "payload", ROOT / "scripts/build_entsoe_coal_payload.py"
)
PAYLOAD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PAYLOAD)


def certify(root, path):
    results = [
        CERTIFY.certify_request((str(root), e, "PL", DOMAIN))
        for e in entries(root)
        if e["status"] == "ok"
    ]
    rows, unnamed, counters, count = CERTIFY.summarize(results)
    report = {
        "country": "PL",
        "domain": DOMAIN,
        "psrs": ["B02"],
        "start": "2024-03-01",
        "end": "2024-03-02",
        "extractor_commit": "test",
        "manifest_sha256": __import__("hashlib")
        .sha256((root / "manifest.jsonl").read_bytes())
        .hexdigest(),
        "unit_months": rows,
    }
    path.write_text(json.dumps(report))
    return path


@pytest.fixture
def names(tmp_path, monkeypatch):
    def write(units):
        path = tmp_path / "names.json"
        path.write_text(json.dumps({"units": units}))
        monkeypatch.setattr(entsoe_names, "PATH", path)
        entsoe_names.units.cache_clear()

    write([])
    yield write
    entsoe_names.units.cache_clear()


def identity(tmp_path, stored=()):
    path = tmp_path / "identity.json"
    path.write_text(
        json.dumps(
            {
                "stored_names": [
                    {"stored_name": n, "psr": "B02", "rule": "exact", "eics": [UNIT]}
                    for n in stored
                ]
            }
        )
    )
    return path


def build(tmp_path, root, ident, out="payload", workers=1):
    return PAYLOAD.build(
        root,
        certify(root, tmp_path / "cert.json"),
        ident,
        "test-repair",
        1700000000000,
        tmp_path / out,
        workers,
    )


def rows_of(tmp_path, manifest, out="payload"):
    data = gzip.decompress(
        (tmp_path / out / manifest["months"][0]["file"]).read_bytes()
    )
    lines = data.decode().splitlines()
    return [dict(zip(lines[0].split("\t"), line.split("\t"))) for line in lines[1:]]


def test_literal_name_rows_match_certified_energy(tmp_path, extractor, names):
    root = archive(tmp_path)
    manifest = build(tmp_path, root, identity(tmp_path))
    month = manifest["months"][0]
    assert (month["rows"], month["mwh"], month["excluded"]) == (2, "300", [])
    rows = rows_of(tmp_path, manifest)
    assert {r["plant_name"] for r in rows} == {"Belchatow 2"}
    assert [r["generation_mw"] for r in rows] == ["100", "200"]
    assert rows[0]["unit_eic"] == UNIT and rows[0]["source_unit_name"] == "Belchatow 2"


def test_reviewed_unit_gets_its_canonical_name(tmp_path, extractor, names):
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
    rows = rows_of(tmp_path, build(tmp_path, archive(tmp_path), identity(tmp_path)))
    assert {r["plant_name"] for r in rows} == {"Belchatow B2"}
    assert {r["source_unit_name"] for r in rows} == {"Belchatow 2"}


def test_unnamed_series_is_named_from_identity_map_or_excluded(
    tmp_path, extractor, names
):
    root = archive(tmp_path, body=document(name=""))
    named = build(tmp_path, root, identity(tmp_path, ["Belchatow 2"]), out="a")
    rows = rows_of(tmp_path, named, out="a")
    assert {r["plant_name"] for r in rows} == {"Belchatow 2"}
    assert {r["source_unit_name"] for r in rows} == {"\\N"}
    excluded = build(tmp_path, root, identity(tmp_path), out="b")["months"][0]
    assert excluded["rows"] == 0
    assert excluded["excluded"] == [
        {
            "unit_eic": UNIT,
            "psr": "B02",
            "day": "2024-03-01",
            "observations": 2,
            "mwh": "300",
        }
    ]


def test_payload_must_match_certified_observations(tmp_path, extractor, names):
    root = archive(tmp_path)
    cert = certify(root, tmp_path / "cert.json")
    extractor["mutate"] = lambda rows: [dict(rows[0], generation_mw=101.0)] + rows[1:]
    with pytest.raises(ValueError, match="differ from the certified digest"):
        PAYLOAD.build(root, cert, identity(tmp_path), "t", 1, tmp_path / "p", 1)


def test_reviewed_unit_under_unreviewed_name_fails(tmp_path, extractor, names):
    names(
        [
            {
                "country_code": "PL",
                "unit_eic": UNIT,
                "psr_type": "B02",
                "plant_name": "Belchatow B2",
                "source_names": ["Belchatow B2"],
                "legacy_names": ["Belchatow B2"],
            }
        ]
    )
    with pytest.raises(ValueError, match="unreviewed name"):
        build(tmp_path, archive(tmp_path), identity(tmp_path))


def test_output_is_byte_identical(tmp_path, extractor, names):
    root = archive(tmp_path)
    first = build(tmp_path, root, identity(tmp_path), out="one")
    second = build(tmp_path, root, identity(tmp_path), out="two")
    assert first["months"][0]["sha256"] == second["months"][0]["sha256"]


def test_month_with_only_no_data_gets_an_empty_file(tmp_path, extractor, names):
    root = archive(tmp_path, (("2024-03-01", "no_data"),))
    manifest = build(tmp_path, root, identity(tmp_path))
    assert [(m["month"], m["rows"], m["mwh"]) for m in manifest["months"]] == [
        ("2024-03", 0, "0")
    ]
    assert rows_of(tmp_path, manifest) == []
