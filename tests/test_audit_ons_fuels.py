"""The independent ONS fuel audit applies the documented selection policy."""

from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from audit_ons_fuels import account, join_gaps, merge  # noqa: E402


def row(**changes):
    base = {
        "din_instante": "2026-01-01 00:00:00",
        "id_ons": "PLANT",
        "nom_usina": "Plant",
        "ceg": "UTE.CV.SC.000000-0.01",
        "nom_tipousina": "TÉRMICA",
        "nom_tipocombustivel": "Carvão",
        "cod_modalidadeoperacao": "TIPO I",
        "val_geracao": 10.0,
    }
    return {**base, **changes}


def test_policy_counts_each_row_once_per_fuel():
    df = pd.DataFrame(
        [
            row(),
            row(id_ons="B", val_geracao=None),  # null measurement
            row(id_ons="C", val_geracao=-1),
            row(id_ons="D", ceg="a;b"),
            row(id_ons=" - "),
            row(id_ons="E", cod_modalidadeoperacao="tipo ii-b "),
            row(id_ons="F", din_instante="not a date"),
            row(id_ons="G", ceg=None),  # null CEG is allowed
            row(id_ons="DUP"),
            row(id_ons="DUP", nom_usina="Other name"),
            row(id_ons="W", nom_tipocombustivel="Eólica", cod_modalidadeoperacao="TIPO II-C"),
            row(id_ons="X", nom_tipocombustivel="Eólica", cod_modalidadeoperacao="TIPO III"),
            row(id_ons="Y", nom_tipocombustivel="-"),
            row(id_ons="Z", cod_modalidadeoperacao="Nova modalidade"),
        ]
    )
    table, _ = account(df)
    coal = table["Carvão"]
    assert coal["raw_rows"] == 11
    assert coal["accepted_rows"] == 3  # PLANT, E, G
    assert coal["accepted_mwh"] == 30.0
    assert coal["rows_by_modality"] == {"individual": 10, "unresolved": 1}
    assert coal["rejected_eligible"] == {
        "duplicate_observation": 2,
        "invalid_generation": 1,
        "missing_generation": 1,
        "invalid_timestamp": 1,
        "unresolved_identity": 2,
    }
    assert table["Eólica"]["accepted_rows"] == 0
    assert table["Eólica"]["rows_by_modality"] == {"forecast": 1, "group": 1}
    assert table["<missing>"]["rejected_eligible"] == {"unresolved_fuel": 1}


def test_merge_adds_files():
    one, _ = account(pd.DataFrame([row()]))
    two, _ = account(pd.DataFrame([row(din_instante="2026-02-01 00:00:00"), row(id_ons="N", val_geracao=None)]))
    total = merge([one, two])["Carvão"]
    assert (total["raw_rows"], total["accepted_rows"], total["accepted_mwh"]) == (3, 2, 20.0)
    assert total["rejected_eligible"] == {"missing_generation": 1}


def test_gaps_join_across_monthly_files_and_skip_groups():
    def hourly(start, hours, **changes):
        times = pd.date_range(start, periods=hours, freq="h").astype(str)
        return pd.DataFrame([row(din_instante=t, val_geracao=None, **changes) for t in times])

    july = pd.concat([hourly("2026-07-31 22:00", 2), hourly("2026-07-31 22:00", 2, id_ons="GRP", cod_modalidadeoperacao="TIPO II-C")])
    august = pd.concat([hourly("2026-08-01 00:00", 3), pd.DataFrame([row(din_instante="2026-08-01 05:00:00", val_geracao=None)])])
    joined = join_gaps([account(july)[1], account(august)[1]])
    hour = 3_600_000
    start = int(pd.Timestamp("2026-07-31 22:00").value // 1_000_000)
    assert [(g["ons_plant_id"], g["start_ms"], g["end_ms"], g["hours"]) for g in joined] == [
        ("PLANT", start, start + 5 * hour, 5),
        ("PLANT", start + 7 * hour, start + 8 * hour, 1),
    ]
