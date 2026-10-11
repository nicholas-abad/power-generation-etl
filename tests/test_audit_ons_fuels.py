"""The independent ONS fuel audit applies the documented selection policy."""

from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from audit_ons_fuels import account, merge  # noqa: E402


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
    table = account(df)
    coal = table["Carvão"]
    assert coal["raw_rows"] == 11
    assert coal["accepted_rows"] == 3  # PLANT, E, G
    assert coal["accepted_mwh"] == 30.0
    assert coal["rows_by_modality"] == {"individual": 10, "unresolved": 1}
    assert coal["rejected_eligible"] == {
        "duplicate_observation": 2,
        "invalid_generation": 2,
        "invalid_timestamp": 1,
        "unresolved_identity": 2,
    }
    assert table["Eólica"]["accepted_rows"] == 0
    assert table["Eólica"]["rows_by_modality"] == {"forecast": 1, "group": 1}
    assert table["<missing>"]["rejected_eligible"] == {"unresolved_fuel": 1}


def test_merge_adds_files():
    one = account(pd.DataFrame([row()]))
    two = account(pd.DataFrame([row(din_instante="2026-02-01 00:00:00"), row(id_ons="N", val_geracao=None)]))
    total = merge([one, two])["Carvão"]
    assert (total["raw_rows"], total["accepted_rows"], total["accepted_mwh"]) == (3, 2, 20.0)
    assert total["rejected_eligible"] == {"invalid_generation": 1}
