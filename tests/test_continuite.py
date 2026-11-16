"""Tests de la détection de trous dans les identifiants séquentiels."""

import duckdb
import pyarrow as pa

from quality.continuite import bilan_continuite, trouver_trous


def connexion(lignes: list[tuple[str, int, int]]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    table = pa.Table.from_pylist(
        # trade_id en texte, comme dans le contrat v1
        [{"symbol": s, "trade_id": str(i), "trade_time": t} for s, i, t in lignes]
    )
    con.register("trades", table)
    return con


def test_suite_continue_sans_trou():
    con = connexion([("BTC", i, 1000 + i) for i in range(1, 11)])
    assert trouver_trous(con, "trades") == []
    (bilan,) = bilan_continuite(con, "trades")
    assert bilan.attendus == 10 and bilan.manquants == 0


def test_trou_localise_avec_ses_bornes_temporelles():
    con = connexion([("BTC", 1, 100), ("BTC", 2, 200), ("BTC", 6, 600), ("BTC", 7, 700)])
    (trou,) = trouver_trous(con, "trades")
    assert (trou.premier_manquant, trou.dernier_manquant, trou.manquants) == (3, 5, 3)
    assert (trou.avant_ms, trou.apres_ms) == (200, 600)


def test_symboles_independants():
    # Les ids de ETH ne doivent pas combler ni créer de trou dans BTC
    con = connexion([("BTC", 1, 1), ("BTC", 2, 2), ("ETH", 50, 1), ("ETH", 52, 2)])
    trous = trouver_trous(con, "trades")
    assert [(t.symbol, t.premier_manquant) for t in trous] == [("ETH", 51)]


def test_tri_numerique_et_non_textuel():
    # En texte, "10" < "9" : sans CAST, on inventerait un trou
    con = connexion([("BTC", i, i) for i in (8, 9, 10, 11)])
    assert trouver_trous(con, "trades") == []


def test_doublons_ne_masquent_pas_un_trou():
    con = connexion([("BTC", 1, 1), ("BTC", 1, 1), ("BTC", 3, 3)])
    (bilan,) = bilan_continuite(con, "trades")
    assert bilan.distincts == 2 and bilan.manquants == 1
    assert len(trouver_trous(con, "trades")) == 1