"""Tests de la continuité du carnet (pu = u précédent)."""

import duckdb
import pyarrow as pa

from quality.continuite import trouver_ruptures_carnet


def connexion(evenements: list[tuple[str, int, int, int]]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    table = pa.Table.from_pylist(
        [{"symbol": s, "u": u, "pu": pu, "event_time": t} for s, u, pu, t in evenements]
    )
    con.register("depth", table)
    return con


def test_chaine_continue():
    con = connexion([("BTC", 10, 5, 1), ("BTC", 15, 10, 2), ("BTC", 22, 15, 3)])
    assert trouver_ruptures_carnet(con, "depth") == []


def test_rupture_detectee_et_localisee():
    # Le message (u=30) annonce pu=20 alors que le précédent reçu avait u=15
    con = connexion([("BTC", 10, 5, 100), ("BTC", 15, 10, 200), ("BTC", 30, 20, 300)])
    (rupture,) = trouver_ruptures_carnet(con, "depth")
    assert (rupture.u_precedent, rupture.pu_recu) == (15, 20)
    assert (rupture.avant_ms, rupture.apres_ms) == (200, 300)


def test_symboles_independants():
    con = connexion([("BTC", 10, 5, 1), ("ETH", 12, 3, 1), ("BTC", 15, 10, 2), ("ETH", 20, 12, 2)])
    assert trouver_ruptures_carnet(con, "depth") == []


def test_doublon_ne_cree_pas_de_rupture():
    con = connexion([("BTC", 10, 5, 1), ("BTC", 15, 10, 2), ("BTC", 15, 10, 2), ("BTC", 22, 15, 3)])
    assert trouver_ruptures_carnet(con, "depth") == []


def test_premier_message_du_jour_sans_reference():
    # Rien avant le premier message : pas de rupture possible
    con = connexion([("BTC", 10, 999, 1)])
    assert trouver_ruptures_carnet(con, "depth") == []