"""Tests de la comparaison A / B."""

import duckdb
import pyarrow as pa
import pytest

from quality.comparaison import comparer

COLONNES = ["id", "prix"]


def connexion(a: list[dict], b: list[dict]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.register("a", pa.Table.from_pylist(a))
    con.register("b", pa.Table.from_pylist(b))
    return con


def test_sources_identiques():
    lignes = [{"id": 1, "prix": "10"}, {"id": 2, "prix": "11"}]
    ecart = comparer(connexion(lignes, lignes), "a", "b", COLONNES)
    assert ecart.identiques and ecart.lignes_a == ecart.lignes_b == 2


def test_ecart_detecte_dans_les_deux_sens():
    a = [{"id": 1, "prix": "10"}, {"id": 2, "prix": "11"}]
    b = [{"id": 1, "prix": "10"}, {"id": 3, "prix": "12"}]
    ecart = comparer(connexion(a, b), "a", "b", COLONNES)
    assert not ecart.identiques
    assert ecart.a_sauf_b == 1 and ecart.b_sauf_a == 1


def test_doublons_comptes_a_part():
    a = [{"id": 1, "prix": "10"}, {"id": 1, "prix": "10"}]
    b = [{"id": 1, "prix": "10"}]
    ecart = comparer(connexion(a, b), "a", "b", COLONNES)
    assert ecart.identiques  # mêmes lignes distinctes
    assert ecart.doublons_a == 1 and ecart.doublons_b == 0


def test_colonnes_hors_comparaison_ignorees():
    a = [{"id": 1, "prix": "10", "recu": 100}]
    b = [{"id": 1, "prix": "10", "recu": 999}]
    assert comparer(connexion(a, b), "a", "b", COLONNES).identiques


def test_sous_requete_acceptee():
    a = [{"id": 1, "prix": "10"}, {"id": 2, "prix": "11"}]
    b = [{"id": 1, "prix": "10"}]
    ecart = comparer(connexion(a, b), "(SELECT * FROM a WHERE id = 1)", "b", COLONNES)
    assert ecart.identiques


def test_colonnes_vides_refusees():
    with pytest.raises(ValueError):
        comparer(duckdb.connect(), "a", "b", [])