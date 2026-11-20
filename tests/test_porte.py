"""Tests de la porte qualité : fidélité de compaction (bloquant) et rapport (non bloquant)."""

import json

import duckdb
import pyarrow as pa

from exchanges.binance.controles import CONTROLES
from quality.porte import controler_jour, rapport_qualite, verifier_compaction


def con_avec(**tables: list[dict]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for nom, lignes in tables.items():
        con.register(nom, pa.Table.from_pylist(lignes))
    return con


def raw_trade(trade_id: int) -> dict:
    payload = {"e": "aggTrade", "E": trade_id, "a": trade_id, "p": "1", "q": "1",
               "T": trade_id, "m": False}
    return {"kind": "trade", "market": "futures", "symbol": "BTCUSDT",
            "received_at": trade_id + 5, "payload": json.dumps(payload)}


def raw_depth(u: int, pu: int) -> dict:
    payload = {"e": "depthUpdate", "E": u, "T": u, "U": pu + 1, "u": u, "pu": pu,
               "b": [], "a": []}
    return {"kind": "depth", "market": "futures", "symbol": "BTCUSDT",
            "received_at": u + 5, "payload": json.dumps(payload)}


# --- Fidélité de compaction (bloquant) ---

def test_compaction_fidele():
    lignes = [raw_trade(i) for i in range(1, 6)]
    assert verifier_compaction(con_avec(avant=lignes, apres=lignes), "avant", "apres").fidele


def test_ligne_perdue_bloque():
    avant = [raw_trade(i) for i in range(1, 6)]
    resultat = verifier_compaction(con_avec(avant=avant, apres=avant[:-1]), "avant", "apres")
    assert not resultat.fidele and resultat.lignes_avant - resultat.lignes_apres == 1


def test_doublon_perdu_bloque():
    # La compaction ne dédoublonne pas : c'est le travail du Silver
    avant = [raw_trade(1), raw_trade(1), raw_trade(2)]
    apres = [raw_trade(1), raw_trade(2)]
    assert not verifier_compaction(con_avec(avant=avant, apres=apres), "avant", "apres").fidele


def test_multiplicites_echangees_bloquent():
    # Mêmes lignes distinctes, même total, mais multiplicités différentes
    avant = [raw_trade(1), raw_trade(1), raw_trade(2)]
    apres = [raw_trade(1), raw_trade(2), raw_trade(2)]
    assert not verifier_compaction(con_avec(avant=avant, apres=apres), "avant", "apres").fidele


def test_colonne_perdue_bloque():
    avant = [raw_trade(1)]
    apres = [{k: v for k, v in raw_trade(1).items() if k != "received_at"}]
    resultat = verifier_compaction(con_avec(avant=avant, apres=apres), "avant", "apres")
    assert not resultat.fidele and not resultat.colonnes_identiques

def test_ligne_modifiee_bloque():
    # Même nombre de lignes, mais un payload altéré : seule l'empreinte le voit
    avant = [raw_trade(i) for i in range(1, 6)]
    apres = avant[:-1] + [{**raw_trade(5), "payload": "corrompu"}]
    resultat = verifier_compaction(con_avec(avant=avant, apres=apres), "avant", "apres")
    assert resultat.lignes_avant == resultat.lignes_apres and not resultat.fidele


def test_ordre_des_lignes_indifferent():
    avant = [raw_trade(i) for i in range(1, 6)]
    assert verifier_compaction(con_avec(avant=avant, apres=avant[::-1]), "avant", "apres").fidele


def test_ordre_des_colonnes_indifferent():
    avant = [raw_trade(1)]
    apres = [dict(reversed(list(raw_trade(1).items())))]
    assert verifier_compaction(con_avec(avant=avant, apres=apres), "avant", "apres").fidele


# --- Rapport qualité (non bloquant) ---

def test_rapport_trade_propre():
    con = con_avec(raw=[raw_trade(i) for i in range(1, 11)])
    rapport = rapport_qualite(con, "raw", "trade", CONTROLES)
    assert rapport.lignes == 10 and rapport.alertes == ()


def test_rapport_trade_signale_trou_et_doublon():
    con = con_avec(raw=[raw_trade(1), raw_trade(2), raw_trade(2), raw_trade(6)])
    rapport = rapport_qualite(con, "raw", "trade", CONTROLES)
    assert rapport.doublons == 1
    assert rapport.alertes == ("1 doublon(s)", "3 trade(s) manquant(s)")


def test_rapport_depth_signale_rupture():
    con = con_avec(raw=[raw_depth(10, 5), raw_depth(15, 10), raw_depth(30, 20)])
    rapport = rapport_qualite(con, "raw", "depth", CONTROLES)
    assert rapport.alertes == ("1 rupture(s) de carnet",)


def test_type_sans_controle_compte_seulement():
    con = con_avec(raw=[{**raw_trade(1), "kind": "markprice"}])
    rapport = rapport_qualite(con, "raw", "markprice", CONTROLES)
    assert rapport.lignes == 1 and rapport.alertes == ()


def test_nouveau_controle_sans_modifier_la_porte():
    # Ouvert/fermé : on étend la porte en enregistrant un contrôle, sans toucher à son code
    def toujours_deux(con, source):
        return {"anomalie(s) de test": 2}

    con = con_avec(raw=[raw_trade(1)])
    rapport = rapport_qualite(con, "raw", "trade", {"trade": (toujours_deux,)})
    assert rapport.alertes == ("2 anomalie(s) de test",)


# --- Verdict (point d'entrée unique) ---

def test_verdict_fidele_avec_trou_autorise_la_suppression_et_alerte():
    lignes = [raw_trade(1), raw_trade(2), raw_trade(6)]
    verdict = controler_jour(con_avec(avant=lignes, apres=lignes), "avant", "apres", "trade",
                             CONTROLES)
    assert verdict.suppression_autorisee
    assert verdict.alertes == ("3 trade(s) manquant(s)",)


def test_verdict_infidele_bloque_la_suppression():
    avant = [raw_trade(i) for i in range(1, 6)]
    verdict = controler_jour(con_avec(avant=avant, apres=avant[:-1]), "avant", "apres", "trade",
                             CONTROLES)
    assert not verdict.suppression_autorisee
    assert verdict.alertes[0].startswith("compaction infidèle")