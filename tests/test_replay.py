"""Tests du rejeu Bronze -> v1 : mapping, rejets, déterminisme, lecture Parquet."""

import json

import pyarrow as pa
import pyarrow.parquet as pq

from exchanges.binance.normalize import NORMALIZERS
from processing.replay import lire_enveloppes, rejouer

TOPICS = frozenset({"futures.trade.v1"})


def enveloppe_trade(trade_id: int = 3504406200, buyer_is_maker: bool = False) -> dict:
    """Enveloppe brute telle que l'ingestion la produit (payload aggTrade Binance)."""
    return {
        "kind": "trade",
        "market": "futures",
        "symbol": "BTCUSDT",
        "payload": {
            "e": "aggTrade",
            "E": 1793616028541,
            "a": trade_id,
            "s": "BTCUSDT",
            "p": "77964.80",
            "q": "0.065",
            "T": 1793616028521,
            "m": buyer_is_maker,
        },
    }


def test_mapping_trade_vers_v1():
    produits, bilan = rejouer([enveloppe_trade()], NORMALIZERS, TOPICS)

    assert bilan.lus == 1 and bilan.normalises == 1 and bilan.rejetes == 0
    topic, record = produits[0]
    assert topic == "futures.trade.v1"
    assert record == {
        "exchange": "binance",
        "market": "futures",
        "symbol": "BTCUSDT",
        "trade_id": "3504406200",  # entier Binance -> texte dans le contrat v1
        "price": "77964.80",  # prix gardé en texte : aucun arrondi
        "quantity": "0.065",
        "aggressor_side": "buy",  # m = false : l'acheteur a pris l'initiative
        "trade_time": 1793616028521,
        "event_time": 1793616028541,
    }


def test_buyer_is_maker_donne_un_vendeur_agresseur():
    produits, _ = rejouer([enveloppe_trade(buyer_is_maker=True)], NORMALIZERS, TOPICS)
    assert produits[0][1]["aggressor_side"] == "sell"


def test_message_invalide_compte_comme_rejete_sans_planter():
    invalide = enveloppe_trade()
    del invalide["payload"]["p"]  # prix manquant

    produits, bilan = rejouer([enveloppe_trade(1), invalide, enveloppe_trade(2)], NORMALIZERS, TOPICS)

    assert bilan.lus == 3 and bilan.normalises == 2 and bilan.rejetes == 1
    assert [r["trade_id"] for _, r in produits] == ["1", "2"]


def test_topic_hors_liste_est_ignore():
    produits, bilan = rejouer([enveloppe_trade()], NORMALIZERS, frozenset())
    assert produits == [] and bilan.rejetes == 1


def test_rejeu_deterministe():
    enveloppes = [enveloppe_trade(i, buyer_is_maker=i % 2 == 0) for i in range(100)]

    premier, bilan1 = rejouer(enveloppes, NORMALIZERS, TOPICS)
    second, bilan2 = rejouer(enveloppes, NORMALIZERS, TOPICS)

    assert premier == second
    assert bilan1 == bilan2


def test_lire_enveloppes_decode_le_payload_par_lots(tmp_path):
    lignes = [{**enveloppe_trade(i), "received_at": 1793616028600} for i in range(25)]
    # Comme le ParquetWriter de prod : payload stocké en texte JSON
    table = pa.Table.from_pylist([{**l, "payload": json.dumps(l["payload"])} for l in lignes])
    fichier = tmp_path / "part-test.parquet"
    pq.write_table(table, fichier)

    enveloppes = list(lire_enveloppes([fichier], taille_lot=10))  # 3 lots : 10 + 10 + 5

    assert len(enveloppes) == 25
    assert enveloppes[0]["payload"] == lignes[0]["payload"]
    assert "received_at" not in enveloppes[0]  # seules les colonnes utiles sont lues