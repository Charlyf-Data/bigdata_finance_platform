"""Preuve ponctuelle : le v1 de prod du 02/11 se recalcule à l'identique depuis le raw.

Usage (depuis la racine du dépôt) :
    python3 -m sandbox.determinisme.preuve_2026_11_02
"""

import sys
from pathlib import Path

import duckdb
import pyarrow as pa

from exchanges.binance.normalize import NORMALIZERS
from processing.replay import lire_enveloppes, rejouer
from quality.comparaison import comparer

JOUR = "2026-11-02"
RAW_DIR = Path(f"data/analyse/binance.futures.trade.raw/date={JOUR}")
V1_DIR = Path(f"data/analyse/futures.trade.v1/date={JOUR}")
TOPIC_V1 = "futures.trade.v1"
COLONNES = [
    "exchange", "market", "symbol", "trade_id", "price", "quantity",
    "aggressor_side", "trade_time", "event_time",
]
# Marge autour de minuit : le raw est rangé par heure de réception, le v1 par heure d'événement
FENETRE = (
    f"event_time BETWEEN epoch_ms(TIMESTAMP '{JOUR} 00:05:00') "
    f"AND epoch_ms(TIMESTAMP '{JOUR} 23:55:00')"
)


def main() -> int:
    fichiers_raw = sorted(RAW_DIR.glob("*.parquet"))
    if not fichiers_raw or not any(V1_DIR.glob("*.parquet")):
        print(f"Données absentes : {RAW_DIR} ou {V1_DIR}")
        return 2

    produits, bilan = rejouer(lire_enveloppes(fichiers_raw), NORMALIZERS, frozenset({TOPIC_V1}))
    print(f"Rejeu : {bilan.lus:,} lus, {bilan.normalises:,} normalisés, {bilan.rejetes:,} rejetés")

    con = duckdb.connect()
    con.register("rejeu", pa.Table.from_pylist([record for _, record in produits]))
    reel = f"read_parquet('{V1_DIR}/*.parquet', union_by_name=true)"
    ecart = comparer(
        con,
        f"(SELECT * FROM rejeu WHERE {FENETRE})",
        f"(SELECT * FROM {reel} WHERE {FENETRE})",
        COLONNES,
    )

    print(f"Fenêtre {JOUR} 00:05 -> 23:55 UTC")
    print(f"  rejeu : {ecart.lignes_a:,} lignes, {ecart.doublons_a:,} doublons")
    print(f"  réel  : {ecart.lignes_b:,} lignes, {ecart.doublons_b:,} doublons")
    print(f"  rejeu sauf réel : {ecart.a_sauf_b:,} | réel sauf rejeu : {ecart.b_sauf_a:,}")
    print("DÉTERMINISTE ✅" if ecart.identiques else "ÉCART ❌")
    return 0 if ecart.identiques else 1


if __name__ == "__main__":
    sys.exit(main())