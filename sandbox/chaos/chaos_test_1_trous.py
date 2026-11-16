"""Chaos test n°1 (02/11) : le sink arrêté de 00:20:48 à ~00:29:44 UTC a-t-il perdu des trades ?

Usage (depuis la racine du dépôt) :
    python3 -m sandbox.chaos.chaos_test_1_trous
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from quality.continuite import bilan_continuite, trouver_trous

V1_DIR = Path("data/analyse/futures.trade.v1/date=2026-11-02")
PANNE = ("2026-11-02 00:15:00", "2026-11-02 00:40:00")


def heure(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def dans_panne(ms: int) -> bool:
    debut, fin = (
        datetime.fromisoformat(h).replace(tzinfo=timezone.utc).timestamp() * 1000 for h in PANNE
    )
    return debut <= ms <= fin


def main() -> int:
    if not any(V1_DIR.glob("*.parquet")):
        print(f"Données absentes : {V1_DIR}")
        return 2

    con = duckdb.connect()
    source = f"read_parquet('{V1_DIR}/*.parquet', union_by_name=true)"

    print("== Continuité par symbole (journée du 02/11)")
    for b in bilan_continuite(con, source):
        print(f"  {b.symbol:<10} ids {b.premier_id}..{b.dernier_id} : "
              f"{b.distincts:,} reçus / {b.attendus:,} attendus -> {b.manquants:,} manquants")

    trous = trouver_trous(con, source)
    print(f"== {len(trous)} trou(s)")
    for t in trous:
        zone = "PANNE" if dans_panne(t.avant_ms) or dans_panne(t.apres_ms) else "hors panne"
        print(f"  {t.symbol:<10} {t.manquants:>6} manquants entre {heure(t.avant_ms)} "
              f"et {heure(t.apres_ms)} UTC [{zone}]")

    perdus_panne = sum(t.manquants for t in trous if dans_panne(t.avant_ms) or dans_panne(t.apres_ms))
    print("Fenêtre de panne : AUCUNE PERTE ✅" if perdus_panne == 0
          else f"Fenêtre de panne : {perdus_panne} trades perdus ❌")
    return 0 if perdus_panne == 0 else 1


if __name__ == "__main__":
    sys.exit(main())