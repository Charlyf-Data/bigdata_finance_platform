"""Chaos test n°1 (02/11) : la chaîne du carnet futures est-elle intacte ?

Règle Binance (diff depth futures) : chaque message annonce dans `pu` le `u` du
message précédent. Si pu != u précédent, des mises à jour du carnet ont été perdues.

Usage (depuis la racine du dépôt) :
    python3 -m sandbox.chaos.chaos_test_1_carnet
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from quality.continuite import trouver_ruptures_carnet

RAW_DIR = Path("data/analyse/binance.futures.depth.raw/date=2026-11-02")
PANNE = ("2026-11-02 00:15:00", "2026-11-02 00:40:00")


def heure(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def dans_panne(ms: int) -> bool:
    debut, fin = (
        datetime.fromisoformat(h).replace(tzinfo=timezone.utc).timestamp() * 1000 for h in PANNE
    )
    return debut <= ms <= fin


def main() -> int:
    if not any(RAW_DIR.glob("*.parquet")):
        print(f"Données absentes : {RAW_DIR}")
        return 2

    con = duckdb.connect()
    # Le Bronze garde le payload Binance en texte JSON : on en extrait u, pu et E
    con.execute(
        f"""
        CREATE VIEW depth AS
        SELECT symbol,
               CAST(payload ->> '$.u'  AS BIGINT) AS u,
               CAST(payload ->> '$.pu' AS BIGINT) AS pu,
               CAST(payload ->> '$.E'  AS BIGINT) AS event_time
        FROM read_parquet('{RAW_DIR}/*.parquet', union_by_name=true)
        WHERE kind = 'depth'
        """
    )
    total, sans_pu = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE pu IS NULL) FROM depth"
    ).fetchall()[0]
    print(f"== {total:,} messages depth, {sans_pu:,} sans pu")

    ruptures = trouver_ruptures_carnet(con, "depth")
    print(f"== {len(ruptures)} rupture(s) de chaîne")
    for r in ruptures:
        zone = "PANNE" if dans_panne(r.avant_ms) or dans_panne(r.apres_ms) else "hors panne"
        print(f"  {r.symbol:<10} u précédent {r.u_precedent} != pu {r.pu_recu} "
              f"entre {heure(r.avant_ms)} et {heure(r.apres_ms)} UTC [{zone}]")

    en_panne = [r for r in ruptures if dans_panne(r.avant_ms) or dans_panne(r.apres_ms)]
    print("Fenêtre de panne : CARNET INTACT ✅" if not en_panne
          else f"Fenêtre de panne : {len(en_panne)} rupture(s) ❌")
    return 0 if not en_panne else 1


if __name__ == "__main__":
    sys.exit(main())