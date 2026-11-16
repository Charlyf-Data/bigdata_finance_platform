"""Inventaire de dossiers Parquet : nb de fichiers, nb de lignes, plage horaire UTC.

Usage (depuis la racine du dépôt) :
    python3 sandbox/exploration/inventaire.py <dossier> [<dossier> ...]

Exemple :
    python3 sandbox/exploration/inventaire.py \
        data/analyse/binance.futures.trade.raw data/analyse/futures.trade.v1
"""

import sys
from pathlib import Path

import duckdb

# raw : rangé par heure de réception ; v1 : par heure d'événement Binance
COLONNES_TEMPS = ("received_at", "event_time")


def inventaire(dossier: Path) -> None:
    print(f"== {dossier}")
    # rglob : fonctionne que les fichiers soient à plat ou dans des sous-dossiers date=
    fichiers = sorted(str(f) for f in dossier.rglob("*.parquet"))
    if not fichiers:
        print("   aucun fichier .parquet")
        return

    rel = duckdb.read_parquet(fichiers, union_by_name=True)
    temps = next((c for c in COLONNES_TEMPS if c in rel.columns), None)
    if temps is None:
        lignes = rel.aggregate("count(*)").fetchall()[0][0]
        print(f"   {len(fichiers):,} fichiers, {lignes:,} lignes (aucune colonne de temps)")
        return

    lignes, debut, fin = rel.aggregate(
        f"count(*), min(make_timestamp({temps} * 1000)), max(make_timestamp({temps} * 1000))"
    ).fetchall()[0]
    print(f"   {len(fichiers):,} fichiers, {lignes:,} lignes")
    print(f"   {temps} : {debut} -> {fin} (UTC)")


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    for chemin in sys.argv[1:]:
        inventaire(Path(chemin))
    return 0


if __name__ == "__main__":
    sys.exit(main())