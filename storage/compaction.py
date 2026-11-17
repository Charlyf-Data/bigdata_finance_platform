"""Compaction d'un jour de Bronze : des milliers de fichiers minute -> un fichier ZSTD.

Garanties :
- streaming : au plus `taille_lot` lignes en mémoire, quel que soit le volume du jour ;
- schéma explicite : les types ne sont plus devinés fichier par fichier ;
- atomique : écriture dans un fichier caché temporaire, renommage seulement après contrôle ;
- sans perte : les fichiers minute ne sont supprimés que si la porte qualité juge la
  compaction fidèle (mêmes lignes, doublons compris) ;
- idempotent : relancer sur un jour déjà compacté ne change rien.
"""

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quality.porte import Verdict

# Schéma des enveloppes brutes (Bronze). Le payload Binance reste en texte JSON.
SCHEMA_RAW = pa.schema(
    [
        ("exchange", pa.string()),
        ("market", pa.string()),
        ("kind", pa.string()),
        ("symbol", pa.string()),
        ("received_at", pa.int64()),
        ("payload", pa.string()),
    ]
)

# (connexion, source avant, source après, kind) -> Verdict
Verificateur = Callable[[duckdb.DuckDBPyConnection, str, str, str], Verdict]


class Statut(Enum):
    COMPACTE = "compacté"
    DEJA_FAIT = "déjà compacté"
    REPRISE = "reprise après interruption"
    VIDE = "aucun fichier"
    BLOQUE = "bloqué : rien supprimé"


@dataclass(frozen=True)
class Resultat:
    dossier: Path
    statut: Statut
    fichiers_source: int = 0
    octets_avant: int = 0
    octets_apres: int = 0
    alertes: tuple[str, ...] = ()

    @property
    def ratio(self) -> float | None:
        return self.octets_apres / self.octets_avant if self.octets_avant else None


def nom_compacte(dossier_jour: Path) -> str:
    """`date=2026-11-02` -> `compacte-2026-11-02.parquet`."""
    return f"compacte-{dossier_jour.name.removeprefix('date=')}.parquet"


def _source(fichiers: Sequence[Path]) -> str:
    liste = ", ".join("'" + str(f).replace("'", "''") + "'" for f in fichiers)
    # hive_partitioning=false : sinon DuckDB ajoute une colonne `date` tirée du nom du dossier
    return f"read_parquet([{liste}], union_by_name=true, hive_partitioning=false)"


def _conformer(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Impose le schéma : colonnes dans l'ordre, types forcés, colonne absente = nulls."""
    colonnes = [
        table[champ.name].cast(champ.type)
        if champ.name in table.column_names
        else pa.nulls(table.num_rows, champ.type)
        for champ in schema
    ]
    return pa.Table.from_arrays(colonnes, schema=schema)


def _fsync(chemin: Path) -> None:
    fd = os.open(chemin, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _ecrire(fichiers: Sequence[Path], cible: Path, schema: pa.Schema, taille_lot: int) -> None:
    with pq.ParquetWriter(cible, schema, compression="zstd") as writer:
        for fichier in fichiers:
            for lot in pq.ParquetFile(fichier).iter_batches(batch_size=taille_lot):
                writer.write_table(_conformer(pa.Table.from_batches([lot]), schema))
    _fsync(cible)


def _supprimer(fichiers: Sequence[Path], dossier: Path) -> None:
    for fichier in fichiers:
        fichier.unlink()
    _fsync(dossier)


def compacter_jour(
    dossier_jour: Path,
    kind: str,
    verifier: Verificateur,
    schema: pa.Schema = SCHEMA_RAW,
    taille_lot: int = 50_000,
    memoire_duckdb: str = "300MB",
    threads_duckdb: int = 2,
) -> Resultat:
    """Compacte un dossier `<topic>/date=YYYY-MM-DD` fermé (le sink n'y écrit plus)."""
    final = dossier_jour / nom_compacte(dossier_jour)
    temporaire = dossier_jour / f".{final.name}.tmp"  # caché : ignoré par les lectures *.parquet
    minutes = sorted(f for f in dossier_jour.glob("part-*.parquet"))
    temporaire.unlink(missing_ok=True)  # reste d'une exécution interrompue

    if not minutes:
        statut = Statut.DEJA_FAIT if final.exists() else Statut.VIDE
        return Resultat(dossier_jour, statut)

    octets_avant = sum(f.stat().st_size for f in minutes)
    # La mémoire de DuckDB est partagée entre ses threads : on fixe leur nombre pour que
    # la consommation ne dépende pas de la machine (16 threads sur le PC, 8 sur le serveur).
    con = duckdb.connect(
        config={
            "memory_limit": memoire_duckdb,
            "threads": threads_duckdb,
            "preserve_insertion_order": False,
            "temp_directory": str(dossier_jour.parent / ".duckdb_tmp"),
        }
    )
    try:
        if final.exists():
            # Interruption entre le renommage et la suppression : on revérifie avant de supprimer
            verdict = verifier(con, _source(minutes), _source([final]), kind)
            statut_ok = Statut.REPRISE
        else:
            _ecrire(minutes, temporaire, schema, taille_lot)
            verdict = verifier(con, _source(minutes), _source([temporaire]), kind)
            statut_ok = Statut.COMPACTE
    except Exception as erreur:  # noqa: BLE001 - tout échec doit bloquer, jamais supprimer
        temporaire.unlink(missing_ok=True)
        return Resultat(dossier_jour, Statut.BLOQUE, len(minutes), octets_avant, 0,
                        (f"erreur : {type(erreur).__name__}: {erreur}",))
    finally:
        con.close()

    if not verdict.suppression_autorisee:
        temporaire.unlink(missing_ok=True)
        return Resultat(dossier_jour, Statut.BLOQUE, len(minutes), octets_avant, 0, verdict.alertes)

    if statut_ok is Statut.COMPACTE:
        os.replace(temporaire, final)  # atomique sur un même système de fichiers
        _fsync(dossier_jour)
    _supprimer(minutes, dossier_jour)
    return Resultat(
        dossier_jour, statut_ok, len(minutes), octets_avant, final.stat().st_size, verdict.alertes
    )

