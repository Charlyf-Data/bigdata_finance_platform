"""Rejeu Bronze -> v1 : rejoue des enveloppes brutes dans le vrai Normalizer.

Le coeur (`rejouer`) est pur : aucune lecture de fichier, aucune horloge.
La lecture du Parquet brut est le travail de l'adaptateur `lire_enveloppes`.
"""

import json
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from processing.normalizer import Normalizer

COLONNES_ENVELOPPE = ["kind", "market", "symbol", "payload"]


@dataclass(frozen=True)
class BilanRejeu:
    """Preuve chiffrée d'un rejeu : immuable une fois calculée."""

    lus: int
    normalises: int

    @property
    def rejetes(self) -> int:
        # Calculé, jamais stocké : impossible d'avoir lus != normalises + rejetes
        return self.lus - self.normalises


def rejouer(
    enveloppes: Iterable[dict],
    normalizers: dict[str, Callable[[dict, str, str], dict]],
    topics: frozenset[str],
) -> tuple[list[tuple[str, dict]], BilanRejeu]:
    """Rejoue des enveloppes brutes et renvoie les (topic, record v1) produits + le bilan.

    Chaque enveloppe doit contenir kind, market, symbol et un payload déjà décodé (dict).
    """
    produits: list[tuple[str, dict]] = []

    def publish(topic: str, cle: str, record: dict) -> None:
        produits.append((topic, record))

    normalizer = Normalizer(normalizers, topics, publish)
    lus = 0
    for enveloppe in enveloppes:
        lus += 1
        normalizer.handle(enveloppe)
    return produits, BilanRejeu(lus=lus, normalises=len(produits))


def lire_enveloppes(fichiers: Iterable[Path], taille_lot: int = 10_000) -> Iterator[dict]:
    """Adaptateur : lit des Parquet bruts par lots et décode le payload JSON.

    Générateur : au plus `taille_lot` lignes en mémoire à la fois.
    """
    for fichier in fichiers:
        parquet = pq.ParquetFile(fichier)
        for lot in parquet.iter_batches(batch_size=taille_lot, columns=COLONNES_ENVELOPPE):
            for ligne in lot.to_pylist():
                yield {**ligne, "payload": json.loads(ligne["payload"])}