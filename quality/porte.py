"""Porte qualité d'un jour de données brutes (Bronze).

Deux usages, volontairement séparés :
- `verifier_compaction` : BLOQUANT. Les fichiers compactés doivent contenir exactement
  les mêmes lignes que les fichiers minute, doublons compris. Sinon, on ne supprime rien.
  La comparaison se fait par empreinte (nombre de lignes + somme des hash de chaque ligne) :
  mémoire constante, quel que soit le volume du jour.
- `rapport_qualite` : NON BLOQUANT. Les anomalies sont signalées, mais la donnée est
  archivée quand même : une donnée avec un trou reste précieuse.

La porte ne connaît aucun exchange : les contrôles spécifiques (format du payload,
règles de continuité) lui sont fournis par un registre `type -> contrôles`,
comme le Normalizer reçoit ses NORMALIZERS.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

import duckdb

# Un contrôle reçoit une connexion et une source DuckDB, et renvoie {libellé: nombre d'anomalies}.
Controle = Callable[[duckdb.DuckDBPyConnection, str], dict[str, int]]


@dataclass(frozen=True)
class FideliteCompaction:
    colonnes_identiques: bool
    lignes_avant: int
    lignes_apres: int
    # Somme des hash de toutes les lignes : indépendante de l'ordre, sensible aux doublons
    # (une ligne en double compte deux fois). Un écart = au moins une ligne perdue ou modifiée.
    empreinte_avant: int
    empreinte_apres: int

    @property
    def fidele(self) -> bool:
        return (
            self.colonnes_identiques
            and self.lignes_avant == self.lignes_apres
            and self.empreinte_avant == self.empreinte_apres
        )


@dataclass(frozen=True)
class RapportQualite:
    kind: str
    lignes: int
    doublons: int
    anomalies: Mapping[str, int] = field(default_factory=dict)

    @property
    def alertes(self) -> tuple[str, ...]:
        messages = []
        if self.lignes == 0:
            messages.append("aucune ligne")
        if self.doublons:
            messages.append(f"{self.doublons} doublon(s)")
        messages.extend(f"{n} {libelle}" for libelle, n in self.anomalies.items() if n)
        return tuple(messages)


@dataclass(frozen=True)
class Verdict:
    """Un seul appel pour la compaction : peut-on supprimer, et que faut-il signaler ?"""

    fidelite: FideliteCompaction
    qualite: RapportQualite

    @property
    def suppression_autorisee(self) -> bool:
        # Seule la fidélité décide : un défaut déjà présent dans la donnée
        # ne se répare pas en gardant les fichiers minute.
        return self.fidelite.fidele

    @property
    def alertes(self) -> tuple[str, ...]:
        if not self.fidelite.fidele:
            return ("compaction infidèle : suppression bloquée", *self.qualite.alertes)
        return self.qualite.alertes


def _colonnes(con: duckdb.DuckDBPyConnection, source: str) -> list[str]:
    return [ligne[0] for ligne in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()]


def _liste(colonnes: list[str]) -> str:
    # Ordre alphabétique : l'empreinte ne dépend pas de l'ordre des colonnes dans le fichier
    return ", ".join(f'"{c}"' for c in sorted(colonnes))


def _empreinte(con: duckdb.DuckDBPyConnection, source: str, colonnes: list[str]) -> tuple[int, int]:
    """(nombre de lignes, somme des hash des lignes) en une seule passe, mémoire constante."""
    lignes, somme = con.execute(
        f"SELECT count(*), coalesce(sum(hash({_liste(colonnes)})::HUGEINT), 0) FROM {source}"
    ).fetchall()[0]
    return lignes, int(somme)


def verifier_compaction(
    con: duckdb.DuckDBPyConnection, avant: str, apres: str
) -> FideliteCompaction:
    """Compare, doublons compris, les sources avant et après compaction."""
    cols_avant, cols_apres = _colonnes(con, avant), _colonnes(con, apres)
    identiques = sorted(cols_avant) == sorted(cols_apres)
    lignes_avant, empreinte_avant = _empreinte(con, avant, cols_avant)
    lignes_apres, empreinte_apres = _empreinte(con, apres, cols_apres)
    return FideliteCompaction(identiques, lignes_avant, lignes_apres, empreinte_avant, empreinte_apres)


def rapport_qualite(
    con: duckdb.DuckDBPyConnection,
    source: str,
    kind: str,
    controles: Mapping[str, Sequence[Controle]],
) -> RapportQualite:
    """Rapport non bloquant : lignes, doublons, puis les contrôles enregistrés pour ce type."""
    colonnes = _colonnes(con, source)
    lignes, distinctes = con.execute(
        f"SELECT count(*), count(DISTINCT hash({_liste(colonnes)})) FROM {source}"
    ).fetchall()[0]
    doublons = lignes - distinctes  # par hash : 8 octets par ligne au lieu de la ligne entière
    anomalies: dict[str, int] = {}
    for controle in controles.get(kind, ()):
        anomalies |= controle(con, source)
    return RapportQualite(kind, lignes, doublons, anomalies)


def controler_jour(
    con: duckdb.DuckDBPyConnection,
    avant: str,
    apres: str,
    kind: str,
    controles: Mapping[str, Sequence[Controle]],
) -> Verdict:
    """Point d'entrée unique de la compaction pour un jour et un topic."""
    return Verdict(
        fidelite=verifier_compaction(con, avant, apres),
        qualite=rapport_qualite(con, apres, kind, controles),
    )