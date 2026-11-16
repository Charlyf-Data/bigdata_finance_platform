"""Porte qualité d'un jour de données brutes (Bronze).

Deux usages, volontairement séparés :
- `verifier_compaction` : BLOQUANT. Les fichiers compactés doivent contenir exactement
  les mêmes lignes que les fichiers minute, doublons compris. Sinon, on ne supprime rien.
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
    avant_sauf_apres: int  # en multiensemble (EXCEPT ALL) : les doublons comptent
    apres_sauf_avant: int

    @property
    def fidele(self) -> bool:
        return (
            self.colonnes_identiques
            and self.lignes_avant == self.lignes_apres
            and self.avant_sauf_apres == 0
            and self.apres_sauf_avant == 0
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


def _compte(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return con.execute(f"SELECT count(*) FROM ({sql})").fetchall()[0][0]


def verifier_compaction(
    con: duckdb.DuckDBPyConnection, avant: str, apres: str
) -> FideliteCompaction:
    """Compare ligne à ligne, doublons compris, les sources avant et après compaction."""
    cols_avant, cols_apres = _colonnes(con, avant), _colonnes(con, apres)
    if sorted(cols_avant) != sorted(cols_apres):
        return FideliteCompaction(False, _compte(con, f"SELECT * FROM {avant}"),
                                  _compte(con, f"SELECT * FROM {apres}"), -1, -1)

    cols = ", ".join(f'"{c}"' for c in sorted(cols_avant))
    a = f"SELECT {cols} FROM {avant}"
    b = f"SELECT {cols} FROM {apres}"
    return FideliteCompaction(
        colonnes_identiques=True,
        lignes_avant=_compte(con, a),
        lignes_apres=_compte(con, b),
        avant_sauf_apres=_compte(con, f"{a} EXCEPT ALL {b}"),
        apres_sauf_avant=_compte(con, f"{b} EXCEPT ALL {a}"),
    )


def rapport_qualite(
    con: duckdb.DuckDBPyConnection,
    source: str,
    kind: str,
    controles: Mapping[str, Sequence[Controle]],
) -> RapportQualite:
    """Rapport non bloquant : lignes, doublons, puis les contrôles enregistrés pour ce type."""
    lignes = _compte(con, f"SELECT * FROM {source}")
    doublons = lignes - _compte(con, f"SELECT DISTINCT * FROM {source}")
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