"""Comparaison de deux jeux de lignes : écarts dans les deux sens et doublons.

Sert de porte qualité : preuve de rejeu, comptes avant/après compaction.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import duckdb


@dataclass(frozen=True)
class Ecart:
    """Résultat d'une comparaison A / B sur un ensemble de colonnes."""

    lignes_a: int
    lignes_b: int
    distinctes_a: int
    distinctes_b: int
    a_sauf_b: int
    b_sauf_a: int

    @property
    def identiques(self) -> bool:
        """Mêmes lignes distinctes des deux côtés (les doublons sont comptés à part)."""
        return self.a_sauf_b == 0 and self.b_sauf_a == 0

    @property
    def doublons_a(self) -> int:
        return self.lignes_a - self.distinctes_a

    @property
    def doublons_b(self) -> int:
        return self.lignes_b - self.distinctes_b


def comparer(
    con: duckdb.DuckDBPyConnection,
    source_a: str,
    source_b: str,
    colonnes: Sequence[str],
) -> Ecart:
    """Compare deux sources DuckDB (nom de table, vue ou sous-requête entre parenthèses).

    Comparaison ensembliste (EXCEPT) dans les deux sens, sur les seules colonnes données.
    """
    if not colonnes:
        raise ValueError("au moins une colonne est nécessaire")
    cols = ", ".join(f'"{c}"' for c in colonnes)
    a = f"SELECT {cols} FROM {source_a}"
    b = f"SELECT {cols} FROM {source_b}"

    def compte(sql: str) -> int:
        return con.execute(f"SELECT count(*) FROM ({sql})").fetchall()[0][0]

    return Ecart(
        lignes_a=compte(a),
        lignes_b=compte(b),
        distinctes_a=compte(f"SELECT DISTINCT * FROM ({a})"),
        distinctes_b=compte(f"SELECT DISTINCT * FROM ({b})"),
        a_sauf_b=compte(f"{a} EXCEPT {b}"),
        b_sauf_a=compte(f"{b} EXCEPT {a}"),
    )