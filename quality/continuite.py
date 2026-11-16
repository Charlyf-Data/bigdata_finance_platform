"""Continuité des identifiants séquentiels : détecte les trous (messages perdus).

Binance numérote les aggTrades de 1 en 1, par symbole : un saut dans `trade_id`
signifie qu'au moins un trade n'a jamais été enregistré.
"""

from dataclasses import dataclass

import duckdb


@dataclass(frozen=True)
class Trou:
    symbol: str
    premier_manquant: int
    dernier_manquant: int
    avant_ms: int  # trade_time du dernier trade reçu avant le trou
    apres_ms: int  # trade_time du premier trade reçu après le trou

    @property
    def manquants(self) -> int:
        return self.dernier_manquant - self.premier_manquant + 1


@dataclass(frozen=True)
class Continuite:
    symbol: str
    premier_id: int
    dernier_id: int
    distincts: int

    @property
    def attendus(self) -> int:
        return self.dernier_id - self.premier_id + 1

    @property
    def manquants(self) -> int:
        return self.attendus - self.distincts


def _ids(source: str, colonne_id: str, colonne_temps: str) -> str:
    # Un id par ligne, dédoublonné : les doublons ne doivent pas masquer un trou
    return (
        f"SELECT symbol, CAST({colonne_id} AS BIGINT) AS id, min({colonne_temps}) AS t "
        f"FROM {source} GROUP BY symbol, CAST({colonne_id} AS BIGINT)"
    )


def bilan_continuite(
    con: duckdb.DuckDBPyConnection,
    source: str,
    colonne_id: str = "trade_id",
    colonne_temps: str = "trade_time",
) -> list[Continuite]:
    """Par symbole : premier et dernier id, nombre d'ids distincts."""
    lignes = con.execute(
        f"SELECT symbol, min(id), max(id), count(*) "
        f"FROM ({_ids(source, colonne_id, colonne_temps)}) GROUP BY symbol ORDER BY symbol"
    ).fetchall()
    return [Continuite(*ligne) for ligne in lignes]


def trouver_trous(
    con: duckdb.DuckDBPyConnection,
    source: str,
    colonne_id: str = "trade_id",
    colonne_temps: str = "trade_time",
) -> list[Trou]:
    """Liste des trous, par symbole, dans l'ordre des ids."""
    lignes = con.execute(
        f"""
        WITH ids AS ({_ids(source, colonne_id, colonne_temps)}),
        suite AS (
            SELECT symbol, id, t,
                   lag(id) OVER (PARTITION BY symbol ORDER BY id) AS id_prec,
                   lag(t)  OVER (PARTITION BY symbol ORDER BY id) AS t_prec
            FROM ids
        )
        SELECT symbol, id_prec + 1, id - 1, t_prec, t
        FROM suite
        WHERE id - id_prec > 1
        ORDER BY symbol, id
        """
    ).fetchall()
    return [Trou(*ligne) for ligne in lignes]