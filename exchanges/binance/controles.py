"""Contrôles qualité spécifiques à Binance, branchés sur la porte générique.

Seul ce module connaît le format du payload Binance stocké dans le Bronze.
"""

from collections.abc import Sequence

import duckdb

from quality.continuite import trouver_ruptures_carnet, trouver_trous
from quality.porte import Controle


def trades_manquants(con: duckdb.DuckDBPyConnection, source: str) -> dict[str, int]:
    """aggTrade : l'identifiant `a` augmente de 1 en 1 par symbole."""
    vue = (
        "(SELECT symbol, CAST(payload ->> '$.a' AS BIGINT) AS trade_id, "
        "CAST(payload ->> '$.T' AS BIGINT) AS trade_time "
        f"FROM {source} WHERE kind = 'trade')"
    )
    return {"trade(s) manquant(s)": sum(t.manquants for t in trouver_trous(con, vue))}


def ruptures_carnet_futures(con: duckdb.DuckDBPyConnection, source: str) -> dict[str, int]:
    """Diff depth futures : chaque `pu` doit égaler le `u` précédent (le spot n'a pas de `pu`)."""
    vue = (
        "(SELECT symbol, CAST(payload ->> '$.u' AS BIGINT) AS u, "
        "CAST(payload ->> '$.pu' AS BIGINT) AS pu, "
        "CAST(payload ->> '$.E' AS BIGINT) AS event_time "
        f"FROM {source} WHERE kind = 'depth' AND market = 'futures')"
    )
    return {"rupture(s) de carnet": len(trouver_ruptures_carnet(con, vue))}


# Type neutre -> contrôles. La porte ne connaît que ce dictionnaire.
CONTROLES: dict[str, Sequence[Controle]] = {
    "trade": (trades_manquants,),
    "depth": (ruptures_carnet_futures,),
}