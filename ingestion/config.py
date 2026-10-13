"""Forme d'une connexion WebSocket et d'une requête REST planifiée."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class StreamGroup:
    """Une connexion WebSocket et les flux qu'elle transporte.

    Attributes:
        name: Nom du groupe, passé en argument à run_ws.
        market: "spot" ou "futures" ; entre dans le nom des topics.
        base_url: Adresse WebSocket de l'exchange.
        streams: Flux à recevoir, dans la syntaxe de l'exchange.
        max_reconnect_delay: Plafond du backoff de reconnexion, en secondes.
    """

    name: str
    market: str
    base_url: str
    streams: tuple[str, ...]
    max_reconnect_delay: float = 60.0


@dataclass(frozen=True)
class RestJob:
    """Une requête REST répétée à intervalle fixe.

    Attributes:
        kind: Type neutre de la donnée ; entre dans le nom du topic.
        symbol: Symbole concerné ; sert de clé Kafka.
        url: Adresse complète de l'endpoint.
        params: Paramètres de la requête.
        interval_s: Intervalle entre deux appels réussis, en secondes.
    """

    kind: str
    symbol: str
    url: str
    params: dict = field(default_factory=dict)
    interval_s: float = 10.0
