"""Événement brut : l'unité qui circule de l'adaptateur d'exchange au routeur."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Event:
    """Un message d'exchange identifié, mais pas transformé.

    Attributes:
        kind: Type neutre de donnée ("kline", "trade", "booktop", "depth"...).
        symbol: Symbole natif de l'exchange ("BTCUSDT"). Sert de clé Kafka.
        payload: Message natif, intact.
    """

    kind: str
    symbol: str
    payload: dict
