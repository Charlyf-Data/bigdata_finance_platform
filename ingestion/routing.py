"""Aiguillage d'un événement vers son topic brut."""

import logging
import time
from collections.abc import Callable

from common.events import Event

logger = logging.getLogger(__name__)


class Router:
    """Choisit le topic brut, la clé, et publie le message natif dans son enveloppe.

    Topic : <exchange>.<marché>.<type>.raw, qui doit figurer dans topics.txt.
    """

    def __init__(
        self,
        exchange: str,
        market: str,
        known_topics: frozenset[str],
        publish: Callable[[str, str, dict], None],
    ) -> None:
        self.exchange = exchange
        self.market = market
        self.known_topics = known_topics
        self.publish = publish
        self._reported: set[str] = set()

    def dispatch(self, event: Event) -> None:
        topic = f"{self.exchange}.{self.market}.{event.kind}.raw"
        if topic not in self.known_topics:
            if topic not in self._reported:  # un seul log par topic inconnu
                logger.error("Topic %s absent de topics.txt : messages ignorés", topic)
                self._reported.add(topic)
            return

        envelope = {
            "exchange": self.exchange,
            "market": self.market,
            "kind": event.kind,
            "symbol": event.symbol,
            "received_at": int(time.time() * 1000),
            "payload": event.payload,
        }
        self.publish(topic, event.symbol, envelope)
