"""Cycle de vie d'une connexion WebSocket, indépendant de l'exchange."""

import logging
from collections.abc import Callable

from common.errors import InvalidMessage
from common.events import Event
from ingestion.adapter import ExchangeAdapter

logger = logging.getLogger(__name__)


class StreamCallbacks:
    """Réagit aux événements de websocket-client et délègue le parsing.

    Il ne connaît ni Kafka, ni les topics, ni le format de l'exchange :
    l'adaptateur découpe, on_event reçoit chaque événement.
    """

    def __init__(
        self,
        adapter: ExchangeAdapter,
        subscribe: list[str],
        on_event: Callable[[Event], None],
        on_connected: Callable[[], None],
    ) -> None:
        self.adapter = adapter
        self.subscribe = subscribe
        self.on_event = on_event
        self.on_connected = on_connected

    def on_open(self, wsapp) -> None:
        self.on_connected()
        for message in self.subscribe:
            wsapp.send(message)
        logger.info("Connexion établie (%s)", self.adapter.name)

    def on_message(self, wsapp, message: str) -> None:
        try:
            events = self.adapter.parse(message)
        except InvalidMessage as e:
            logger.warning("Message ignoré : %s", e)
            return
        for event in events:
            self.on_event(event)  # hors du try : une erreur Kafka reste visible

    def on_error(self, wsapp, error) -> None:
        logger.error("Erreur WebSocket : %s", error)

    def on_close(self, wsapp, close_status_code, close_msg) -> None:
        logger.info("Connexion fermée — code : %s", close_status_code)

    def on_ping(self, wsapp, message) -> None:
        logger.debug("Ping reçu — pong automatique")

    def on_pong(self, wsapp, message) -> None:
        logger.debug("Pong reçu")
