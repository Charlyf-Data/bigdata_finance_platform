"""Arrêt propre sur SIGINT (Ctrl+C) et SIGTERM (docker stop), attente interruptible."""

import logging
import signal
import threading
from collections.abc import Callable

logger = logging.getLogger(__name__)


class ShutdownHandler:
    """Capte les signaux d'arrêt et prévient ceux qui se sont inscrits.

    Il ne connaît aucun composant : le WebSocket s'inscrit avec
    on_stop(wsapp.close), le poller REST n'a rien à inscrire.
    """

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._on_stop: list[Callable[[], None]] = []
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    @property
    def should_stop(self) -> bool:
        return self._stop.is_set()

    def wait(self, seconds: float) -> bool:
        """Attend `seconds`, mais se réveille dès qu'un arrêt est demandé.

        Returns:
            True si un arrêt a été demandé.
        """
        return self._stop.wait(max(0.0, seconds))

    def on_stop(self, callback: Callable[[], None]) -> None:
        """Inscrit une action à exécuter à l'arrêt."""
        self._on_stop.append(callback)

    def _handle(self, signum, frame) -> None:
        logger.info("Signal %s reçu — arrêt demandé", signal.Signals(signum).name)
        self._stop.set()
        for callback in self._on_stop:
            callback()
