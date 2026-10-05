"""Gestion de l'arrêt propre du consumer via signaux système.

Classe avec état : 'should_stop' est un drapeau piloté par les signaux
SIGINT (Ctrl+C) et SIGTERM (docker stop). C'est l'unique source de
vérité pour décider d'un arrêt volontaire, fiable sur Linux et VPS.
"""

import logging
import signal

logger = logging.getLogger(__name__)


class ShutdownHandler:
    """Capte les signaux d'arrêt et expose un drapeau de sortie.

    Tant qu'aucun signal n'est reçu, 'should_stop' reste False et la
    boucle de reconnexion continue. À la réception de SIGINT ou SIGTERM,
    le drapeau passe à True et la connexion est fermée pour interrompre
    run_forever immédiatement.

    Attributes:
        should_stop: True dès qu'un signal d'arrêt a été reçu.
        wsapp: Connexion WebSocket à fermer pour interrompre run_forever.
    """

    def __init__(self, wsapp=None) -> None:
        """Initialise le drapeau et enregistre les handlers de signaux.

        Args:
            wsapp: Connexion WebSocket à fermer lors de l'arrêt (optionnel,
                peut être assignée après création).
        """
        self.should_stop: bool = False
        self.wsapp = wsapp
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame) -> None:
        """Handler commun appelé à la réception d'un signal d'arrêt.

        Args:
            signum: Numéro du signal reçu (SIGINT ou SIGTERM).
            frame: Frame d'exécution courante (non utilisée).
        """
        self.should_stop = True
        logger.info("Signal %s reçu — arrêt demandé", signal.Signals(signum).name)
        if self.wsapp is not None:
            self.wsapp.close()