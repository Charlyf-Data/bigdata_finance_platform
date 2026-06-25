"""Callbacks WebSocket pour le stream Binance.

Les callbacks ne pilotent pas l'arrêt : ils réagissent aux événements
et journalisent. À l'ouverture d'une connexion réussie, le compteur de
reconnexion est remis à zéro via le ReconnectHandler injecté.
"""

import logging

from reconnect import ReconnectHandler

logger = logging.getLogger(__name__)


class BinanceCallbacks:
    """Gère les événements du cycle de vie de la connexion WebSocket.

    Reçoit un ReconnectHandler pour réinitialiser le backoff dès qu'une
    connexion s'établit avec succès.
    """

    def __init__(self, reconnect: ReconnectHandler) -> None:
        """Initialise les callbacks avec le gestionnaire de reconnexion.

        Args:
            reconnect: Gestionnaire de backoff à réinitialiser à l'ouverture.
        """
        self.reconnect = reconnect

    def on_open(self, wsapp) -> None:
        """Appelé à l'ouverture de la connexion.

        Réinitialise le compteur de tentatives : une connexion réussie
        signifie que le réseau fonctionne, on repart d'un délai minimal.

        Args:
            wsapp: Instance WebSocketApp à l'origine de l'événement.
        """
        self.reconnect.reset()
        logger.info("Connexion établie")

    def on_message(self, wsapp, message: str) -> None:
        """Appelé à chaque message reçu du serveur.

        Args:
            wsapp: Instance WebSocketApp à l'origine de l'événement.
            message: Charge utile brute du message (JSON non parsé).
        """
        logger.info("Message reçu: %s", message)

    def on_error(self, wsapp, error) -> None:
        """Appelé lors d'une erreur de connexion.

        Args:
            wsapp: Instance WebSocketApp à l'origine de l'événement.
            error: Exception ou message décrivant l'erreur.
        """
        logger.error("Erreur: %s", error)

    def on_close(self, wsapp, close_status_code, close_msg) -> None:
        """Appelé à la fermeture de la connexion.

        Journalise uniquement : la décision d'arrêter ou de reconnecter
        appartient au ShutdownHandler dans la boucle principale.

        Args:
            wsapp: Instance WebSocketApp à l'origine de l'événement.
            close_status_code: Code de fermeture WebSocket (1000 = normal).
            close_msg: Message de fermeture éventuel.
        """
        logger.info("Connexion fermée — code: %s", close_status_code)

    def on_ping(self, wsapp, message) -> None:
        """Appelé à la réception d'un ping serveur.

        Le pong est envoyé automatiquement par websocket-client.

        Args:
            wsapp: Instance WebSocketApp à l'origine de l'événement.
            message: Charge utile du ping.
        """
        logger.debug("Ping reçu — pong automatique")

    def on_pong(self, wsapp, message) -> None:
        """Appelé à la réception d'un pong serveur.

        Args:
            wsapp: Instance WebSocketApp à l'origine de l'événement.
            message: Charge utile du pong.
        """
        logger.debug("Pong reçu")