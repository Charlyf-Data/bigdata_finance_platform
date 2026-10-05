"""Callbacks WebSocket pour le stream Binance.

Les callbacks ne pilotent pas l'arrêt : ils réagissent aux événements
et journalisent. À l'ouverture d'une connexion réussie, le compteur de
reconnexion est remis à zéro via le ReconnectHandler injecté.
"""

import logging
import json
from reconnect import ReconnectHandler
from transform import transrorm_kline
from typing import Callable  
logger = logging.getLogger(__name__)


class BinanceCallbacks:
    """Gère les événements du cycle de vie de la connexion WebSocket.

    Reçoit un ReconnectHandler pour réinitialiser le backoff dès qu'une
    connexion s'établit avec succès.
    """

    def __init__(self, reconnect: ReconnectHandler, on_record: Callable[[dict], None]) -> None:
        """Initialise les callbacks avec le gestionnaire de reconnexion.

        Args:
            reconnect: Gestionnaire de backoff à réinitialiser à l'ouverture.
            on_record: Fonction à appeler lorsqu'un enregistrement est transformé.
        """
        self.reconnect = reconnect
        self.on_record = on_record

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
        try:
            data = json.loads(message)
            if data.get("e") != "kline":
                logger.warning("Message inattendu reçu: %s", message)
                return
            record = transrorm_kline(data)
            self.on_record(record)
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Message ignoré: %s", e)


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