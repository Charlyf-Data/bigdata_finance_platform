"""Point d'entrée du consumer Binance.

Assemble la configuration, les callbacks, le gestionnaire de
reconnexion et le gestionnaire d'arrêt, puis lance la boucle de
connexion résiliente pilotée par les signaux système.
"""

import logging

import websocket
from callbacks import BinanceCallbacks
from config import BinanceConfig
from reconnect import ReconnectHandler
from shutdown import ShutdownHandler
from kafka_publisher import KafkaPublisher    

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


def run() -> None:
    """Lance le consumer et maintient la connexion via backoff.

    La boucle se poursuit tant qu'aucun signal d'arrêt n'a été reçu.
    Une déconnexion subie (24h, réseau) déclenche une reconnexion
    temporisée. Un signal SIGINT ou SIGTERM provoque un arrêt propre.
    """
    config = BinanceConfig()
    reconnect = ReconnectHandler(max_delay=config.max_reconnect_delay)
    publisher = KafkaPublisher("localhost:9092", "klines.raw")
    callbacks = BinanceCallbacks(reconnect=reconnect, on_record=publisher.publish)
    shutdown = ShutdownHandler()

    wsapp = websocket.WebSocketApp(
        config.build_url(),
        on_open=callbacks.on_open,
        on_message=callbacks.on_message,
        on_error=callbacks.on_error,
        on_close=callbacks.on_close,
        on_ping=callbacks.on_ping,
        on_pong=callbacks.on_pong,
    )
    shutdown.wsapp = wsapp  

    while not shutdown.should_stop:
        try:
            wsapp.run_forever()
        except Exception as exc:
            logging.error("Erreur inattendue: %s", exc)
        finally:
            if not shutdown.should_stop:
                reconnect.wait()
    publisher.close()


if __name__ == "__main__":
    run()