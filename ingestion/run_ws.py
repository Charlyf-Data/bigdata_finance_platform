"""Point d'entrée WebSocket : un processus par groupe de flux.

Depuis la racine du projet :
    python3 -m ingestion.run_ws spot
    python3 -m ingestion.run_ws futures-market
    python3 -m ingestion.run_ws futures-public

Changer d'exchange : remplacer les deux imports exchanges.binance ci-dessous.
"""

import argparse
import logging

import websocket
from confluent_kafka import Producer

from common.backoff import ExponentialBackoff
from common.kafka_config import KafkaConfig
from common.kafka_publisher import KafkaPublisher
from common.logging_setup import setup_logging
from common.shutdown import ShutdownHandler
from exchanges.binance.adapter import BinanceAdapter
from exchanges.binance.streams import GROUPS
from ingestion.routing import Router
from ingestion.stream_callbacks import StreamCallbacks

logger = logging.getLogger(__name__)


def run_ws(group_name: str) -> None:
    """Maintient une connexion vivante jusqu'à SIGINT ou SIGTERM."""
    kafka = KafkaConfig()
    adapter = BinanceAdapter()
    group = GROUPS[group_name]

    publisher = KafkaPublisher(Producer({"bootstrap.servers": kafka.bootstrap_servers}))
    router = Router(adapter.name, group.market, kafka.topics, publisher.publish)
    backoff = ExponentialBackoff(cap=group.max_reconnect_delay)
    shutdown = ShutdownHandler()
    callbacks = StreamCallbacks(
        adapter=adapter,
        subscribe=adapter.subscribe_messages(group),
        on_event=router.dispatch,
        on_connected=backoff.reset,
    )

    wsapp = websocket.WebSocketApp(
        adapter.ws_url(group),
        on_open=callbacks.on_open,
        on_message=callbacks.on_message,
        on_error=callbacks.on_error,
        on_close=callbacks.on_close,
        on_ping=callbacks.on_ping,
        on_pong=callbacks.on_pong,
    )
    shutdown.on_stop(wsapp.close)

    logger.info("Démarrage du groupe %s : %s", group.name, ", ".join(group.streams))
    while not shutdown.should_stop:
        try:
            wsapp.run_forever()
        except Exception as exc:
            logger.error("Erreur inattendue : %s", exc)
        if not shutdown.should_stop:
            delay = backoff.next_delay()
            logger.warning("Reconnexion dans %.1fs", delay)
            shutdown.wait(delay)

    publisher.flush()
    logger.info("Arrêt propre du groupe %s", group.name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Consumer WebSocket vers Kafka")
    parser.add_argument("group", choices=sorted(GROUPS), help="groupe de flux à lancer")
    args = parser.parse_args()
    setup_logging()
    run_ws(args.group)


if __name__ == "__main__":
    main()
