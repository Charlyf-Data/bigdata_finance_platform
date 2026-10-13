"""Point d'entrée REST : open interest et snapshots du carnet.

Depuis la racine du projet :
    python3 -m ingestion.run_rest
"""

import logging

from confluent_kafka import Producer

from common.kafka_config import KafkaConfig
from common.kafka_publisher import KafkaPublisher
from common.logging_setup import setup_logging
from common.shutdown import ShutdownHandler
from exchanges.binance.adapter import BinanceAdapter
from exchanges.binance.rest_client import BinanceRestClient
from exchanges.binance.streams import REST_JOBS, REST_MARKET
from ingestion.rest_poller import RestPoller
from ingestion.routing import Router

logger = logging.getLogger(__name__)


def run_rest() -> None:
    """Interroge chaque job à son intervalle jusqu'à SIGINT ou SIGTERM."""
    kafka = KafkaConfig()
    publisher = KafkaPublisher(Producer({"bootstrap.servers": kafka.bootstrap_servers}))
    router = Router(BinanceAdapter.name, REST_MARKET, kafka.topics, publisher.publish)
    shutdown = ShutdownHandler()
    poller = RestPoller(REST_JOBS, BinanceRestClient(), router.dispatch, shutdown)

    logger.info("Démarrage du poller REST : %s", ", ".join(job.kind for job in REST_JOBS))
    poller.run()
    publisher.flush()
    logger.info("Arrêt propre du poller REST")


def main() -> None:
    setup_logging()
    run_rest()


if __name__ == "__main__":
    main()
