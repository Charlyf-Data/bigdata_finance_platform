"""Point d'entrée du normaliseur : topics bruts -> topics v1.

Depuis la racine du projet :
    python3 -m processing.run_normalize

Changer d'exchange : remplacer l'import exchanges.binance et le préfixe EXCHANGE.
"""

import logging

from confluent_kafka import Producer

from common.kafka_config import KafkaConfig
from common.kafka_publisher import KafkaPublisher
from common.kafka_reader import KafkaReader
from common.logging_setup import setup_logging
from common.shutdown import ShutdownHandler
from exchanges.binance.normalize import NORMALIZERS
from processing.normalizer import Normalizer

logger = logging.getLogger(__name__)

EXCHANGE = "binance"


def run_normalize() -> None:
    """Lit le brut par lots ; valide la lecture seulement après livraison des v1."""
    kafka = KafkaConfig()
    raw_topics = sorted(
        t for t in kafka.topics if t.startswith(f"{EXCHANGE}.") and t.endswith(".raw")
    )
    reader = KafkaReader(kafka.bootstrap_servers, f"normalizer-{EXCHANGE}", raw_topics)
    publisher = KafkaPublisher(Producer({"bootstrap.servers": kafka.bootstrap_servers}))
    normalizer = Normalizer(NORMALIZERS, kafka.topics, publisher.publish)
    shutdown = ShutdownHandler()

    logger.info("Normalisation de %d topics bruts", len(raw_topics))
    while not shutdown.should_stop:
        batch = reader.poll_batch()
        for _topic, raw in batch:
            normalizer.handle(raw)
        if batch:
            publisher.flush()  # tout est livré...
            reader.commit()    # ...avant de valider la lecture

    publisher.flush()
    reader.commit()
    reader.close()
    logger.info("Arrêt propre du normaliseur")


def main() -> None:
    setup_logging()
    run_normalize()


if __name__ == "__main__":
    main()
