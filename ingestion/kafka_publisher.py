"""Publication des records transformés dans Kafka."""

import json
import logging

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class KafkaPublisher:
    """Envoie les records transformés vers un topic Kafka."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        """Initialise le producteur Kafka.

        Args:
            bootstrap_servers: Adresse du cluster Kafka.
            topic: Nom du topic où publier les messages.
        """
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.topic = topic

    def publish(self, record: dict) -> None:
        """Envoie un record transformé vers Kafka."""
        self.producer.produce(
            self.topic,
            key=record["symbol"],
            value=json.dumps(record),
            on_delivery=self._on_delivery,
        )
        self.producer.poll(0)  # Traite les accusés de réception en attente

    def close(self) -> None:
        """Livre les messages restants avant l'arrêt."""
        self.producer.flush()

    def _on_delivery(self, err, msg) -> None:
        """Appelé par poll() pour chaque accusé de réception."""
        if err is not None:
            logger.error("Échec de livraison: %s", err)

