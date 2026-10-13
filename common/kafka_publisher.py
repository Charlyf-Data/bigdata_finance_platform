"""Publication générique dans Kafka : un topic, une clé, une valeur JSON."""

import json
import logging

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class KafkaPublisher:
    """Envoie des messages JSON dans Kafka.

    Le Producer est injecté : un seul par processus, remplaçable par un faux en test.
    """

    def __init__(self, producer: Producer) -> None:
        self.producer = producer

    def publish(self, topic: str, key: str, value: dict) -> None:
        """Dépose le message dans le tampon, puis traite les accusés en attente."""
        self.producer.produce(
            topic,
            key=key,
            value=json.dumps(value),
            on_delivery=self._on_delivery,
        )
        self.producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Attend la livraison des messages en tampon, au plus `timeout` secondes.

        Returns:
            Le nombre de messages restés non livrés (0 si tout est parti).
        """
        remaining = self.producer.flush(timeout)
        if remaining:
            logger.error("%d messages non livrés : Kafka injoignable ?", remaining)
        return remaining

    def _on_delivery(self, err, msg) -> None:
        """Appelé par poll() pour chaque accusé de réception."""
        if err is not None:
            logger.error("Échec de livraison vers %s : %s", msg.topic(), err)
