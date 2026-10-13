"""Lecture générique de Kafka par lots, avec validation manuelle des offsets.

Partagé par processing (normaliseur) et storage (sink) : les deux lisent Kafka.
"""

import json
import logging
from collections.abc import Iterable

from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)


class KafkaReader:
    """Lit des messages JSON par lots ; les offsets n'avancent que sur commit()."""

    def __init__(self, bootstrap_servers: str, group_id: str, topics: Iterable[str]) -> None:
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        })
        self.consumer.subscribe(sorted(topics))
        self._uncommitted = False

    def poll_batch(self, max_messages: int = 500, timeout: float = 1.0) -> list[tuple[str, dict]]:
        """Renvoie jusqu'à max_messages couples (topic, valeur décodée)."""
        batch: list[tuple[str, dict]] = []
        for msg in self.consumer.consume(num_messages=max_messages, timeout=timeout):
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Erreur de lecture : %s", msg.error())
                continue
            self._uncommitted = True
            try:
                batch.append((msg.topic(), json.loads(msg.value())))
            except (TypeError, ValueError) as e:
                logger.warning("Message illisible ignoré sur %s : %s", msg.topic(), e)
        return batch

    def commit(self) -> None:
        """Valide les offsets de tout ce qui a été lu jusqu'ici."""
        if self._uncommitted:
            self.consumer.commit(asynchronous=False)
            self._uncommitted = False

    def close(self) -> None:
        self.consumer.close()
