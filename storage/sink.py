"""Point d'entrée du stockage : tous les topics -> fichiers Parquet.

Depuis la racine du projet :
    python3 -m storage.sink

Règle : écrire sur disque, puis valider l'offset. Un crash entre les deux
peut dupliquer quelques messages, jamais en perdre.
"""

import logging
import os
from pathlib import Path

from common.kafka_config import KafkaConfig
from common.kafka_reader import KafkaReader
from common.logging_setup import setup_logging
from common.shutdown import ShutdownHandler
from storage.parquet_writer import ParquetWriter

logger = logging.getLogger(__name__)


def run_sink() -> None:
    kafka = KafkaConfig()
    reader = KafkaReader(kafka.bootstrap_servers, "storage-sink", kafka.topics)
    writer = ParquetWriter(Path(os.getenv("DATA_DIR", "data")))
    shutdown = ShutdownHandler()

    logger.info("Stockage de %d topics vers %s", len(kafka.topics), writer.base_dir)
    while not shutdown.should_stop:
        for topic, record in reader.poll_batch():
            writer.write(topic, record)
        if writer.should_flush():
            writer.flush()
            reader.commit()

    writer.flush()
    reader.commit()
    reader.close()
    logger.info("Arrêt propre du stockage")


def main() -> None:
    setup_logging()
    run_sink()


if __name__ == "__main__":
    main()
