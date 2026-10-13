"""Adresse du broker et liste des topics autorisés."""

import os
from dataclasses import dataclass, field
from pathlib import Path

TOPICS_FILE = Path(__file__).with_name("topics.txt")


def load_topics(path: Path = TOPICS_FILE) -> frozenset[str]:
    """Lit topics.txt : une ligne par topic, commentaires et lignes vides ignorés."""
    lines = path.read_text(encoding="utf-8").splitlines()
    return frozenset(
        line.strip() for line in lines if line.strip() and not line.strip().startswith("#")
    )


def _bootstrap_from_env() -> str:
    # localhost:9092 depuis ta machine, kafka:29092 depuis un conteneur
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@dataclass(frozen=True)
class KafkaConfig:
    """Paramètres Kafka, figés pour toute l'exécution."""

    bootstrap_servers: str = field(default_factory=_bootstrap_from_env)
    topics: frozenset[str] = field(default_factory=load_topics)
