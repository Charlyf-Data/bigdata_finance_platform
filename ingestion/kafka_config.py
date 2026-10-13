from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaConfig:
    """Paramètres de connexion et noms des topics Kafka."""
    bootstrap_servers: str = "localhost:9092"
    topic_klines: str = "klines.raw"
    topic_trades: str = "trades.raw"
    topic_orderbook: str = "orderbook.raw"
