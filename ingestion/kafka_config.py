

from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaConfig:
    """Paramètres de connexion et noms des topics Kafka."""
    bootstrap_servers: str = "localhost:9092"
    topic_klines: str = "klines.raw"
    topic_trades: str = "trades.raw"
    topic_orderbook: str = "orderbook.raw"
    topic_tickers: str = "tickers.raw"
    topic_funding: str = "funding.raw"
    topic_liquidations: str = "liquidations.raw"
    topic_open_interest: str = "open_interest.raw"
    topic_index: str = "index.raw"
    topic_futures: str = "futures.raw"
    topic_options: str = "options.raw"
    topic_news: str = "news.raw"
    topic_social: str = "social.raw"
    topic_sentiment: str = "sentiment.raw"