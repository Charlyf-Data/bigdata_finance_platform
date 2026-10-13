"""Adaptateur WebSocket Binance : enveloppe combinée -> événements neutres."""

import json

from common.errors import InvalidMessage
from common.events import Event
from ingestion.config import StreamGroup

# Nom du flux Binance -> type neutre, utilisé dans les noms de topics
KINDS: dict[str, str] = {
    "kline": "kline",
    "aggTrade": "trade",
    "bookTicker": "booktop",
    "depth": "depth",
    "markPrice": "markprice",
    "forceOrder": "liquidation",
}


def stream_type(stream: str) -> str:
    """'btcusdt@depth@100ms' -> 'depth' ; 'btcusdt@kline_1m' -> 'kline'."""
    return stream.split("@")[1].split("_")[0]


class BinanceAdapter:
    """Lit le format « combined stream » de Binance : {"stream": ..., "data": ...}."""

    name = "binance"

    def ws_url(self, group: StreamGroup) -> str:
        return f"{group.base_url}/stream?streams={'/'.join(group.streams)}"

    def subscribe_messages(self, group: StreamGroup) -> list[str]:
        return []  # Binance s'abonne par l'URL

    def parse(self, message: str) -> list[Event]:
        try:
            envelope = json.loads(message)
        except json.JSONDecodeError as e:
            raise InvalidMessage(f"JSON invalide : {e}") from e

        if not isinstance(envelope, dict) or "stream" not in envelope:
            return []  # réponse technique, pas une donnée de marché

        try:
            stream = envelope["stream"]
            data = envelope["data"]
            kind = KINDS[stream_type(stream)]
        except (KeyError, IndexError) as e:
            raise InvalidMessage(f"flux inconnu ou enveloppe incomplète : {e}") from e

        symbol = stream.split("@")[0].upper()
        return [Event(kind=kind, symbol=symbol, payload=data)]
