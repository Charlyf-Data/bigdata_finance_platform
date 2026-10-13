"""Connexions WebSocket et requêtes REST Binance.

Depuis avril 2026, les flux futures sont répartis entre deux adresses :
/public (carnet : bookTicker, depth) et /market (trades, mark price,
liquidations, klines).
"""

from ingestion.config import RestJob, StreamGroup

SYMBOL = "btcusdt"

SPOT = StreamGroup(
    name="spot",
    market="spot",
    base_url="wss://data-stream.binance.vision",
    streams=(f"{SYMBOL}@kline_1m", f"{SYMBOL}@aggTrade"),
)

FUTURES_PUBLIC = StreamGroup(
    name="futures-public",
    market="futures",
    base_url="wss://fstream.binance.com/public",
    streams=(f"{SYMBOL}@bookTicker", f"{SYMBOL}@depth@100ms"),
)

FUTURES_MARKET = StreamGroup(
    name="futures-market",
    market="futures",
    base_url="wss://fstream.binance.com/market",
    streams=(f"{SYMBOL}@aggTrade", f"{SYMBOL}@markPrice@1s", f"{SYMBOL}@forceOrder"),
)

GROUPS: dict[str, StreamGroup] = {g.name: g for g in (SPOT, FUTURES_PUBLIC, FUTURES_MARKET)}

REST_MARKET = "futures"
_FAPI = "https://fapi.binance.com"

REST_JOBS: tuple[RestJob, ...] = (
    RestJob(
        kind="open-interest",
        symbol=SYMBOL.upper(),
        url=f"{_FAPI}/fapi/v1/openInterest",
        params={"symbol": SYMBOL.upper()},
        interval_s=10.0,
    ),
    RestJob(
        kind="depth-snapshot",
        symbol=SYMBOL.upper(),
        url=f"{_FAPI}/fapi/v1/depth",
        params={"symbol": SYMBOL.upper(), "limit": 1000},
        interval_s=300.0,
    ),
)
