"""Contrat neutre v1 : ce que l'aval peut attendre, quel que soit l'exchange.

Règles communes à tous les types :
- les prix et quantités restent en texte décimal : aucun arrondi avant le calcul ;
- les horodatages sont des entiers en millisecondes UTC ;
- `exchange`, `market` et `symbol` sont toujours présents ;
- un champ que l'exchange ne fournit pas vaut None.

Un changement de format ne modifie jamais ces types : il crée un contrat v2.
"""

from typing import Literal, TypedDict


class Kline(TypedDict):
    exchange: str
    market: str
    symbol: str
    interval: str
    open_time: int
    close_time: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    quote_volume: str
    taker_buy_volume: str
    taker_buy_quote_volume: str
    nb_trades: int
    is_closed: bool
    event_time: int


class Trade(TypedDict):
    exchange: str
    market: str
    symbol: str
    trade_id: str
    price: str
    quantity: str
    aggressor_side: Literal["buy", "sell"]
    trade_time: int
    event_time: int


class BookTop(TypedDict):
    exchange: str
    market: str
    symbol: str
    update_id: str
    bid_price: str
    bid_quantity: str
    ask_price: str
    ask_quantity: str
    event_time: int | None
    transaction_time: int | None


class DepthDiff(TypedDict):
    exchange: str
    market: str
    symbol: str
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int | None
    bids: list[list[str]]
    asks: list[list[str]]
    event_time: int
    transaction_time: int | None


class DepthSnapshot(TypedDict):
    exchange: str
    market: str
    symbol: str
    last_update_id: int
    bids: list[list[str]]
    asks: list[list[str]]
    event_time: int | None
    transaction_time: int | None


class MarkPrice(TypedDict):
    exchange: str
    market: str
    symbol: str
    mark_price: str
    index_price: str
    estimated_settle_price: str
    funding_rate: str
    next_funding_time: int
    event_time: int


class Liquidation(TypedDict):
    exchange: str
    market: str
    symbol: str
    order_side: Literal["buy", "sell"]
    price: str
    average_price: str
    quantity: str
    filled_quantity: str
    status: str
    trade_time: int
    event_time: int


class OpenInterest(TypedDict):
    exchange: str
    market: str
    symbol: str
    open_interest: str
    event_time: int
