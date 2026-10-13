"""Normalisation Binance : format natif -> contrat neutre v1.

Chaque fonction reçoit le payload natif, le marché et le symbole de
l'enveloppe brute. Un champ manquant lève InvalidMessage.
"""

from collections.abc import Callable

from common.contracts import (
    BookTop,
    DepthDiff,
    DepthSnapshot,
    Kline,
    Liquidation,
    MarkPrice,
    OpenInterest,
    Trade,
)
from common.errors import InvalidMessage

EXCHANGE = "binance"


def normalize_kline(payload: dict, market: str, symbol: str) -> Kline:
    k = payload["k"]
    return Kline(
        exchange=EXCHANGE,
        market=market,
        symbol=symbol,
        interval=k["i"],
        open_time=k["t"],
        close_time=k["T"],
        open=k["o"],
        high=k["h"],
        low=k["l"],
        close=k["c"],
        volume=k["v"],
        quote_volume=k["q"],
        taker_buy_volume=k["V"],
        taker_buy_quote_volume=k["Q"],
        nb_trades=k["n"],
        is_closed=k["x"],
        event_time=payload["E"],
    )


def normalize_trade(payload: dict, market: str, symbol: str) -> Trade:
    return Trade(
        exchange=EXCHANGE,
        market=market,
        symbol=symbol,
        trade_id=str(payload["a"]),
        price=payload["p"],
        quantity=payload["q"],
        # m = "buyer is maker" : s'il est vrai, c'est le vendeur qui a pris l'initiative
        aggressor_side="sell" if payload["m"] else "buy",
        trade_time=payload["T"],
        event_time=payload["E"],
    )


def normalize_booktop(payload: dict, market: str, symbol: str) -> BookTop:
    return BookTop(
        exchange=EXCHANGE,
        market=market,
        symbol=symbol,
        update_id=str(payload["u"]),
        bid_price=payload["b"],
        bid_quantity=payload["B"],
        ask_price=payload["a"],
        ask_quantity=payload["A"],
        event_time=payload.get("E"),  # absent sur le spot
        transaction_time=payload.get("T"),
    )


def normalize_depth(payload: dict, market: str, symbol: str) -> DepthDiff:
    return DepthDiff(
        exchange=EXCHANGE,
        market=market,
        symbol=symbol,
        first_update_id=payload["U"],
        final_update_id=payload["u"],
        previous_final_update_id=payload.get("pu"),  # absent sur le spot
        bids=payload["b"],
        asks=payload["a"],
        event_time=payload["E"],
        transaction_time=payload.get("T"),
    )


def normalize_depth_snapshot(payload: dict, market: str, symbol: str) -> DepthSnapshot:
    return DepthSnapshot(
        exchange=EXCHANGE,
        market=market,
        symbol=symbol,
        last_update_id=payload["lastUpdateId"],
        bids=payload["bids"],
        asks=payload["asks"],
        event_time=payload.get("E"),
        transaction_time=payload.get("T"),
    )


def normalize_markprice(payload: dict, market: str, symbol: str) -> MarkPrice:
    return MarkPrice(
        exchange=EXCHANGE,
        market=market,
        symbol=symbol,
        mark_price=payload["p"],
        index_price=payload["i"],
        estimated_settle_price=payload["P"],
        funding_rate=payload["r"],
        next_funding_time=payload["T"],
        event_time=payload["E"],
    )


def normalize_liquidation(payload: dict, market: str, symbol: str) -> Liquidation:
    order = payload["o"]
    return Liquidation(
        exchange=EXCHANGE,
        market=market,
        symbol=symbol,
        # SELL = une position longue liquidée ; BUY = une position courte
        order_side=order["S"].lower(),
        price=order["p"],
        average_price=order["ap"],
        quantity=order["q"],
        filled_quantity=order["z"],
        status=order["X"],
        trade_time=order["T"],
        event_time=payload["E"],
    )


def normalize_open_interest(payload: dict, market: str, symbol: str) -> OpenInterest:
    return OpenInterest(
        exchange=EXCHANGE,
        market=market,
        symbol=symbol,
        open_interest=payload["openInterest"],
        event_time=payload["time"],
    )


def _strict(fn: Callable[[dict, str, str], dict]) -> Callable[[dict, str, str], dict]:
    """Traduit tout champ manquant ou de mauvais type en InvalidMessage."""

    def wrapper(payload: dict, market: str, symbol: str) -> dict:
        try:
            return fn(payload, market, symbol)
        except (KeyError, TypeError, AttributeError) as e:
            raise InvalidMessage(f"{fn.__name__} : champ manquant ou invalide ({e!r})") from e

    wrapper.__name__ = fn.__name__
    return wrapper


# Type neutre -> fonction de normalisation. Le normaliseur ne connaît que ce dictionnaire.
NORMALIZERS: dict[str, Callable[[dict, str, str], dict]] = {
    "kline": _strict(normalize_kline),
    "trade": _strict(normalize_trade),
    "booktop": _strict(normalize_booktop),
    "depth": _strict(normalize_depth),
    "depth-snapshot": _strict(normalize_depth_snapshot),
    "markprice": _strict(normalize_markprice),
    "liquidation": _strict(normalize_liquidation),
    "open-interest": _strict(normalize_open_interest),
}
