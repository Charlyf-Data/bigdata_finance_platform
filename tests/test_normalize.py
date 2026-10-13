import pytest

from common.errors import InvalidMessage
from exchanges.binance.normalize import NORMALIZERS

KLINE = {
    "e": "kline", "E": 1791239932035, "s": "BTCUSDT",
    "k": {
        "t": 1791239880000, "T": 1791239939999, "s": "BTCUSDT", "i": "1m",
        "o": "85892.25", "c": "85926.01", "h": "85926.01", "l": "85892.01",
        "v": "5.53415", "n": 1452, "x": False, "q": "475000.12",
        "V": "3.1", "Q": "266000.5",
    },
}
AGG_TRADE = {
    "e": "aggTrade", "E": 1791239932035, "s": "BTCUSDT", "a": 26129,
    "p": "85926.01", "q": "0.012", "f": 100, "l": 105, "T": 1791239932030, "m": True,
}
BOOK_TICKER = {
    "e": "bookTicker", "u": 400900217, "E": 1568014460893, "T": 1568014460891,
    "s": "BTCUSDT", "b": "85926.00", "B": "3.2", "a": "85926.01", "A": "1.1",
}
DEPTH = {
    "e": "depthUpdate", "E": 123456789, "T": 123456788, "s": "BTCUSDT",
    "U": 157, "u": 160, "pu": 149, "b": [["85926.00", "1.5"]], "a": [["85926.01", "0"]],
}
MARK_PRICE = {
    "e": "markPriceUpdate", "E": 1562305380000, "s": "BTCUSDT", "p": "85920.1",
    "i": "85918.4", "P": "85919.0", "r": "0.00010000", "T": 1562306400000,
}
FORCE_ORDER = {
    "e": "forceOrder", "E": 1568014460893,
    "o": {
        "s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC", "q": "0.014",
        "p": "85000", "ap": "85010", "X": "FILLED", "l": "0.014", "z": "0.014",
        "T": 1568014460893,
    },
}
OPEN_INTEREST = {"openInterest": "10659.509", "symbol": "BTCUSDT", "time": 1589437530011}
DEPTH_SNAPSHOT = {
    "lastUpdateId": 1027024, "E": 1589436922972, "T": 1589436922959,
    "bids": [["85926.00", "431.0"]], "asks": [["85926.01", "12.0"]],
}


def test_kline_garde_les_prix_en_texte_et_les_volumes_agresseurs():
    k = NORMALIZERS["kline"](KLINE, "spot", "BTCUSDT")
    assert k["close"] == "85926.01"
    assert k["taker_buy_volume"] == "3.1"
    assert k["quote_volume"] == "475000.12"
    assert k["open_time"] == 1791239880000
    assert k["exchange"] == "binance" and k["market"] == "spot"


@pytest.mark.parametrize(("m", "side"), [(True, "sell"), (False, "buy")])
def test_trade_cote_agresseur(m, side):
    trade = NORMALIZERS["trade"]({**AGG_TRADE, "m": m}, "futures", "BTCUSDT")
    assert trade["aggressor_side"] == side
    assert trade["trade_id"] == "26129"


def test_booktop_futures_et_spot():
    futures = NORMALIZERS["booktop"](BOOK_TICKER, "futures", "BTCUSDT")
    assert futures["bid_price"] == "85926.00" and futures["event_time"] == 1568014460893
    spot = {k: v for k, v in BOOK_TICKER.items() if k not in ("e", "E", "T")}
    assert NORMALIZERS["booktop"](spot, "spot", "BTCUSDT")["event_time"] is None


def test_depth_garde_les_identifiants_de_sequence():
    d = NORMALIZERS["depth"](DEPTH, "futures", "BTCUSDT")
    assert (d["first_update_id"], d["final_update_id"], d["previous_final_update_id"]) == (157, 160, 149)
    assert d["asks"] == [["85926.01", "0"]]


def test_depth_snapshot():
    s = NORMALIZERS["depth-snapshot"](DEPTH_SNAPSHOT, "futures", "BTCUSDT")
    assert s["last_update_id"] == 1027024 and s["symbol"] == "BTCUSDT"


def test_markprice():
    m = NORMALIZERS["markprice"](MARK_PRICE, "futures", "BTCUSDT")
    assert m["funding_rate"] == "0.00010000" and m["next_funding_time"] == 1562306400000


def test_liquidation():
    liq = NORMALIZERS["liquidation"](FORCE_ORDER, "futures", "BTCUSDT")
    assert liq["order_side"] == "sell" and liq["average_price"] == "85010"


def test_open_interest():
    oi = NORMALIZERS["open-interest"](OPEN_INTEREST, "futures", "BTCUSDT")
    assert oi["open_interest"] == "10659.509" and oi["event_time"] == 1589437530011


@pytest.mark.parametrize("kind", sorted(NORMALIZERS))
def test_message_incomplet_leve_invalid_message(kind):
    with pytest.raises(InvalidMessage):
        NORMALIZERS[kind]({}, "futures", "BTCUSDT")
