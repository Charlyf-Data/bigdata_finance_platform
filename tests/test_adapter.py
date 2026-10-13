import json

import pytest

from common.errors import InvalidMessage
from exchanges.binance.adapter import BinanceAdapter, stream_type
from exchanges.binance.streams import FUTURES_MARKET


@pytest.mark.parametrize(
    ("stream", "expected"),
    [
        ("btcusdt@kline_1m", "kline"),
        ("btcusdt@aggTrade", "aggTrade"),
        ("btcusdt@depth@100ms", "depth"),
        ("btcusdt@markPrice@1s", "markPrice"),
        ("btcusdt@forceOrder", "forceOrder"),
        ("btcusdt@bookTicker", "bookTicker"),
    ],
)
def test_stream_type(stream, expected):
    assert stream_type(stream) == expected


def test_ws_url_combine_les_flux():
    url = BinanceAdapter().ws_url(FUTURES_MARKET)
    assert url == (
        "wss://fstream.binance.com/market/stream?streams="
        "btcusdt@aggTrade/btcusdt@markPrice@1s/btcusdt@forceOrder"
    )


def test_parse_enveloppe_combinee():
    data = {"e": "aggTrade", "s": "BTCUSDT", "a": 1, "p": "1", "q": "1", "T": 1, "m": True, "E": 1}
    events = BinanceAdapter().parse(json.dumps({"stream": "btcusdt@aggTrade", "data": data}))
    assert len(events) == 1
    assert events[0].kind == "trade"
    assert events[0].symbol == "BTCUSDT"
    assert events[0].payload == data


def test_parse_ignore_les_reponses_techniques():
    assert BinanceAdapter().parse('{"result": null, "id": 1}') == []


def test_parse_json_invalide():
    with pytest.raises(InvalidMessage):
        BinanceAdapter().parse("{pas du json")


def test_parse_flux_inconnu():
    with pytest.raises(InvalidMessage):
        BinanceAdapter().parse(json.dumps({"stream": "btcusdt@ticker", "data": {}}))
