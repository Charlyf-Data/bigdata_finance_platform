import json

from exchanges.binance.adapter import BinanceAdapter
from ingestion.stream_callbacks import StreamCallbacks


class FakeWs:
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(message)


def test_on_open_reinitialise_et_s_abonne():
    calls = []
    cb = StreamCallbacks(BinanceAdapter(), ["sub"], lambda e: None, lambda: calls.append("reset"))
    ws = FakeWs()
    cb.on_open(ws)
    assert calls == ["reset"] and ws.sent == ["sub"]


def test_on_message_transmet_chaque_evenement_et_ignore_le_reste():
    events = []
    cb = StreamCallbacks(BinanceAdapter(), [], events.append, lambda: None)
    cb.on_message(None, json.dumps({"stream": "btcusdt@kline_1m", "data": {"k": {}}}))
    cb.on_message(None, "{pas du json")
    cb.on_message(None, '{"result": null, "id": 1}')
    assert [e.kind for e in events] == ["kline"]
