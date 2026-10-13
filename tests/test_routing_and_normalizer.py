from common.events import Event
from exchanges.binance.normalize import NORMALIZERS
from ingestion.routing import Router
from processing.normalizer import Normalizer

TOPICS = frozenset({"binance.futures.trade.raw", "futures.trade.v1"})
AGG_TRADE = {"a": 1, "p": "10", "q": "2", "T": 5, "m": False, "E": 6}


def test_router_publie_le_natif_dans_son_enveloppe():
    sent = []
    router = Router("binance", "futures", TOPICS, lambda t, k, v: sent.append((t, k, v)))
    router.dispatch(Event("trade", "BTCUSDT", AGG_TRADE))
    topic, key, value = sent[0]
    assert topic == "binance.futures.trade.raw" and key == "BTCUSDT"
    assert value["payload"] == AGG_TRADE and value["kind"] == "trade"
    assert isinstance(value["received_at"], int)


def test_router_ignore_un_topic_non_declare():
    sent = []
    router = Router("binance", "futures", TOPICS, lambda t, k, v: sent.append(t))
    router.dispatch(Event("depth", "BTCUSDT", {}))
    assert sent == []


def test_normalizer_brut_vers_v1():
    sent = []
    normalizer = Normalizer(NORMALIZERS, TOPICS, lambda t, k, v: sent.append((t, k, v)))
    raw = {"exchange": "binance", "market": "futures", "kind": "trade",
           "symbol": "BTCUSDT", "received_at": 7, "payload": AGG_TRADE}
    normalizer.handle(raw)
    topic, key, record = sent[0]
    assert topic == "futures.trade.v1" and key == "BTCUSDT"
    assert record["aggressor_side"] == "buy"


def test_normalizer_ignore_un_payload_invalide():
    sent = []
    normalizer = Normalizer(NORMALIZERS, TOPICS, lambda t, k, v: sent.append(t))
    normalizer.handle({"market": "futures", "kind": "trade", "symbol": "BTCUSDT", "payload": {}})
    normalizer.handle({"market": "futures", "kind": "inconnu", "symbol": "BTCUSDT", "payload": {}})
    assert sent == []


class FakeProducer:
    def __init__(self, remaining):
        self.remaining = remaining
        self.produced = []

    def produce(self, topic, key, value, on_delivery):
        self.produced.append((topic, key, value))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return self.remaining


def test_publisher_serialise_en_json_et_signale_les_non_livres():
    from common.kafka_publisher import KafkaPublisher

    producer = FakeProducer(remaining=3)
    publisher = KafkaPublisher(producer)
    publisher.publish("t", "BTCUSDT", {"a": 1})
    assert producer.produced == [("t", "BTCUSDT", '{"a": 1}')]
    assert publisher.flush(timeout=0.1) == 3
