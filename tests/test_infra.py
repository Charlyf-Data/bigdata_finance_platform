import pyarrow.parquet as pq

from common.backoff import ExponentialBackoff
from common.kafka_config import load_topics
from exchanges.binance.adapter import KINDS, stream_type
from exchanges.binance.normalize import NORMALIZERS
from exchanges.binance.streams import GROUPS, REST_JOBS, REST_MARKET
from storage.parquet_writer import ParquetWriter


def test_backoff_double_plafonne_et_se_reinitialise():
    b = ExponentialBackoff(base=1.0, cap=8.0)
    delays = [b.next_delay() for _ in range(6)]
    assert 0.5 <= delays[0] <= 1.0
    assert all(d <= 8.0 for d in delays) and delays[-1] >= 4.0
    b.reset()
    assert b.next_delay() <= 1.0


def test_tous_les_flux_ont_leurs_topics_declares():
    """Garde-fou DRY : une faute de nom entre streams.py et topics.txt échoue ici."""
    topics = load_topics()
    for group in GROUPS.values():
        for stream in group.streams:
            kind = KINDS[stream_type(stream)]
            assert f"binance.{group.market}.{kind}.raw" in topics
            assert f"{group.market}.{kind}.v1" in topics
            assert kind in NORMALIZERS
    for job in REST_JOBS:
        assert f"binance.{REST_MARKET}.{job.kind}.raw" in topics
        assert f"{REST_MARKET}.{job.kind}.v1" in topics
        assert job.kind in NORMALIZERS


def test_parquet_ecrit_par_topic_et_par_jour(tmp_path):
    writer = ParquetWriter(tmp_path, max_rows=2)
    raw = {"exchange": "binance", "market": "futures", "kind": "trade", "symbol": "BTCUSDT",
           "received_at": 1791239932035, "payload": {"p": "1"}}
    writer.write("binance.futures.trade.raw", raw)
    writer.write("futures.trade.v1", {"symbol": "BTCUSDT", "price": "1", "event_time": 1791239932035})
    assert writer.should_flush()
    assert writer.flush() == 2

    files = sorted(tmp_path.rglob("*.parquet"))
    assert len(files) == 2
    raw_file = next(f for f in files if "trade.raw" in str(f))
    assert raw_file.parent.name.startswith("date=2026-")
    assert pq.read_table(raw_file).to_pylist()[0]["payload"] == '{"p": "1"}'
