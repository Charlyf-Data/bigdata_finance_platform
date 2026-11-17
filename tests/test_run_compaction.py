"""Tests du lanceur de compaction."""

from datetime import date, datetime, timedelta, timezone

from storage.run_compaction import kind_du_topic, main


def test_kind_du_topic():
    assert kind_du_topic("binance.futures.trade.raw") == "trade"
    assert kind_du_topic("binance.futures.depth-snapshot.raw") == "depth-snapshot"


def test_refuse_le_jour_en_cours(tmp_path):
    aujourdhui = datetime.now(timezone.utc).date()
    assert main(["--data-dir", str(tmp_path), "--jour", aujourdhui.isoformat()]) == 2


def test_jour_passe_sans_donnees(tmp_path):
    hier = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    assert main(["--data-dir", str(tmp_path), "--jour", hier]) == 0
    assert date.fromisoformat(hier)