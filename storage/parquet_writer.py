"""Écriture en Parquet, un dossier par topic et par jour (UTC)."""

import json
import logging
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class ParquetWriter:
    """Accumule les records en mémoire, puis les écrit par paquets.

    Arborescence : <base_dir>/<topic>/date=YYYY-MM-DD/part-<id>.parquet
    Le payload natif des topics bruts est stocké en texte JSON : son
    format varie selon le type de message, une colonne texte ne casse jamais.
    """

    def __init__(
        self,
        base_dir: Path,
        max_rows: int = 50_000,
        max_age_s: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.base_dir = base_dir
        self.max_rows = max_rows
        self.max_age_s = max_age_s
        self.clock = clock
        self._buffers: dict[tuple[str, str], list[dict]] = defaultdict(list)
        self._rows = 0
        self._since = clock()

    def write(self, topic: str, record: dict) -> None:
        self._buffers[(topic, _day(record))].append(_flatten(record))
        self._rows += 1

    def should_flush(self) -> bool:
        too_big = self._rows >= self.max_rows
        too_old = self._rows > 0 and self.clock() - self._since >= self.max_age_s
        return too_big or too_old

    def flush(self) -> int:
        """Écrit tous les tampons sur disque et renvoie le nombre de lignes écrites."""
        written = self._rows
        for (topic, day), rows in self._buffers.items():
            folder = self.base_dir / topic / f"date={day}"
            folder.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows), folder / f"part-{uuid.uuid4().hex}.parquet")
        if written:
            logger.info("%d lignes écrites dans %d fichiers", written, len(self._buffers))
        self._buffers.clear()
        self._rows = 0
        self._since = self.clock()
        return written


def _day(record: dict) -> str:
    """Jour UTC du record : réception pour le brut, événement pour le v1."""
    ts = record.get("received_at") or record.get("event_time")
    moment = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else datetime.now(timezone.utc)
    return moment.strftime("%Y-%m-%d")


def _flatten(record: dict) -> dict:
    if isinstance(record.get("payload"), (dict, list)):
        return {**record, "payload": json.dumps(record["payload"])}
    return record
