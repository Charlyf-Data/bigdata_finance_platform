"""Conversion des messages bruts vers le contrat neutre v1."""

import logging
from collections.abc import Callable

from common.errors import InvalidMessage

logger = logging.getLogger(__name__)


class Normalizer:
    """Applique la normalisation d'un exchange et publie dans <marché>.<type>.v1.

    Il ne connaît aucun exchange : il reçoit le dictionnaire NORMALIZERS
    de l'exchange choisi.
    """

    def __init__(
        self,
        normalizers: dict[str, Callable[[dict, str, str], dict]],
        known_topics: frozenset[str],
        publish: Callable[[str, str, dict], None],
    ) -> None:
        self.normalizers = normalizers
        self.known_topics = known_topics
        self.publish = publish
        self._reported: set[str] = set()

    def handle(self, raw: dict) -> None:
        """Normalise une enveloppe brute et publie le résultat."""
        kind = raw.get("kind")
        normalize = self.normalizers.get(kind)
        if normalize is None:
            self._report_once(f"kind:{kind}", "Aucune normalisation pour le type %s", kind)
            return

        try:
            record = normalize(raw["payload"], raw["market"], raw["symbol"])
        except (InvalidMessage, KeyError) as e:
            logger.warning("Message brut ignoré (%s) : %s", kind, e)
            return

        topic = f"{record['market']}.{kind}.v1"
        if topic not in self.known_topics:
            self._report_once(topic, "Topic %s absent de topics.txt : messages ignorés", topic)
            return
        self.publish(topic, record["symbol"], record)

    def _report_once(self, key: str, message: str, arg: object) -> None:
        if key not in self._reported:
            logger.error(message, arg)
            self._reported.add(key)
