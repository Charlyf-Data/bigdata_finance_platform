"""Client REST Binance : requête, timeout et classement des erreurs."""

import logging

import requests

from common.errors import PermanentError, RateLimited, TransientError

logger = logging.getLogger(__name__)

DEFAULT_RETRY_AFTER = 60.0


class BinanceRestClient:
    """Appelle l'API REST et traduit chaque échec en erreur commune.

    | Réponse            | Erreur levée    | Réaction du poller       |
    | timeout, réseau    | TransientError  | backoff sur ce job       |
    | 5xx                | TransientError  | backoff sur ce job       |
    | 429, 418           | RateLimited     | pause de toute l'IP      |
    | autre 4xx          | PermanentError  | log, pas d'insistance    |
    """

    def __init__(
        self,
        session: requests.Session | None = None,
        timeout: tuple[float, float] = (3.05, 10.0),
    ) -> None:
        self.session = session or requests.Session()
        self.timeout = timeout

    def get(self, url: str, params: dict) -> dict:
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
        except (requests.Timeout, requests.ConnectionError) as e:
            raise TransientError(str(e)) from e

        logger.debug("Poids utilisé sur 1 min : %s", response.headers.get("X-MBX-USED-WEIGHT-1M"))

        if response.status_code in (418, 429):
            raise RateLimited(_retry_after(response.headers.get("Retry-After")))
        if response.status_code >= 500:
            raise TransientError(f"HTTP {response.status_code}")
        if response.status_code >= 400:
            raise PermanentError(f"HTTP {response.status_code} : {response.text[:200]}")

        try:
            return response.json()
        except ValueError as e:
            raise TransientError(f"réponse illisible : {e}") from e


def _retry_after(header: str | None) -> float:
    try:
        return float(header) if header is not None else DEFAULT_RETRY_AFTER
    except ValueError:
        return DEFAULT_RETRY_AFTER
