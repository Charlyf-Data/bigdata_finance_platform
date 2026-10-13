"""Planification des requêtes REST : une échéance et un backoff par job."""

import logging
import time
from collections.abc import Callable
from typing import Protocol

from common.backoff import ExponentialBackoff
from common.errors import PermanentError, RateLimited, TransientError
from common.events import Event
from ingestion.config import RestJob

logger = logging.getLogger(__name__)


class RestClient(Protocol):
    """Ce dont le poller a besoin d'un client REST, et rien de plus."""

    def get(self, url: str, params: dict) -> dict: ...


class Stopper(Protocol):
    """Ce dont le poller a besoin pour s'arrêter proprement."""

    @property
    def should_stop(self) -> bool: ...

    def wait(self, seconds: float) -> bool: ...


class RestPoller:
    """Appelle chaque job à son intervalle, sans qu'un job en bloque un autre.

    - succès : backoff remis à zéro, prochaine échéance = intervalle du job ;
    - panne passagère : backoff sur ce job seulement ;
    - limite atteinte (429/418) : pause de tous les jobs, l'IP est commune ;
    - requête invalide : log, prochaine tentative à l'intervalle normal.
    """

    def __init__(
        self,
        jobs: tuple[RestJob, ...],
        client: RestClient,
        on_event: Callable[[Event], None],
        stopper: Stopper,
        make_backoff: Callable[[], ExponentialBackoff] = ExponentialBackoff,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.jobs = jobs
        self.client = client
        self.on_event = on_event
        self.stopper = stopper
        self.clock = clock
        self.backoffs = {job.kind: make_backoff() for job in jobs}
        now = clock()
        self.next_due = {job.kind: now for job in jobs}

    def run(self) -> None:
        while not self.stopper.should_stop:
            job = min(self.jobs, key=lambda j: self.next_due[j.kind])
            pause = self.next_due[job.kind] - self.clock()
            if pause > 0 and self.stopper.wait(pause):
                break  # arrêt demandé pendant l'attente
            self._poll(job)

    def _poll(self, job: RestJob) -> None:
        now = self.clock()
        try:
            data = self.client.get(job.url, job.params)
        except RateLimited as e:
            logger.warning("Limite Binance atteinte : pause de %ss pour tous les jobs", e.retry_after)
            for kind in self.next_due:
                self.next_due[kind] = max(self.next_due[kind], now + e.retry_after)
            return
        except TransientError as e:
            delay = self.backoffs[job.kind].next_delay()
            logger.warning("%s en échec (%s) : nouvel essai dans %.1fs", job.kind, e, delay)
            self.next_due[job.kind] = now + delay
            return
        except PermanentError as e:
            logger.error("Requête %s invalide : %s", job.kind, e)
            self.next_due[job.kind] = now + job.interval_s
            return

        self.backoffs[job.kind].reset()
        self.next_due[job.kind] = now + job.interval_s
        self.on_event(Event(kind=job.kind, symbol=job.symbol, payload=data))
