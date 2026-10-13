import pytest
import requests

from common.errors import PermanentError, RateLimited, TransientError
from exchanges.binance.rest_client import BinanceRestClient
from ingestion.config import RestJob
from ingestion.rest_poller import RestPoller


class FakeResponse:
    def __init__(self, status, body=None, headers=None):
        self.status_code = status
        self._body = body if body is not None else {}
        self.headers = headers or {}
        self.text = str(self._body)

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, outcome):
        self.outcome = outcome

    def get(self, url, params, timeout):
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


@pytest.mark.parametrize(
    ("outcome", "error"),
    [
        (requests.Timeout("lent"), TransientError),
        (requests.ConnectionError("coupé"), TransientError),
        (FakeResponse(503), TransientError),
        (FakeResponse(429, headers={"Retry-After": "12"}), RateLimited),
        (FakeResponse(418, headers={"Retry-After": "120"}), RateLimited),
        (FakeResponse(400), PermanentError),
    ],
)
def test_client_classe_les_erreurs(outcome, error):
    with pytest.raises(error):
        BinanceRestClient(session=FakeSession(outcome)).get("u", {})


def test_client_lit_retry_after():
    client = BinanceRestClient(session=FakeSession(FakeResponse(429, headers={"Retry-After": "12"})))
    with pytest.raises(RateLimited) as exc:
        client.get("u", {})
    assert exc.value.retry_after == 12.0


def test_client_renvoie_le_json():
    body = {"openInterest": "1"}
    assert BinanceRestClient(session=FakeSession(FakeResponse(200, body))).get("u", {}) == body


class ScriptedClient:
    def __init__(self, script):
        self.script = list(script)

    def get(self, url, params):
        outcome = self.script.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class FakeStopper:
    def __init__(self, clock, max_waits):
        self.clock = clock
        self.waits = 0
        self.max_waits = max_waits

    @property
    def should_stop(self):
        return self.waits >= self.max_waits

    def wait(self, seconds):
        self.clock.now += seconds
        self.waits += 1
        return self.should_stop


OI = RestJob(kind="open-interest", symbol="BTCUSDT", url="u", interval_s=10.0)
SNAP = RestJob(kind="depth-snapshot", symbol="BTCUSDT", url="u2", interval_s=300.0)


def make_poller(script, jobs=(OI,)):
    clock = FakeClock()
    events = []
    poller = RestPoller(jobs, ScriptedClient(script), events.append, FakeStopper(clock, 99), clock=clock)
    return poller, clock, events


def test_succes_emet_un_evenement_et_planifie_l_intervalle():
    poller, clock, events = make_poller([{"openInterest": "1"}])
    poller._poll(OI)
    assert events[0].kind == "open-interest" and events[0].symbol == "BTCUSDT"
    assert poller.next_due["open-interest"] == clock.now + 10.0


def test_panne_passagere_ne_touche_que_ce_job():
    poller, clock, _ = make_poller([TransientError("503")], jobs=(OI, SNAP))
    before = poller.next_due["depth-snapshot"]
    poller._poll(OI)
    assert clock.now < poller.next_due["open-interest"] <= clock.now + 1.0
    assert poller.next_due["depth-snapshot"] == before


def test_limite_atteinte_met_tous_les_jobs_en_pause():
    poller, clock, _ = make_poller([RateLimited(120.0)], jobs=(OI, SNAP))
    poller._poll(OI)
    assert all(due >= clock.now + 120.0 for due in poller.next_due.values())


def test_requete_invalide_reessaie_a_l_intervalle_normal():
    poller, clock, _ = make_poller([PermanentError("400")])
    poller._poll(OI)
    assert poller.next_due["open-interest"] == clock.now + 10.0


def test_run_alterne_les_jobs_sans_se_bloquer():
    clock = FakeClock()
    events = []
    script = [{"openInterest": str(i)} for i in range(50)]
    poller = RestPoller((OI, SNAP), ScriptedClient(script * 2), events.append, FakeStopper(clock, 5), clock=clock)
    poller.run()
    kinds = [e.kind for e in events]
    assert "open-interest" in kinds and "depth-snapshot" in kinds
