"""Le contrat que tout adaptateur d'exchange doit respecter."""

from typing import Protocol

from common.events import Event
from ingestion.config import StreamGroup


class ExchangeAdapter(Protocol):
    """Tout ce qu'il faut savoir d'un exchange pour lire son WebSocket.

    Un nouvel exchange = une classe qui respecte ce contrat. Rien d'autre
    dans ingestion/ ne change.
    """

    name: str

    def ws_url(self, group: StreamGroup) -> str:
        """Adresse de connexion du groupe."""
        ...

    def subscribe_messages(self, group: StreamGroup) -> list[str]:
        """Messages à envoyer après la connexion. Vide si l'abonnement passe par l'URL."""
        ...

    def parse(self, message: str) -> list[Event]:
        """Découpe un message reçu en 0, 1 ou plusieurs événements.

        Raises:
            InvalidMessage: si le message est illisible ou incomplet.
        """
        ...
