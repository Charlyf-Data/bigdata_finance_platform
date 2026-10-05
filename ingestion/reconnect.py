"""Gestion de la reconnexion avec backoff exponentiel.

Classe avec état : 'tentative' s'incrémente à chaque échec et doit
persister entre les appels pour calculer le délai croissant.
"""

import logging
import time

logger = logging.getLogger(__name__)


class ReconnectHandler:
    """Calcule et applique un délai de reconnexion exponentiel.

    Le délai double à chaque tentative (1s, 2s, 4s, 8s...) jusqu'à
    un plafond, ce qui évite de saturer Binance et le bannissement IP.

    Attributes:
        max_delay: Plafond du délai en secondes.
        tentative: Compteur de tentatives consécutives.
    """

    def __init__(self, max_delay: int = 60) -> None:
        """Initialise le gestionnaire de reconnexion.

        Args:
            max_delay: Délai maximum en secondes entre deux tentatives.
        """
        self.max_delay: int = max_delay
        self.tentative: int = 0

    def reset(self) -> None:
        """Réinitialise le compteur après une connexion réussie."""
        self.tentative = 0

    def wait(self) -> None:
        """Attend le délai calculé puis incrémente le compteur.

        Le délai suit la formule min(2^tentative, max_delay).
        """
        delai = min(2 ** self.tentative, self.max_delay)
        logger.warning("Reconnexion dans %ss... (tentative %s)", delai, self.tentative)
        time.sleep(delai)
        self.tentative += 1