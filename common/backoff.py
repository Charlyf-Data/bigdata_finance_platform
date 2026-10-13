"""Délais croissants entre deux tentatives. Calcule, ne dort jamais."""

import random


class ExponentialBackoff:
    """Délai qui double à chaque échec, plafonné, avec un jitter.

    Le jitter tire le délai entre la moitié et la totalité de la valeur
    calculée : plusieurs processus qui tombent ensemble ne se relancent
    pas au même instant.
    """

    def __init__(self, base: float = 1.0, cap: float = 60.0) -> None:
        self.base = base
        self.cap = cap
        self.attempt = 0

    def next_delay(self) -> float:
        """Renvoie le délai à attendre, puis prépare le suivant."""
        delay = min(self.cap, self.base * 2 ** self.attempt)
        self.attempt += 1
        return random.uniform(delay / 2, delay)

    def reset(self) -> None:
        """Après un succès, on repart du délai minimal."""
        self.attempt = 0
