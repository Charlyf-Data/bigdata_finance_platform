"""Erreurs partagées entre ceux qui les lèvent et ceux qui les attrapent.

Ce module est un contrat : un faux client de test lève les mêmes erreurs
que le vrai, sans importer son code.
"""


class InvalidMessage(Exception):
    """Un message ne respecte pas le format attendu (champ manquant, JSON invalide)."""


class ApiError(Exception):
    """Base des erreurs d'appel à une API REST."""


class RateLimited(ApiError):
    """Limite atteinte (429) ou IP bannie (418) : toute l'adresse IP doit se taire."""

    def __init__(self, retry_after: float) -> None:
        super().__init__(f"réessayer dans {retry_after}s")
        self.retry_after = retry_after


class TransientError(ApiError):
    """Panne passagère (timeout, réseau, 5xx) : réessayer ce job plus tard."""


class PermanentError(ApiError):
    """Requête invalide (4xx hors 418 et 429) : insister ne changera rien."""
