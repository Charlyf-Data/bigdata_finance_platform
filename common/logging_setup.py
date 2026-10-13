"""Configuration des logs, identique pour tous les processus."""

import logging


def setup_logging(level: int = logging.INFO) -> None:
    """À appeler une seule fois, au démarrage de chaque point d'entrée."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
