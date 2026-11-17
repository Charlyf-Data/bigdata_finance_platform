"""Compacte tous les topics bruts d'un jour fermé (par défaut : la veille, en UTC).

Usage :
    python -m storage.run_compaction --data-dir /data [--jour 2026-11-02]

Code de sortie : 0 si tout est compacté (alertes éventuelles comprises), 1 si un topic
est bloqué, 2 si le jour demandé n'est pas fermé.
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from functools import partial
from pathlib import Path

from exchanges.binance.controles import CONTROLES
from quality.porte import controler_jour
from storage.compaction import Statut, compacter_jour

logger = logging.getLogger(__name__)


def kind_du_topic(topic: str) -> str:
    """`binance.futures.depth-snapshot.raw` -> `depth-snapshot`."""
    return topic.split(".")[-2]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--jour", type=date.fromisoformat)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    aujourdhui = datetime.now(timezone.utc).date()
    jour = args.jour or aujourdhui - timedelta(days=1)
    if jour >= aujourdhui:
        logger.error("Jour %s non fermé (aujourd'hui UTC : %s) : refus", jour, aujourdhui)
        return 2

    verifier = partial(controler_jour, controles=CONTROLES)
    bloques = 0
    for dossier_topic in sorted(args.data_dir.glob("*.raw")):
        dossier_jour = dossier_topic / f"date={jour.isoformat()}"
        if not dossier_jour.is_dir():
            continue
        r = compacter_jour(dossier_jour, kind_du_topic(dossier_topic.name), verifier)
        ratio = f", ratio {r.ratio:.2f}" if r.ratio is not None else ""
        logger.info("%s : %s (%d fichiers%s)", dossier_topic.name, r.statut.value,
                    r.fichiers_source, ratio)
        for alerte in r.alertes:
            logger.warning("%s : %s", dossier_topic.name, alerte)
        bloques += r.statut is Statut.BLOQUE
    return 1 if bloques else 0


if __name__ == "__main__":
    sys.exit(main())