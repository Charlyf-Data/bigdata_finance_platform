"""Tests de la compaction d'un jour de Bronze."""

import json
from functools import partial
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from exchanges.binance.controles import CONTROLES
from quality.porte import FideliteCompaction, RapportQualite, Verdict, controler_jour
from storage.compaction import SCHEMA_RAW, Statut, compacter_jour, nom_compacte

VERIFIER = partial(controler_jour, controles=CONTROLES)


def enveloppe(trade_id: int) -> dict:
    payload = {"e": "aggTrade", "E": trade_id, "a": trade_id, "p": "1", "q": "1",
               "T": trade_id, "m": False}
    return {"exchange": "binance", "market": "futures", "kind": "trade", "symbol": "BTCUSDT",
            "received_at": trade_id + 5, "payload": json.dumps(payload)}


def jour_avec_minutes(tmp_path: Path, paquets: list[list[dict]]) -> Path:
    dossier = tmp_path / "binance.futures.trade.raw" / "date=2026-11-02"
    dossier.mkdir(parents=True)
    for i, lignes in enumerate(paquets):
        # from_pylist comme le ParquetWriter de prod : schéma deviné, SNAPPY par défaut
        pq.write_table(pa.Table.from_pylist(lignes), dossier / f"part-{i:04d}.parquet")
    return dossier


def lire(dossier: Path) -> pa.Table:
    return pq.read_table(dossier / nom_compacte(dossier))


def test_compaction_nominale(tmp_path):
    dossier = jour_avec_minutes(tmp_path, [[enveloppe(i) for i in range(1, 51)],
                                           [enveloppe(i) for i in range(51, 101)]])

    r = compacter_jour(dossier, "trade", VERIFIER, taille_lot=7)

    assert r.statut is Statut.COMPACTE and r.fichiers_source == 2 and r.alertes == ()
    assert [f.name for f in dossier.iterdir()] == ["compacte-2026-11-02.parquet"]
    table = lire(dossier)
    assert table.num_rows == 100 and table.schema.equals(SCHEMA_RAW)
    assert pq.ParquetFile(dossier / nom_compacte(dossier)).metadata.row_group(0).column(0).compression == "ZSTD"


def test_schema_impose_sur_colonne_entierement_nulle(tmp_path):
    # Un paquet où symbol vaut toujours None : from_pylist le type `null`, pas `string`
    sans_symbole = [{**enveloppe(i), "symbol": None} for i in range(1, 4)]
    dossier = jour_avec_minutes(tmp_path, [sans_symbole, [enveloppe(4)]])

    r = compacter_jour(dossier, "trade", VERIFIER)

    assert r.statut is Statut.COMPACTE
    assert lire(dossier).schema.field("symbol").type == pa.string()


def test_idempotent(tmp_path):
    dossier = jour_avec_minutes(tmp_path, [[enveloppe(i) for i in range(1, 11)]])
    compacter_jour(dossier, "trade", VERIFIER)
    contenu = lire(dossier)

    r = compacter_jour(dossier, "trade", VERIFIER)

    assert r.statut is Statut.DEJA_FAIT
    assert lire(dossier).equals(contenu)


def test_jour_vide(tmp_path):
    dossier = tmp_path / "binance.futures.trade.raw" / "date=2026-11-02"
    dossier.mkdir(parents=True)
    assert compacter_jour(dossier, "trade", VERIFIER).statut is Statut.VIDE


def test_bloque_ne_supprime_rien(tmp_path):
    dossier = jour_avec_minutes(tmp_path, [[enveloppe(i) for i in range(1, 11)]])
    minutes_avant = sorted(p.name for p in dossier.iterdir())

    def verdict_infidele(con, avant, apres, kind):
        return Verdict(FideliteCompaction(True, 10, 9, 1, 0), RapportQualite(kind, 9, 0))

    r = compacter_jour(dossier, "trade", verdict_infidele)

    assert r.statut is Statut.BLOQUE
    assert sorted(p.name for p in dossier.iterdir()) == minutes_avant  # ni compacté ni .tmp


def test_reprise_apres_interruption(tmp_path):
    # Crash simulé : le fichier compacté existe déjà, mais les minutes n'ont pas été supprimées
    dossier = jour_avec_minutes(tmp_path, [[enveloppe(i) for i in range(1, 11)]])
    pq.write_table(pq.read_table(dossier / "part-0000.parquet").cast(SCHEMA_RAW),
                   dossier / nom_compacte(dossier), compression="zstd")

    r = compacter_jour(dossier, "trade", VERIFIER)

    assert r.statut is Statut.REPRISE
    assert [f.name for f in dossier.iterdir()] == ["compacte-2026-11-02.parquet"]


def test_temporaire_orphelin_nettoye(tmp_path):
    dossier = jour_avec_minutes(tmp_path, [[enveloppe(1)]])
    (dossier / ".compacte-2026-11-02.parquet.tmp").write_bytes(b"debris")

    compacter_jour(dossier, "trade", VERIFIER)

    assert not (dossier / ".compacte-2026-11-02.parquet.tmp").exists()


def test_alerte_qualite_sans_bloquer(tmp_path):
    # Un trou dans les trade_id : signalé, mais la compaction fidèle autorise la suppression
    dossier = jour_avec_minutes(tmp_path, [[enveloppe(1), enveloppe(2), enveloppe(6)]])

    r = compacter_jour(dossier, "trade", VERIFIER)

    assert r.statut is Statut.COMPACTE
    assert r.alertes == ("3 trade(s) manquant(s)",)


def test_doublons_preserves(tmp_path):
    # La compaction ne dédoublonne pas : c'est le travail du Silver
    dossier = jour_avec_minutes(tmp_path, [[enveloppe(1), enveloppe(2)], [enveloppe(2)]])

    r = compacter_jour(dossier, "trade", VERIFIER)

    assert r.statut is Statut.COMPACTE and lire(dossier).num_rows == 3
    assert r.alertes == ("1 doublon(s)",)


def test_colonne_date_hive_ignoree(tmp_path):
    dossier = jour_avec_minutes(tmp_path, [[enveloppe(1)]])
    compacter_jour(dossier, "trade", VERIFIER)
    assert "date" not in lire(dossier).column_names
    # DuckDB lu sans hive_partitioning ne voit pas non plus de colonne date
    cols = [c[0] for c in duckdb.sql(
        f"DESCRIBE SELECT * FROM read_parquet('{dossier}/*.parquet', hive_partitioning=false)"
    ).fetchall()]
    assert "date" not in cols




def test_erreur_pendant_la_verification_bloque_sans_rien_supprimer(tmp_path):
    dossier = jour_avec_minutes(tmp_path, [[enveloppe(i) for i in range(1, 11)]])
    minutes_avant = sorted(p.name for p in dossier.iterdir())

    def verificateur_en_panne(con, avant, apres, kind):
        raise MemoryError("simulation")

    r = compacter_jour(dossier, "trade", verificateur_en_panne)

    assert r.statut is Statut.BLOQUE
    assert r.alertes == ("erreur : MemoryError: simulation",)
    assert sorted(p.name for p in dossier.iterdir()) == minutes_avant  # pas de .tmp laissé