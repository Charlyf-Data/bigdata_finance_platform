# Runbook — Disque plein (plan de repli)

**Déclencheur** : alerte Healthchecks.io « disque à X % (seuil 80 %) ».
**Objectif** : libérer de la place **sans perdre de données**, en attendant S3.
**Règle d'or** : on ne supprime jamais sur le serveur avant d'avoir prouvé que la copie est identique.

## 0. Diagnostic (5 min)

Sur le serveur :

```bash
df -h /
cd /srv/bigdata/data && du -sh */ | sort -h | tail -5
sudo docker system df
```

Vérifier que la hausse vient bien des données Parquet (rythme normal ~3,5 Go/jour)
et pas d'une anomalie : Kafka très au-dessus de ~18 Go, ou des logs Docker qui gonflent.

## 1. Leviers, du moins risqué au plus risqué

| Ordre | Levier | Gain | Risque |
|---|---|---|---|
| 1 | Supprimer les anciennes images de l'application (garder la version N-1 pour le rollback) | ~1 Go | Nul |
| 2 | Rapatrier les jours les plus anciens sur le PC (section 2) | ~3,5 Go/jour | Nul si la procédure est suivie |
| 3 | Dernier recours : baisser temporairement la rétention Kafka | Variable | Règle « rétention Kafka > panne max du sink » enfreinte |

## 2. Rapatrier un jour sur le PC

Un jour à la fois, **jamais le jour en cours** (le sink y écrit encore).

### 2.1 Serveur : inventaire et archive

```bash
JOUR=2026-11-01
cd /srv/bigdata/data
ls -d */date=${JOUR:?}
find */date=${JOUR} -type f | wc -l          # nb de fichiers source : NOTER
du -sch */date=${JOUR} | tail -1
tar -cf /srv/bigdata/repli-${JOUR}.tar */date=${JOUR}
tar -tf /srv/bigdata/repli-${JOUR}.tar | grep -vc '/$'   # doit égaler le nb source
sha256sum /srv/bigdata/repli-${JOUR}.tar                 # empreinte de référence : NOTER
```

Pas de compression : le Parquet est déjà compressé.
L'archive a besoin d'autant d'espace libre que le jour lui-même.

### 2.2 PC (WSL) : copie et vérification

```bash
cd /mnt/c/Users/charl/Documents/big_data_architect
mkdir -p data/archive
scp.exe bigdata-prod:/srv/bigdata/repli-2026-11-01.tar data/archive/
sha256sum data/archive/repli-2026-11-01.tar               # identique au serveur ?
tar -tf data/archive/repli-2026-11-01.tar | grep -vc '/$' # même nb de fichiers ?
```

`data/` est ignoré par Git : l'archive ne sera jamais commitée.

### 2.3 Serveur : suppression, seulement si empreinte ET nb de fichiers sont identiques

```bash
ls -d /srv/bigdata/data/*/date=${JOUR:?}     # relire la liste avant de supprimer
sudo rm -r -- /srv/bigdata/data/*/date=${JOUR:?}
rm /srv/bigdata/repli-${JOUR}.tar
df -h /
```

`${JOUR:?}` arrête la commande si la variable est vide : impossible de supprimer
autre chose que le jour visé.

## 3. Après le repli

- Noter dans le journal ci-dessous : date, jours rapatriés, Go libérés.
- Les jours rapatriés devront rejoindre S3 en phase 4. La clé S3 du PC étant en
  lecture seule, il faudra soit les renvoyer sur le serveur pour le circuit
  normal (compaction → upload), soit prévoir une procédure dédiée.

## Journal des replis

| Date | Jours rapatriés | Go libérés | Remarques |
|---|---|---|---|