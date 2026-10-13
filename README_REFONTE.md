# Refonte multi-exchange — code complet

Pipeline fonctionnel : Binance (WebSocket + REST) → topics bruts → normalisation v1 → Parquet.
Seul `exchanges/binance/` connaît Binance.

```
common/              events, contracts (v1), errors, topics.txt, kafka_config,
                     kafka_publisher, kafka_reader, backoff, shutdown, logging_setup
exchanges/binance/   adapter, streams, rest_client, normalize
ingestion/           config, adapter (Protocol), stream_callbacks, routing,
                     rest_poller, run_ws, run_rest
processing/          normalizer, run_normalize
storage/             parquet_writer, sink
tests/               51 tests
docker/              docker-compose.pipeline.yml (ajout, le compose de base n'est pas modifié)
Dockerfile, .dockerignore, requirements-pipeline.txt, appliquer_refonte.sh
```

## 1. Installer dans ton projet

L'archive se dépose telle quelle à la racine de `C:\Users\charl\Documents\big_data_architect`.
Elle ajoute des fichiers, elle n'en écrase aucun de ta configuration :
`docker/docker-compose.yml` et `requirements.txt` restent intacts, tes autres dossiers aussi.

Dans WSL, en supposant l'archive téléchargée dans `Téléchargements` :

```bash
cd /mnt/c/Users/charl/Documents/big_data_architect
python3 -m zipfile -e /mnt/c/Users/charl/Downloads/big_data_architect_refonte.zip .
bash appliquer_refonte.sh
```

Le script retire les anciens fichiers de code d'`ingestion/` remplacés par la refonte
(`git rm` quand ils sont suivis, donc récupérables), ajoute à `requirements.txt` les
dépendances qui manquent sans rien retirer, et crée `data/`.
`ingestion/README.md` et `ingestion/files_explication_ingestion/` sont conservés.

Retour arrière : `git restore --staged --worktree ingestion/`, ou `git pull` de ta dernière version.

## 2. Repartir de topics propres

Les noms de topics changent (`klines.raw` devient `binance.spot.kline.raw`). Depuis `docker/` :

```bash
docker compose down -v
```

## 3. Lancer

**Tout dans Docker** (depuis `docker/`) :

```bash
docker compose -f docker-compose.yml -f docker-compose.pipeline.yml up -d --build
docker compose -f docker-compose.yml -f docker-compose.pipeline.yml ps
```

`kafka-init` crée les topics depuis `common/topics.txt`, puis les 6 processus démarrent
(3 WebSocket, REST, normaliseur, stockage) et sont relancés s'ils tombent.

**Ou en local** (depuis la racine, `.venv` activé) :

```bash
pip install -r requirements.txt   # complété par le script
python3 -m ingestion.run_ws spot
python3 -m ingestion.run_ws futures-market
python3 -m ingestion.run_ws futures-public
python3 -m ingestion.run_rest
python3 -m processing.run_normalize
python3 -m storage.sink
```

## Ce que l'ajout Docker change, et pourquoi

`docker/docker-compose.yml` n'est pas modifié. `docker-compose.pipeline.yml` s'y ajoute
seulement quand tu le passes avec `-f`. Fusion vérifiée avec Docker Compose :

| Élément | Effet | Utilité |
| --- | --- | --- |
| Ports | Aucun nouveau : 9092 (Kafka) et 8080 (kafka-ui) inchangés | Aucun conflit possible |
| Réseau | Les 6 processus rejoignent `kafka-network` | Ils joignent Kafka par `kafka:29092` |
| `kafka-init` | Sa commande lit `common/topics.txt` au lieu de la liste écrite en dur | Une seule source de topics ; le carnet reçoit `retention.bytes=-1` |
| Volume `../data:/data` | Monté sur le seul conteneur `sink` | Les fichiers Parquet survivent aux conteneurs |
| `restart: unless-stopped` | Sur les 6 processus | Relancés s'ils plantent, pas après un arrêt volontaire |
| Config Kafka | Inchangée (auto-création désactivée, rétention 7 jours par défaut) | — |

Sans `-f docker-compose.pipeline.yml`, ton environnement démarre exactement comme avant.

## 4. Vérifier

```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic binance.spot.kline.raw
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic spot.kline.v1
ls data/
```

Tests : `python3 -m pytest -q` depuis la racine.

## Ce qui a été vérifié, et ce qui reste à vérifier chez toi

Vérifié : les 51 tests (formats réels des messages Binance, classement des erreurs REST,
planification du poller, écriture Parquet, cohérence entre flux, topics et normaliseurs),
et le lancement de chaque processus avec un arrêt propre sur Ctrl+C, même pendant une attente.

Non vérifié : la connexion réelle à Binance et à Kafka (environnement de test sans réseau).
Au premier lancement, surveille en particulier les groupes futures : les adresses `/public`
et `/market` viennent de la migration Binance d'avril 2026.

## Écarts avec la doc de conception

- `KafkaReader` est dans `common/` : le normaliseur et le stockage lisent tous deux Kafka.
- `RestJob` porte un `symbol` mais pas de marché ; le marché REST est `REST_MARKET`.
- `KafkaPublisher.flush(timeout)` ne bloque jamais plus de 10 s et signale les messages non livrés.
- Les normaliseurs reçoivent `(payload, market, symbol)`, et le contrat v1 couvre les 8 types.

## Changer d'exchange

1. Créer `exchanges/<nom>/` avec `adapter.py`, `streams.py`, `rest_client.py`, `normalize.py`.
2. Ajouter ses topics bruts dans `common/topics.txt`.
3. Remplacer les imports `exchanges.binance` dans `run_ws.py`, `run_rest.py` et `run_normalize.py`.

`common/`, `storage/`, les topics v1 et tout ce qui les lit ne changent pas.
