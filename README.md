# Ingestion — Consumer Binance WebSocket

Module d'ingestion temps réel des données de marché Binance.
Première brique du pipeline BigData Finance : il se connecte au flux
WebSocket public de Binance, reçoit les bougies (klines) en continu et
maintient une connexion résiliente prête à alimenter Kafka.


---

## Vue d'ensemble

Le consumer ouvre une connexion WebSocket vers Binance et écoute les
klines (bougies OHLCV) d'une paire de trading. Il est conçu pour tourner
en continu en production : il se reconnecte automatiquement en cas de
coupure et s'arrête proprement sur demande.

Aucune clé API n'est nécessaire : les flux de données de marché Binance
sont publics.

---

## Qu'est-ce qu'une kline ?

Une **kline** (contraction de *candlestick line*) est une bougie de
chandelier japonais représentant l'activité de marché sur un intervalle
de temps donné (ici 1 minute). Elle contient les valeurs **OHLCV** :

| Champ | Signification |
|-------|---------------|
| O (Open)   | Prix d'ouverture de l'intervalle |
| H (High)   | Prix le plus haut atteint |
| L (Low)    | Prix le plus bas atteint |
| C (Close)  | Prix de clôture de l'intervalle |
| V (Volume) | Volume échangé pendant l'intervalle |

Le champ `x` indique si la bougie est **fermée** :

- `x = false` → bougie en cours de formation, ses valeurs évoluent encore
- `x = true`  → bougie fermée et définitive

Seules les bougies fermées doivent être persistées vers Kafka, pour
éviter d'enregistrer plusieurs versions de la même bougie.

---

## Architecture des fichiers

Le module suit le principe de **responsabilité unique** : chaque fichier
fait une seule chose.

```
ingestion/
├── config.py              # paramètres de connexion (dataclass immuable)
├── callbacks.py           # réactions aux événements WebSocket
├── reconnect.py           # logique de backoff exponentiel
├── shutdown.py            # arrêt propre via signaux système
└── binance_consumer.py    # point d'entrée — assemble et lance
```

| Fichier | Type | Responsabilité |
|---------|------|----------------|
| `config.py` | `@dataclass(frozen=True)` | Regroupe les paramètres fixes et construit l'URL |
| `callbacks.py` | classe | Réagit aux événements WebSocket et journalise |
| `reconnect.py` | classe | Calcule et applique le délai de reconnexion |
| `shutdown.py` | classe | Capte les signaux d'arrêt et ferme la connexion |
| `binance_consumer.py` | script | Orchestration de la boucle principale |

Le choix classe vs fonction suit une règle simple : **une classe quand
il y a un état qui persiste** (compteur de tentatives, drapeau d'arrêt),
**une dataclass quand ce sont des paramètres fixes regroupés**.

---

## La logique de connexion résiliente

Le cœur du consumer est une boucle qui maintient la connexion vivante
en permanence et gère trois scénarios de déconnexion.

```
while not should_stop:
    run_forever()          # bloque tant que la connexion est active
    if not should_stop:
        reconnect.wait()   # backoff avant de relancer
```

Deux niveaux distincts travaillent ensemble :

- **`run_forever()`** maintient **une** connexion vivante. Il rend la
  main dès que cette connexion se ferme.
- **La boucle `while`** décide s'il faut **relancer** une nouvelle
  connexion après que `run_forever()` ait rendu la main.

Sans la boucle, le consumer mourrait à la première déconnexion.

---

## Les trois scénarios de fermeture

![Logique de fermeture et reconnexion](files_explication_ingestion/image.png)

| Scénario | Signal reçu ? | Comportement |
|----------|---------------|--------------|
| Déconnexion 24h / réseau | Non | Reconnexion avec backoff |
| Ctrl+C (dev local) | SIGINT | Arrêt propre |
| `docker stop` (prod) | SIGTERM | Arrêt propre |

La distinction repose entièrement sur les **signaux système**, pas sur
les codes de fermeture WebSocket (peu fiables, notamment sur WSL).

- **Un signal reçu** = arrêt volontaire → on stoppe la boucle
- **Aucun signal** = déconnexion subie → on reconnecte

`SIGINT` et `SIGTERM` pointent vers le **même handler** qui met
`should_stop = True` et ferme la connexion pour interrompre
`run_forever()` immédiatement.

---

## Le backoff exponentiel

Pour éviter le bannissement IP de Binance (limite de 300 connexions par
5 minutes), le délai entre deux tentatives de reconnexion **double** à
chaque échec, jusqu'à un plafond.

```
délai = min(2 ^ tentative, max_delay)
```

| Tentative | Délai |
|-----------|-------|
| 0 | 1 s |
| 1 | 2 s |
| 2 | 4 s |
| 3 | 8 s |
| ... | ... |
| 6+ | 60 s (plafond) |

Le compteur est **remis à zéro** dès qu'une connexion réussit
(`on_open` appelle `reconnect.reset()`). Une connexion stable repart
donc toujours d'un délai minimal.

---

## Limites Binance respectées

| Limite | Valeur | Gérée par |
|--------|--------|-----------|
| Connexions | 300 / 5 min / IP | Backoff exponentiel |
| Messages entrants | 5 / seconde | N/A (on ne fait qu'écouter) |
| Streams par connexion | 1024 max | 1 seul stream utilisé |
| Durée connexion | 24h max | Reconnexion automatique |

Le ping/pong est géré automatiquement par `websocket-client` : Binance
envoie un ping toutes les 20 secondes, la librairie répond seule.

---

## Lancer le consumer

```bash
pip install -r requirements.txt
python3 binance_consumer.py
```

Dépendances :

```
websocket-client
kafka-python
```

Arrêt : `Ctrl+C` en local, `docker stop` en conteneur.

---

## Configuration

Tous les paramètres sont dans `config.py` (`BinanceConfig`) :

```python
ws_base = "wss://data-stream.binance.vision/ws"
symbol = "btcusdt"
interval = "1m"
max_reconnect_delay = 60
```

Pour suivre une autre paire ou un autre intervalle, modifier `symbol`
ou `interval`. L'URL est construite par `build_url()`.

Aucun fichier `.env` n'est nécessaire à ce stade : les données de
marché sont publiques et ne contiennent aucun secret. Le `.env`
deviendra utile à l'étape Kafka (adresse du broker, credentials de
stockage, tokens d'alerte).

---

## Prochaine étape

Brancher Kafka : parser le JSON reçu, filtrer les bougies fermées
(`x = true`) et les produire dans un topic Kafka. L'approche prévue est
de créer une classe `KafkaBinanceCallbacks` qui **hérite** de
`BinanceCallbacks` et surcharge uniquement `on_message`, sans toucher au
reste du code.