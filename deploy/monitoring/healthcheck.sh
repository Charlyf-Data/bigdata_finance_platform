#!/usr/bin/env bash
# Dead man's switch : prouve à Healthchecks.io que chaque flux du pipeline écrit encore
# et que le disque n'approche pas de la saturation.
# Lancé toutes les 5 min par healthcheck.timer, en tant que charly (lecture seule sur data/).
set -euo pipefail

DATA_DIR="${DATA_DIR:-/srv/bigdata/data}"
MAX_AGE_MIN="${MAX_AGE_MIN:-5}"
SEUIL_DISQUE="${SEUIL_DISQUE:-80}"

# Un topic brut par flux continu : si un processus d'ingestion se tait, ses topics le disent.
#   ws-spot           : spot.kline, spot.trade
#   ws-futures-market : futures.trade, futures.markprice
#   ws-futures-public : futures.booktop, futures.depth
#   rest-poller       : futures.open-interest (10 s), futures.depth-snapshot (5 min)
# Chaque topic brut est suivi de son topic v1 : si le normalizer tombe ou rejette un format,
# le v1 se tait alors que le brut continue.
# Exclu : liquidation (événements rares, un silence de plusieurs minutes est normal).
# Format : <topic>[:<âge max en minutes>] ; sans âge, MAX_AGE_MIN s'applique.
TOPICS=(
  binance.spot.kline.raw                 spot.kline.v1
  binance.spot.trade.raw                 spot.trade.v1
  binance.futures.trade.raw              futures.trade.v1
  binance.futures.markprice.raw          futures.markprice.v1
  binance.futures.booktop.raw            futures.booktop.v1
  binance.futures.depth.raw              futures.depth.v1
  binance.futures.open-interest.raw      futures.open-interest.v1
  binance.futures.depth-snapshot.raw:12  futures.depth-snapshot.v1:12
)

# HC_URL vient de healthcheck.env (EnvironmentFile du service) : jamais dans Git
: "${HC_URL:?HC_URL manquant}"

# Une valeur mal saisie (ex. "80%") ferait évaluer (( )) à 0 : alerte permanente ou silencieuse
entier() { [[ "$1" =~ ^[0-9]+$ ]]; }
if ! entier "$SEUIL_DISQUE" || (( SEUIL_DISQUE < 1 || SEUIL_DISQUE > 99 )); then
  echo "ERREUR : SEUIL_DISQUE invalide (${SEUIL_DISQUE}), attendu un entier entre 1 et 99" >&2
  exit 2
fi
if ! entier "$MAX_AGE_MIN" || (( MAX_AGE_MIN < 1 )); then
  echo "ERREUR : MAX_AGE_MIN invalide (${MAX_AGE_MIN}), attendu un entier supérieur à 0" >&2
  exit 2
fi

ping_hc() { curl -fsS -m 10 --retry 3 "$@" >/dev/null; }

problemes=()

# Contrôle 1 : fraîcheur des fichiers Parquet, topic par topic
muets=()
for entree in "${TOPICS[@]}"; do
  topic="${entree%%:*}"
  age="$MAX_AGE_MIN"
  [[ "$entree" == *:* ]] && age="${entree##*:}"
  # Tout le dossier du topic (pas seulement date=aujourd'hui) : pas de faux échec à minuit UTC.
  # part-* seulement : le fichier compacté de la nuit ne doit pas masquer un flux arrêté.
  if ! find "${DATA_DIR}/${topic}" -name 'part-*.parquet' -mmin "-${age}" -print -quit 2>/dev/null | grep -q .; then
    muets+=("${topic} (${age} min)")
  fi
done
if (( ${#muets[@]} > 0 )); then
  liste=$(IFS=','; echo "${muets[*]}")
  problemes+=("${#muets[@]} topic(s) sans fichier récent : ${liste//,/, }")
fi

# Contrôle 2 : occupation du disque (Parquet et Kafka partagent la même partition /)
usage=$(df --output=pcent / | tail -n 1 | tr -dc '0-9')
if (( usage >= SEUIL_DISQUE )); then
  problemes+=("disque à ${usage} % (seuil ${SEUIL_DISQUE} %)")
fi

# Un seul ping par exécution : OK, ou /fail avec tous les motifs
if (( ${#problemes[@]} == 0 )); then
  ping_hc "$HC_URL"
  echo "OK : ${#TOPICS[@]} topics à jour ; disque à ${usage} %"
else
  msg=$(IFS='|'; echo "${problemes[*]}")
  echo "ÉCHEC : ${msg}" >&2
  ping_hc --data-raw "$msg" "${HC_URL}/fail"
  exit 1
fi
