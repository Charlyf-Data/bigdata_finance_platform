#!/usr/bin/env bash
# Dead man's switch : prouve à Healthchecks.io que le pipeline écrit encore
# et que le disque n'approche pas de la saturation.
# Lancé toutes les 5 min par healthcheck.timer, en tant que charly (lecture seule sur data/).
set -euo pipefail

DATA_DIR="${DATA_DIR:-/srv/bigdata/data}"
MAX_AGE_MIN="${MAX_AGE_MIN:-5}"
SEUIL_DISQUE="${SEUIL_DISQUE:-80}"
# Un topic brut + un topic normalisé : si le normalizer tombe, le v1 se tait alors que le raw continue
TOPICS=(binance.futures.trade.raw futures.trade.v1)

# HC_URL vient de healthcheck.env (EnvironmentFile du service) : jamais dans Git
: "${HC_URL:?HC_URL manquant}"

# Un seuil mal saisi (ex. "80%") ferait évaluer (( )) à 0 : alerte permanente ou silencieuse
if ! [[ "$SEUIL_DISQUE" =~ ^[0-9]+$ ]] || (( SEUIL_DISQUE < 1 || SEUIL_DISQUE > 99 )); then
  echo "ERREUR : SEUIL_DISQUE invalide (${SEUIL_DISQUE}), attendu un entier entre 1 et 99" >&2
  exit 2
fi

ping_hc() { curl -fsS -m 10 --retry 3 "$@" >/dev/null; }

problemes=()

# Contrôle 1 : fraîcheur des fichiers Parquet
stale=()
for t in "${TOPICS[@]}"; do
  # Tout le dossier du topic (pas seulement date=aujourd'hui) : pas de faux échec à minuit UTC
  if ! find "${DATA_DIR}/${t}" -name '*.parquet' -mmin "-${MAX_AGE_MIN}" -print -quit 2>/dev/null | grep -q .; then
    stale+=("$t")
  fi
done
if (( ${#stale[@]} > 0 )); then
  problemes+=("aucun fichier Parquet depuis ${MAX_AGE_MIN} min : ${stale[*]}")
fi

# Contrôle 2 : occupation du disque (Parquet et Kafka partagent la même partition /)
usage=$(df --output=pcent / | tail -n 1 | tr -dc '0-9')
if (( usage >= SEUIL_DISQUE )); then
  problemes+=("disque à ${usage} % (seuil ${SEUIL_DISQUE} %)")
fi

# Un seul ping par exécution : OK, ou /fail avec tous les motifs
if (( ${#problemes[@]} == 0 )); then
  ping_hc "$HC_URL"
  echo "OK : fichiers récents pour ${TOPICS[*]} ; disque à ${usage} %"
else
  msg=$(IFS='|'; echo "${problemes[*]}")
  echo "ÉCHEC : ${msg}" >&2
  ping_hc --data-raw "$msg" "${HC_URL}/fail"
  exit 1
fi
