#!/usr/bin/env bash
# Dead man's switch : prouve à Healthchecks.io que le pipeline écrit encore.
# Lancé toutes les 5 min par healthcheck.timer, en tant que charly (lecture seule sur data/).
set -euo pipefail

DATA_DIR="${DATA_DIR:-/srv/bigdata/data}"
MAX_AGE_MIN="${MAX_AGE_MIN:-5}"
# Un topic brut + un topic normalisé : si le normalizer tombe, le v1 se tait alors que le raw continue
TOPICS=(binance.futures.trade.raw futures.trade.v1)

# HC_URL vient de healthcheck.env (EnvironmentFile du service) : jamais dans Git
: "${HC_URL:?HC_URL manquant}"

ping_hc() { curl -fsS -m 10 --retry 3 "$@" >/dev/null; }

stale=()
for t in "${TOPICS[@]}"; do
  # Tout le dossier du topic (pas seulement date=aujourd'hui) : pas de faux échec à minuit UTC
  if ! find "${DATA_DIR}/${t}" -name '*.parquet' -mmin "-${MAX_AGE_MIN}" -print -quit 2>/dev/null | grep -q .; then
    stale+=("$t")
  fi
done

if (( ${#stale[@]} == 0 )); then
  ping_hc "$HC_URL"
  echo "OK : fichiers récents pour ${TOPICS[*]}"
else
  msg="aucun fichier Parquet depuis ${MAX_AGE_MIN} min : ${stale[*]}"
  echo "ÉCHEC : ${msg}" >&2
  ping_hc --data-raw "$msg" "${HC_URL}/fail"
  exit 1
fi