#!/usr/bin/env bash
# Compaction nocturne du Bronze (J-1), lancée par compaction.timer.
# Configuration dans compaction.env (EnvironmentFile du service, root 600) : jamais dans Git.
set -uo pipefail   # pas de -e : on doit récupérer le code de sortie pour Healthchecks

: "${COMPACTION_IMAGE:?COMPACTION_IMAGE manquant}"
: "${HC_URL_COMPACTION:?HC_URL_COMPACTION manquant}"
DATA_DIR="${DATA_DIR:-/srv/bigdata/data}"

ping_hc() { curl -fsS -m 10 --retry 3 "$@" >/dev/null || true; }   # un ping raté ne doit pas faire échouer la compaction

ping_hc "${HC_URL_COMPACTION}/start"

journal=$(docker run --rm --name compaction-nuit \
  --network none --memory 1g --cpus 2 --pids-limit 64 \
  --cap-drop ALL --security-opt no-new-privileges \
  --read-only --tmpfs /tmp \
  -v "${DATA_DIR}:/data" \
  --entrypoint python "${COMPACTION_IMAGE}" \
  -m storage.run_compaction --data-dir /data 2>&1)
  code=$?

echo "${journal}"
# Code 0 = OK (alertes qualité éventuelles dans le corps), 1 = topic bloqué, autre = erreur
ping_hc --data-raw "$(printf '%s\n' "${journal}" | tail -c 10000)" "${HC_URL_COMPACTION}/${code}"
exit "${code}"