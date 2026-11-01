#!/usr/bin/env bash
# Recette exécutée SUR LE SERVEUR, avec sudo : appliquer une version, puis vérifier qu'elle tourne.
# Usage : sudo bash /srv/bigdata/remote-deploy.sh 0.1.2
# Code de sortie : 0 = déployé et vérifié, 1 = échec (lu par deploy.sh sur le PC).
set -euo pipefail

REMOTE_DIR="/srv/bigdata"
COMPOSE=(docker compose -f docker-compose.prod.yml)
WAIT_SECONDS=90    # le sink écrit toutes les 60 s : on lui laisse le temps d'écrire au moins une fois

fail() { echo "ÉCHEC : $*" >&2; exit 1; }

# --- Vérifier l'argument ---
[[ $# -eq 1 ]] || fail "usage : $0 X.Y.Z"
[[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "version invalide : $1"
VERSION="$1"

cd "$REMOTE_DIR"

# --- a. Mettre la nouvelle version dans .env ---
grep -q '^APP_VERSION=' .env || fail "ligne APP_VERSION absente de ${REMOTE_DIR}/.env"
sed -i "s/^APP_VERSION=.*/APP_VERSION=${VERSION}/" .env
echo "[serveur] .env -> APP_VERSION=${VERSION}"

# --- b. Télécharger l'image, puis relancer (seuls les conteneurs modifiés sont recréés) ---
"${COMPOSE[@]}" pull --quiet
"${COMPOSE[@]}" up -d

# --- c. Laisser le temps au pipeline de démarrer et au sink d'écrire ---
echo "[serveur] attente de ${WAIT_SECONDS} s avant le smoke test..."
sleep "$WAIT_SECONDS"

# --- d. Smoke test 1 : tous les services tournent (kafka-init exclu : il s'arrête normalement) ---
EXPECTED="$("${COMPOSE[@]}" config --services | grep -vc '^kafka-init$')"
RUNNING="$("${COMPOSE[@]}" ps --services --filter status=running | grep -vc '^kafka-init$' || true)"
[[ "$RUNNING" -eq "$EXPECTED" ]] || {
  "${COMPOSE[@]}" ps -a >&2
  fail "${RUNNING}/${EXPECTED} services en marche"
}

# --- d. Smoke test 2 : le sink a écrit récemment ---
docker logs sink --since 2m 2>&1 | grep -q "lignes écrites" \
  || fail "le sink n'a rien écrit depuis 2 minutes (voir : docker logs sink --tail 20)"

# --- e. Tout est bon ---
echo "[serveur] OK : ${VERSION} tourne, ${RUNNING}/${EXPECTED} services en marche, le sink écrit"
exit 0