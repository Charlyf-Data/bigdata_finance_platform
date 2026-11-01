#!/usr/bin/env bash
# Déploie une version publiée sur le serveur de prod.
# Usage : ./deploy/deploy.sh 0.1.2
# Lancé depuis le PC (WSL). 1-5 : contrôles pré-vol (rien n'est modifié) · 6 : copie · 7 : application + smoke test.
set -euo pipefail

# Se placer à la racine du dépôt (git show en a besoin), d'où que le script soit lancé
cd "$(git rev-parse --show-toplevel)"

# --- Constantes (aucun secret : bigdata-prod est un alias de ~/.ssh/config) ---
SSH="${SSH:-ssh.exe}"
SCP="${SCP:-scp.exe}"
HOST="bigdata-prod"
REMOTE_DIR="/srv/bigdata"
IMAGE="ghcr.io/charlyf-data/bigdata-finance-platform"

# Affiche un message d'erreur et arrête le script
die() { echo "ERREUR : $*" >&2; exit 1; }

# --- 0. Les outils sont là ---
command -v docker >/dev/null || die "docker introuvable : Docker Desktop est-il lancé ?"
docker info >/dev/null 2>&1   || die "le démon Docker ne répond pas : Docker Desktop est-il lancé ?"

# --- 1. Arguments : exactement 1, au format X.Y.Z ---
[[ $# -eq 1 ]] || die "usage : $0 X.Y.Z"
[[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "version invalide : $1 (attendu X.Y.Z, sans v)"
VERSION="$1"
TAG="v${VERSION}"
echo "==> Déploiement de ${TAG}"

# --- 2. Le tag existe dans le dépôt ---
git fetch --tags --quiet
git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null || die "tag ${TAG} introuvable"
echo "    tag ${TAG} : OK"

# --- 3. L'image existe sur ghcr.io (on garde le message d'erreur pour l'afficher) ---
if ! OUT="$(docker buildx imagetools inspect "${IMAGE}:${VERSION}" 2>&1)"; then
  die "image ${IMAGE}:${VERSION} introuvable : ${OUT}"
fi
echo "    image ${VERSION} : OK"

# --- 4. Extraire les fichiers DU TAG (pas de la copie locale) ---
# Dossier temporaire DANS le dépôt (ignoré par Git) : scp.exe, programme Windows, ne voit pas le /tmp de WSL.
TMP="$(mktemp -d .deploy-tmp.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT    # nettoie le dossier temporaire à la sortie, même en cas d'erreur

for f in deploy/docker-compose.prod.yml deploy/remote-deploy.sh common/topics.txt; do
  git show "${TAG}:${f}" > "${TMP}/$(basename "$f")" 2>/dev/null \
    || die "${f} absent du tag ${TAG} (tag antérieur à ce fichier ?)"
done
echo "    fichiers extraits du tag : OK"

# --- 5. Garde-fou : aucun topic retiré par rapport au serveur ---
"$SSH" "$HOST" cat "${REMOTE_DIR}/topics.txt" > "${TMP}/topics.old.txt" \
  || die "impossible de lire ${REMOTE_DIR}/topics.txt sur ${HOST}"

# Lignes présentes dans l'ANCIEN fichier mais absentes du NOUVEAU (commentaires et lignes vides ignorés)
clean() { grep -v '^#' "$1" | grep -v '^[[:space:]]*$' | sort; }
REMOVED="$(comm -23 <(clean "${TMP}/topics.old.txt") <(clean "${TMP}/topics.txt"))"
if [[ -n "$REMOVED" ]]; then
  echo "Topics retirés par ${TAG} :" >&2
  echo "$REMOVED" >&2
  die "déploiement annulé : supprimer un topic est une décision manuelle"
fi
ADDED="$(comm -13 <(clean "${TMP}/topics.old.txt") <(clean "${TMP}/topics.txt"))"
if [[ -n "$ADDED" ]]; then
  echo "    nouveaux topics (kafka-init les créera) :"
  echo "$ADDED"
fi
echo "    topics : OK"

# --- 6. Noter la version actuelle (pour le retour arrière), puis copier les fichiers ---
OLD_VERSION="$("$SSH" "$HOST" "grep '^APP_VERSION=' ${REMOTE_DIR}/.env | cut -d= -f2" | tr -d '\r')"
echo "    version actuelle sur le serveur : ${OLD_VERSION:-inconnue}"

"$SCP" -q "${TMP}/docker-compose.prod.yml" "${TMP}/topics.txt" "${TMP}/remote-deploy.sh" "${HOST}:${REMOTE_DIR}/" \
  || die "copie des fichiers vers ${HOST} impossible"
echo "    fichiers copiés : OK"

# --- 7. Appliquer + smoke test sur le serveur, en UN seul appel (un seul mot de passe sudo) ---
echo "==> Application sur ${HOST} (mot de passe sudo demandé)"
if "$SSH" -t "$HOST" "sudo bash ${REMOTE_DIR}/remote-deploy.sh ${VERSION}"; then
  echo "==> ${TAG} déployée et vérifiée (${OLD_VERSION:-?} -> ${VERSION})"
else
  die "échec du déploiement ou du smoke test. Retour arrière : ./deploy/deploy.sh ${OLD_VERSION:-<ancienne version>}"
fi