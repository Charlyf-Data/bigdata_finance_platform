#!/usr/bin/env bash
# Upload nocturne du Bronze vers S3 (stockage objet verrouillé par Object Lock).
# Un jour raw part seulement s'il est fermé, compacté et sans aucun part-* restant.
# rclone copy (jamais sync) + --immutable, puis rclone check. Ne supprime jamais rien.
#
# Codes de sortie :
#   0     tout ce qui est fermé est archivé et vérifié ;
#   20    l'envoi a réussi, mais une anomalie demande une action : jour fermé non
#         archivable (part-* restants, fichier compacté absent) ou topic muet hier ;
#   autre échec de rclone (copie ou vérification).
set -uo pipefail

: "${S3_BUCKET:?S3_BUCKET manquant}"
: "${HC_URL_UPLOAD:?HC_URL_UPLOAD manquant}"
DATA_DIR="${DATA_DIR:-/srv/bigdata/data}"
BWLIMIT="${BWLIMIT:-30M}"
DISTANT="bronze:${S3_BUCKET}"
CODE_ANOMALIE=20

ping_hc() { curl -fsS -m 10 --retry 3 "$@" >/dev/null || true; }

liste=$(mktemp)
journal=$(mktemp)
trap 'rm -f "$liste" "$journal"' EXIT

ping_hc "${HC_URL_UPLOAD}/start"

if ! cd "$DATA_DIR"; then
  ping_hc --data-raw "dossier introuvable : ${DATA_DIR}" "${HC_URL_UPLOAD}/1"
  exit 1
fi

aujourdhui=$(date -u +%F)
hier=$(date -u -d yesterday +%F)
anomalies=0
shopt -s nullglob

# 1. Chaque topic brut doit avoir écrit hier : un dossier absent = un flux muet toute la journée
for topic in *.raw/; do
  topic=${topic%/}
  if [[ ! -d "$topic/date=$hier" ]]; then
    echo "ANOMALIE : rien reçu hier (${hier}) : $topic"
    anomalies=$((anomalies + 1))
  fi
done >>"$journal"

# 2. Jours fermés : candidats à l'envoi, ou anomalie s'ils ne sont pas archivables
for dossier in *.raw/date=*/; do
  dossier=${dossier%/}
  jour=${dossier##*date=}
  [[ $jour < $aujourdhui ]] || continue
  parts=("$dossier"/part-*)
  if (( ${#parts[@]} > 0 )); then
    echo "ANOMALIE : non archivable, ${#parts[@]} part-* restants : $dossier"
    anomalies=$((anomalies + 1))
    continue
  fi
  fichier="$dossier/compacte-${jour}.parquet"
  if [[ ! -f $fichier ]]; then
    echo "ANOMALIE : non archivable, fichier compacté absent : $dossier"
    anomalies=$((anomalies + 1))
    continue
  fi
  printf '%s\n' "$fichier" >>"$liste"
done >>"$journal"

nb=$(wc -l <"$liste")
echo "candidats : ${nb}, anomalies : ${anomalies}" >>"$journal"

# 3. Envoi puis vérification de tout ce qui est archivable, même s'il y a des anomalies
code=0
if (( nb > 0 )); then
  rclone copy --files-from "$liste" --immutable --transfers 2 \
    --bwlimit "$BWLIMIT" --stats 0 -v . "$DISTANT" >>"$journal" 2>&1
  code=$?
  if (( code == 0 )); then
    rclone check --files-from "$liste" --one-way . "$DISTANT" >>"$journal" 2>&1
    code=$?
  fi
fi
if (( code == 0 && anomalies > 0 )); then
  code=$CODE_ANOMALIE
fi
echo "code : ${code}" >>"$journal"

cat "$journal"
ping_hc --data-raw "$(tail -c 10000 "$journal")" "${HC_URL_UPLOAD}/${code}"
exit "$code"
