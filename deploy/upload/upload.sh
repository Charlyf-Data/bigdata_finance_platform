#!/usr/bin/env bash
# Upload nocturne du Bronze vers S3 (Hetzner Object Storage, Object Lock).
# Un jour raw part seulement s'il est fermé, compacté et sans aucun part-* restant.
# rclone copy (jamais sync) + --immutable, puis rclone check. Ne supprime jamais rien.
set -uo pipefail

: "${S3_BUCKET:?S3_BUCKET manquant}"
: "${HC_URL_UPLOAD:?HC_URL_UPLOAD manquant}"
DATA_DIR="${DATA_DIR:-/srv/bigdata/data}"
BWLIMIT="${BWLIMIT:-30M}"
DISTANT="bronze:${S3_BUCKET}"

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
ignores=0
shopt -s nullglob
for f in *.raw/date=*/compacte-*.parquet; do
  dossier=${f%/*}
  jour=${dossier##*date=}
  [[ $jour < $aujourdhui ]] || continue
  if [[ ${f##*/} != "compacte-${jour}.parquet" ]]; then
    echo "ignoré (nom inattendu) : $f"; ignores=$((ignores + 1)); continue
  fi
  parts=("$dossier"/part-*)
  if (( ${#parts[@]} > 0 )); then
    echo "ignoré (${#parts[@]} part-* restants) : $dossier"; ignores=$((ignores + 1)); continue
  fi
  printf '%s\n' "$f" >>"$liste"
done >>"$journal"

nb=$(wc -l <"$liste")
echo "candidats : ${nb}, ignorés : ${ignores}" >>"$journal"

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
echo "code : ${code}" >>"$journal"

cat "$journal"
ping_hc --data-raw "$(tail -c 10000 "$journal")" "${HC_URL_UPLOAD}/${code}"
exit "$code"
