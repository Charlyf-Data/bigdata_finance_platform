#!/usr/bin/env bash
# Purge locale : supprime du serveur les jours plus vieux que JOURS_LOCAL,
# seulement si le raw correspondant est dans S3 et identique (rclone check).
#  raw : jour D supprimé si D < limite, compacté, sans part-*, vérifié dans S3.
#  v1  : jour D supprimé si D < limite et raw D et D+1 vérifiés dans S3
#        (v1 rangé par event_time : un événement de D peut arriver après minuit).
# DRY_RUN=1 par défaut : liste ce qui serait supprimé, sans rien toucher.
set -uo pipefail

: "${S3_BUCKET:?S3_BUCKET manquant}"
: "${HC_URL_PURGE:?HC_URL_PURGE manquant}"
DATA_DIR="${DATA_DIR:-/srv/bigdata/data}"
JOURS_LOCAL="${JOURS_LOCAL:-7}"
DRY_RUN="${DRY_RUN:-1}"
DISTANT="bronze:${S3_BUCKET}"

ping_hc() { curl -fsS -m 10 --retry 3 "$@" >/dev/null || true; }
liste=$(mktemp)
journal=$(mktemp)
trap 'rm -f "$liste" "$journal"' EXIT
log() { echo "$*" >>"$journal"; }
fin() {
  cat "$journal"
  ping_hc --data-raw "$(tail -c 10000 "$journal")" "${HC_URL_PURGE}/$1"
  exit "$1"
}

ping_hc "${HC_URL_PURGE}/start"
cd "$DATA_DIR" || { log "dossier introuvable : ${DATA_DIR}"; fin 1; }
if ! [[ $JOURS_LOCAL =~ ^[0-9]+$ ]] || (( JOURS_LOCAL < 2 )); then
  log "JOURS_LOCAL invalide : ${JOURS_LOCAL}"; fin 1
fi

limite=$(date -u -d "-${JOURS_LOCAL} days" +%F)
if [[ $DRY_RUN == 0 ]]; then mode="RÉEL"; else mode="ESSAI À BLANC"; fi
log "mode : ${mode}, on garde ${JOURS_LOCAL} jours, purge des jours < ${limite}"
log "disque avant : $(df -h --output=pcent "$DATA_DIR" | tail -1 | tr -d ' ')"

shopt -s nullglob
# 1. raw complets jusqu'à la limite incluse (le lendemain sert au v1)
for f in *.raw/date=*/compacte-*.parquet; do
  dossier=${f%/*}
  jour=${dossier##*date=}
  [[ $jour > $limite ]] && continue
  [[ ${f##*/} == "compacte-${jour}.parquet" ]] || continue
  parts=("$dossier"/part-*)
  (( ${#parts[@]} == 0 )) || continue
  printf '%s\n' "$f" >>"$liste"
done

nb=$(wc -l <"$liste")
log "raw à vérifier dans S3 : ${nb}"
(( nb > 0 )) || { log "rien à purger"; fin 0; }

# 2. preuve : tout doit être identique dans S3, sinon on ne supprime rien
if ! rclone check --files-from "$liste" --one-way . "$DISTANT" >>"$journal" 2>&1; then
  log "check S3 en échec : RIEN SUPPRIMÉ"; fin 2
fi
declare -A verifie
while IFS= read -r f; do
  d=${f%/*}
  verifie["${d%%/*}|${d##*date=}"]=1
done <"$liste"

erreurs=0
supprime() {
  if ! [[ $1 =~ ^[a-z0-9.-]+/date=[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    log "chemin refusé : $1"; erreurs=$((erreurs + 1)); return
  fi
  if [[ $DRY_RUN != 0 ]]; then log "à supprimer : $1"; return; fi
  if rm -rf -- "$1"; then log "supprimé : $1"; else log "ÉCHEC : $1"; erreurs=$((erreurs + 1)); fi
}

# 3. raw
n_raw=0
for d in *.raw/date=*/; do
  d=${d%/}
  jour=${d##*date=}
  [[ $jour < $limite ]] || continue
  if [[ -n ${verifie["${d%%/*}|${jour}"]:-} ]]; then
    supprime "$d"; n_raw=$((n_raw + 1))
  else
    log "gardé (non vérifié dans S3) : $d"
  fi
done

# 4. v1 (futures.trade.v1 <- binance.futures.trade.raw)
n_v1=0
for d in *.v1/date=*/; do
  d=${d%/}
  jour=${d##*date=}
  [[ $jour < $limite ]] || continue
  topic=${d%%/*}
  raw="binance.${topic%.v1}.raw"
  lendemain=$(date -u -d "${jour} +1 day" +%F)
  if [[ -n ${verifie["${raw}|${jour}"]:-} && -n ${verifie["${raw}|${lendemain}"]:-} ]]; then
    supprime "$d"; n_v1=$((n_v1 + 1))
  else
    log "gardé (raw ${jour} ou ${lendemain} non vérifié) : $d"
  fi
done

log "raw : ${n_raw} jours, v1 : ${n_v1} jours, erreurs : ${erreurs}"
log "disque après : $(df -h --output=pcent "$DATA_DIR" | tail -1 | tr -d ' ')"
(( erreurs == 0 )) || fin 3
fin 0
