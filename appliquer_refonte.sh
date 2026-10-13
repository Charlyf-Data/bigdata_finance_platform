#!/usr/bin/env bash
# Applique la refonte. À lancer depuis la racine de big_data_architect :
#   bash appliquer_refonte.sh
#
# Ce script ne touche qu'à ce que la refonte remplace :
# - il retire les anciens fichiers de code d'ingestion/ (récupérables avec git) ;
# - il ajoute à requirements.txt les dépendances manquantes, sans rien retirer.
# Il ne touche ni docker/docker-compose.yml, ni aux autres dossiers du projet.
set -euo pipefail

if [ ! -d docker ] || [ ! -d ingestion ] || [ ! -d common ]; then
  echo "À lancer depuis la racine de big_data_architect, après avoir extrait l'archive."
  exit 1
fi

echo "== 1. Anciens fichiers d'ingestion remplacés par la refonte"
for f in binance_consumer.py callbacks.py binance_config.py kafka_config.py \
         kafka_publisher.py reconnect.py shutdown.py transform.py validation.py; do
  path="ingestion/$f"
  [ -f "$path" ] || continue
  if git ls-files --error-unmatch "$path" >/dev/null 2>&1; then
    git rm -q "$path"
    echo "   retiré (suivi par git, récupérable) : $path"
  else
    rm "$path"
    echo "   retiré (non suivi par git) : $path"
  fi
done
rm -rf ingestion/__pycache__

echo "== 2. Dépendances ajoutées à requirements.txt (rien n'est retiré)"
touch requirements.txt
if [ -s requirements.txt ] && [ -n "$(tail -c1 requirements.txt)" ]; then echo >> requirements.txt; fi
while IFS= read -r line || [ -n "$line" ]; do
  [ -z "$line" ] && continue
  name="${line%%[<>=]*}"
  if grep -qiE "^${name}([<>=[:space:]]|$)" requirements.txt; then
    echo "   déjà présent : $name"
  else
    echo "$line" >> requirements.txt
    echo "   ajouté : $line"
  fi
done < requirements-pipeline.txt

echo "== 3. Dossier des fichiers Parquet"
mkdir -p data

echo
echo "Refonte appliquée. Suite : README_REFONTE.md, section 2."
echo "Pour revenir en arrière : git restore --staged --worktree ingestion/"
