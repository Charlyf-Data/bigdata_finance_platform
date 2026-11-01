
# Une seule image pour tous les processus du pipeline ; la commande choisit le rôle.
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Utilisateur système dédié, sans shell ni home : les processus ne tournent pas en root.
# UID fixe (10001) pour des permissions prévisibles sur le serveur.
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app

# Dépendances d'abord (couche mise en cache), puis suppression de la liste
# dans la MÊME instruction : elle ne reste pas dans l'image.
COPY requirements-pipeline.txt .
RUN pip install --no-cache-dir -r requirements-pipeline.txt \
    && rm requirements-pipeline.txt

# Code en lecture seule pour l'utilisateur app (propriétaire root) :
# un processus compromis ne peut pas modifier le code.
COPY common ./common
COPY exchanges ./exchanges
COPY ingestion ./ingestion
COPY processing ./processing
COPY storage ./storage

# À partir d'ici, tout s'exécute sous l'utilisateur app.
USER app