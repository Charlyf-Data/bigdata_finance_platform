# Une seule image pour tous les processus du pipeline ; la commande choisit le rôle.
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements-pipeline.txt .
RUN pip install --no-cache-dir -r requirements-pipeline.txt

COPY common ./common
COPY exchanges ./exchanges
COPY ingestion ./ingestion
COPY processing ./processing
COPY storage ./storage
