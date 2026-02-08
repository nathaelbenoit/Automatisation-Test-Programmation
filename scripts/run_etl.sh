#!/bin/bash
# Script de lancement du pipeline ETL
set -e

echo "Attente de la base de donnees..."
./scripts/wait-for-db.sh

echo "Initialisation des tables..."
mysql -h"$DB_HOST" -u"$DB_USER" -p"$DB_PASSWORD" --skip-ssl "$DB_NAME" < scripts/init-db.sql

echo "Execution du pipeline ETL..."
python -m src.main

echo "Termine !"
