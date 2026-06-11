# Pipeline ETL - Automatisation, Test & Programmation

Projet universitaire de pipeline **ETL (Extract, Transform, Load)** qui extrait des données depuis des fichiers CSV, les nettoie et les transforme, puis les charge dans une base de données MySQL. Le tout est conteneurisé avec Docker.

## Architecture du projet

```
.
├── data/raw/               # Données sources CSV
│   ├── Persons.csv         # 25 personnes (id, nom, email, ville...)
│   └── Transactions.csv    # 30 transactions (id, montant, catégorie...)
├── scripts/
│   ├── init-db.sql         # Création des tables MySQL
│   ├── run_etl.sh          # Lancement complet du pipeline
│   ├── explore.sh          # Exploration et rapport des fichiers CSV
│   ├── validate.sh         # Validation de la structure d'un CSV
│   └── wait-for-db.sh      # Attente de la disponibilité de MySQL
├── src/
│   ├── config.py           # Configuration via variables d'environnement
│   ├── database.py         # Connexion MySQL avec retry
│   ├── etl.py              # Logique ETL (extract, transform, load)
│   └── main.py             # Point d'entrée du pipeline
├── calcul.py               # Fonctions utilitaires (addition, soustraction)
├── main.py                 # Script d'accueil Git
├── Dockerfile              # Image Python 3.11 + client MySQL
├── docker-compose.yml      # Services db (MySQL 8.0) + app
├── requirements.txt        # Dépendances Python
└── .gitignore
```

## Prérequis

- [Docker](https://www.docker.com/) et Docker Compose

## Démarrage rapide

### 1. Lancer l'environnement

```bash
docker compose up -d --build
```

Cela démarre :
- **db** : MySQL 8.0 (port `3306`) avec healthcheck intégré
- **app** : conteneur Python avec le code source monté en volume

### 2. Exécuter le pipeline ETL

```bash
docker exec -it etl_app bash scripts/run_etl.sh
```

Ce script :
1. Attend que la base de données soit prête
2. Initialise les tables (`persons`, `transactions`)
3. Exécute le pipeline Python (`src.main`)

### 3. Explorer les données

```bash
# Générer un rapport d'exploration des CSV
docker exec -it etl_app bash scripts/explore.sh

# Valider la structure d'un fichier CSV
docker exec -it etl_app bash scripts/validate.sh /app/data/raw/Persons.csv
```

## Pipeline ETL - Détail

### Extract
Lecture des fichiers CSV avec Pandas depuis `data/raw/`.

### Transform
- **Persons** : dédoublonnage sur `person_id`, conversion des dates, nettoyage des emails invalides (sans `@`)
- **Transactions** : dédoublonnage sur `transaction_id`, conversion des dates et montants en types appropriés

### Load
Insertion dans MySQL avec gestion des doublons (`ON DUPLICATE KEY UPDATE`).

## Schéma de la base de données

| Table          | Colonnes principales                                                    |
|----------------|-------------------------------------------------------------------------|
| `persons`      | `person_id` (PK), `first_name`, `last_name`, `email`, `birth_date`, `city`, `country` |
| `transactions` | `transaction_id` (PK), `person_id` (FK), `amount`, `currency`, `transaction_date`, `category`, `status` |

## Configuration

La configuration se fait via variables d'environnement (définies dans `docker-compose.yml`) :

| Variable      | Défaut      | Description              |
|---------------|-------------|--------------------------|
| `DB_HOST`     | `localhost` | Hôte de la base MySQL    |
| `DB_PORT`     | `3306`      | Port MySQL               |
| `DB_NAME`     | `etl_db`    | Nom de la base           |
| `DB_USER`     | `root`      | Utilisateur MySQL        |
| `DB_PASSWORD` | `root`      | Mot de passe MySQL       |
| `DATA_DIR`    | `/app/data/raw` | Répertoire des CSV   |

## Dépendances Python

- `pandas 2.1.0` — manipulation de données
- `mysql-connector-python 8.2.0` — connecteur MySQL
- `pytest 7.4.0` — framework de tests

## Arrêter l'environnement

```bash
docker compose down        # Arrêter les conteneurs
docker compose down -v     # Arrêter et supprimer les volumes (données MySQL)
```
