# FROM python:3.11-slim

# # Installer les outils nécessaires
# RUN apt-get update && apt-get install -y \
#     bash \
#     && rm -rf /var/lib/apt/lists/*

# # Définir le répertoire de travail
# WORKDIR /app

# # Copier et installer les dépendances Python
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # Copier le code source
# COPY . .

# # Rendre les scripts exécutables
# RUN chmod +x scripts/*.sh || true

# # Commande par défaut
# CMD ["bash"]

FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    bash \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x scripts/*.sh || true

CMD ["bash"]
