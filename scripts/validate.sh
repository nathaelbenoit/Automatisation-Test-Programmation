#!/bin/bash
# Script de validation d'un fichier CSV

FICHIER=${1:-""}

# Vérification que l'argument est fourni
if [ -z "$FICHIER" ]; then
    echo "Erreur: Aucun fichier specifie"
    echo "Usage: $0 <fichier.csv>"
    exit 1
fi

# Vérification que le fichier existe
if [ ! -f "$FICHIER" ]; then
    echo "Erreur: Le fichier '$FICHIER' n'existe pas"
    exit 1
fi

# Vérification que le fichier n'est pas vide
if [ ! -s "$FICHIER" ]; then
    echo "Erreur: Le fichier '$FICHIER' est vide"
    exit 1
fi

# Compter le nombre de colonnes dans l'en-tête
NB_COLS_HEADER=$(head -1 "$FICHIER" | tr ',' '\n' | wc -l)
echo "Nombre de colonnes (en-tete): $NB_COLS_HEADER"

# Vérifier la cohérence sur toutes les lignes
ERREUR=0
LIGNE_NUM=0

while read ligne; do
    LIGNE_NUM=$((LIGNE_NUM + 1))
    NB_COLS=$(echo "$ligne" | tr ',' '\n' | wc -l)

    if [ "$NB_COLS" -ne "$NB_COLS_HEADER" ]; then
        echo "Erreur ligne $LIGNE_NUM: $NB_COLS colonnes (attendu: $NB_COLS_HEADER)"
        ERREUR=1
    fi
done < "$FICHIER"

if [ $ERREUR -eq 0 ]; then
    echo "Validation reussie: $FICHIER"
    exit 0
else
    echo "Validation echouee: $FICHIER contient des erreurs"
    exit 1
fi
