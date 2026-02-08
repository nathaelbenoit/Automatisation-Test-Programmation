#!/bin/bash
# Script d'exploration des fichiers CSV

DATA_DIR=${1:-"/app/data/raw"}
OUTPUT_FILE="/app/output/rapport.txt"

echo "=== Rapport d'exploration ===" > $OUTPUT_FILE
echo "Date: $(date)" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

for csv_file in $DATA_DIR/*.csv; do
    if [ -f "$csv_file" ]; then
        echo "Fichier: $(basename $csv_file)" >> $OUTPUT_FILE
        echo "  Lignes: $(wc -l < $csv_file)" >> $OUTPUT_FILE
        echo "  Colonnes: $(head -1 $csv_file)" >> $OUTPUT_FILE
        echo "" >> $OUTPUT_FILE
    fi
done

echo "Rapport genere dans $OUTPUT_FILE"
cat $OUTPUT_FILE
