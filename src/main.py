"""Point d'entree du pipeline ETL."""
from src.config import Config
from src.database import database_connection
from src.etl import (
    extract,
    transform_persons,
    transform_transactions,
    load_persons,
    load_transactions
)

def run_pipeline():
    """Execute le pipeline ETL complet."""
    print("=" * 50)
    print("Demarrage du pipeline ETL")
    print("=" * 50)
    
    with database_connection() as conn:
        # ETL Persons
        print("\n--- Traitement des Personnes ---")
        df_persons = extract(f"{Config.DATA_DIR}/Persons.csv")
        df_persons = transform_persons(df_persons)
        load_persons(df_persons, conn)
        
        # ETL Transactions
        print("\n--- Traitement des Transactions ---")
        df_trans = extract(f"{Config.DATA_DIR}/Transactions.csv")
        df_trans = transform_transactions(df_trans)
        load_transactions(df_trans, conn)
    
    print("\n" + "=" * 50)
    print("Pipeline termine avec succes !")
    print("=" * 50)

if __name__ == '__main__':
    run_pipeline()
