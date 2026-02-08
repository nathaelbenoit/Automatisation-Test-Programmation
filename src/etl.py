"""Module ETL : Extract, Transform, Load."""
from pathlib import Path
import pandas as pd

# ============== EXTRACT ==============
def extract(filepath: str) -> pd.DataFrame:
    """Extrait les donnees d'un fichier CSV.
    
    Args:
        filepath: Chemin vers le fichier CSV.
    
    Returns:
        DataFrame contenant les donnees.
    
    Raises:
        FileNotFoundError: Si le fichier n'existe pas.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Fichier non trouve : {filepath}")
    
    df = pd.read_csv(filepath)
    print(f"Extrait {len(df)} lignes de {path.name}")
    return df

# ============== TRANSFORM ==============
def transform_persons(df: pd.DataFrame) -> pd.DataFrame:
    """Transforme et nettoie les donnees des personnes.
    
    Args:
        df: DataFrame brut des personnes.
    
    Returns:
        DataFrame nettoye.
    """
    df = df.copy()
    
    # Supprimer les doublons sur person_id
    df = df.drop_duplicates(subset=['person_id'])
    
    # Convertir les dates
    df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')
    
    # Remplacer NaT par None pour MySQL
    df['birth_date'] = df['birth_date'].where(
        df['birth_date'].notna(), None
    )
    
    # Nettoyer les emails invalides
    df['email'] = df['email'].where(
        df['email'].str.contains('@', na=False), None
    )
    
    print(f"Transforme {len(df)} personnes")
    return df

def transform_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Transforme et nettoie les donnees des transactions.
    
    Args:
        df: DataFrame brut des transactions.
    
    Returns:
        DataFrame nettoye.
    """
    df = df.copy()
    
    # Supprimer les doublons
    df = df.drop_duplicates(subset=['transaction_id'])
    
    # Convertir les dates
    df['transaction_date'] = pd.to_datetime(
        df['transaction_date'], errors='coerce'
    )
    
    # Convertir les montants en float
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    
    # Filtrer les montants invalides (negatifs sauf remboursements)
    # On garde les montants negatifs pour les remboursements
    
    print(f"Transforme {len(df)} transactions")
    return df

# ============== LOAD ==============
def load_persons(df: pd.DataFrame, conn) -> int:
    """Charge les personnes dans la base de donnees.
    
    Args:
        df: DataFrame des personnes.
        conn: Connexion MySQL.
    
    Returns:
        Nombre de lignes inserees.
    """
    cursor = conn.cursor()
    query = """
        INSERT INTO persons
            (person_id, first_name, last_name, email,
             birth_date, city, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            email = VALUES(email),
            birth_date = VALUES(birth_date),
            city = VALUES(city),
            country = VALUES(country)
    """
    
    count = 0
    for _, row in df.iterrows():
        values = (
            int(row['person_id']),
            row['first_name'],
            row['last_name'],
            row['email'] if pd.notna(row['email']) else None,
            row['birth_date'].strftime('%Y-%m-%d')
                if pd.notna(row['birth_date']) else None,
            row['city'],
            row['country']
        )
        cursor.execute(query, values)
        count += 1
    
    print(f"Charge {count} personnes")
    return count

def load_transactions(df: pd.DataFrame, conn) -> int:
    """Charge les transactions dans la base de donnees.
    
    Args:
        df: DataFrame des transactions.
        conn: Connexion MySQL.
    
    Returns:
        Nombre de lignes inserees.
    """
    cursor = conn.cursor()
    query = """
        INSERT INTO transactions
            (transaction_id, person_id, amount, currency,
             transaction_date, category, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            amount = VALUES(amount),
            status = VALUES(status)
    """
    
    count = 0
    for _, row in df.iterrows():
        values = (
            row['transaction_id'],
            int(row['person_id']),
            float(row['amount']) if pd.notna(row['amount']) else 0,
            row['currency'],
            row['transaction_date'].strftime('%Y-%m-%d %H:%M:%S')
                if pd.notna(row['transaction_date']) else None,
            row['category'],
            row['status']
        )
        cursor.execute(query, values)
        count += 1
    
    print(f"Charge {count} transactions")
    return count
