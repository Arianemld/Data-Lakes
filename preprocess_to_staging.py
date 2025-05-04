import boto3
import pandas as pd
import sqlite3
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


#Download the data from the raw bucket.
def download_from_s3(bucket, key, local_path):
    try:
        s3 = boto3.client('s3',
            endpoint_url='http://localhost:4566',
            aws_access_key_id='test',
            aws_secret_access_key='test'
        )
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(bucket, key, local_path)
        print(f"Fichier {key} téléchargé vers {local_path}")
        return True
    except Exception as e:
        print(f"Échec du téléchargement S3 : {str(e)}")
        return False

#Clean the data
def clean_data(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        unique_lines = list(dict.fromkeys(lines))
        return pd.DataFrame(unique_lines, columns=['text'])
    except Exception as e:
        print(f"Échec du nettoyage : {str(e)}")
        raise

#Connect to the remote SQLite database.
def create_db_connection():
    try:
        db_path = Path("data/staging/database.db")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        print("Connexion SQLite établie")
        return conn
    except Exception as e:
        print(f"Échec connexion SQLite : {str(e)}")
        raise

def init_db(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS texts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    except Exception as e:
        print(f"Échec initialisation DB : {str(e)}")
        raise

#Insert the cleaned data into this table.
def insert_data(conn, df):
    try:
        df = df.rename(columns={'text': 'content'})
        
        df.to_sql('texts', 
                 conn, 
                 if_exists='append', 
                 index=False,
                 dtype={'content': 'TEXT'})
        print(f"{len(df)} lignes insérées")
    except Exception as e:
        print(f"Échec insertion : {str(e)}")
        raise

#Verify that the data is correctly inserted.
def validate_data(conn):
    try:
        count = pd.read_sql("SELECT COUNT(*) FROM texts", conn).iloc[0,0]
        sample = pd.read_sql("SELECT content FROM texts LIMIT 1", conn).iloc[0,0]
        print(f"Validation : {count} lignes")
        print(f"Exemple : {sample[:50]}...")
        return count
    except Exception as e:
        print(f"Échec validation : {str(e)}")
        raise

def main():
    """Flux principal"""
    conn = None
    try:
        if not download_from_s3('raw', 'wikitext/train.txt', 'data/staging/train.txt'):
            return

        df = clean_data('data/staging/train.txt')

        conn = create_db_connection()
        init_db(conn)
        insert_data(conn, df)
        validate_data(conn)
        
    except Exception as e:
        print(f"Échec du pipeline : {str(e)}")
    finally:
        if conn:
            conn.close()
            print("Connexion fermée")

if __name__ == "__main__":
    main()