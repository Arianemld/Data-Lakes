import sqlite3
import pandas as pd
from pymongo import MongoClient
from transformers import AutoTokenizer
from datetime import datetime

# 1. Connexion à SQLite
def get_clean_data():
    """Récupère les données nettoyées depuis SQLite"""
    conn = sqlite3.connect('data/staging/database.db')
    query = "SELECT id, content FROM texts"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# 2. Tokenisation avec Hugging Face
def tokenize_text(text):
    """Tokenise le texte avec un modèle NLP"""
    tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased")
    tokens = tokenizer(text, truncation=True, max_length=512, return_tensors="np")
    return tokens["input_ids"][0].tolist()  # Convertit en liste Python

# 3. Insertion dans MongoDB
def save_to_mongodb(data):
    """Enregistre les données tokenisées dans MongoDB"""
    client = MongoClient("mongodb://localhost:27017/")
    db = client["curated"]
    collection = db["wikitext"]
    
    # Nettoyage préalable (optionnel)
    collection.delete_many({})
    
    # Insertion par batch
    collection.insert_many(data)
    print(f"✅ {len(data)} documents insérés dans MongoDB")

# Pipeline principal
def main():
    # 1. Récupération des données
    df = get_clean_data()
    print(f"📊 {len(df)} textes à traiter")

    # 2. Tokenisation
    df["tokens"] = df["content"].apply(tokenize_text)
    
    # 3. Formatage pour MongoDB
    documents = []
    for _, row in df.iterrows():
        doc = {
            "sqlite_id": row["id"],
            "original_text": row["content"],
            "tokens": row["tokens"],
            "metadata": {
                "source": "SQLite",
                "processing_date": datetime.utcnow().isoformat(),
                "model": "distilbert-base-uncased"
            }
        }
        documents.append(doc)
    
    # 4. Sauvegarde
    save_to_mongodb(documents)

if __name__ == "__main__":
    main()
