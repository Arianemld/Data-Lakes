# Fichier: get_wikitext.py
from datasets import load_dataset
import os

# Créez le dossier si nécessaire
os.makedirs("data/raw", exist_ok=True)

# Téléchargez le dataset
dataset = load_dataset("wikitext", "wikitext-2-v1")

# Sauvegardez les fichiers
for split in ["train", "test", "validation"]:
    with open(f"data/raw/{split}.txt", "w", encoding="utf-8") as f:
        for item in dataset[split]["text"]:
            f.write(item + "\n")