import sqlite3

# 1. Connexion à la base de données (crée un fichier test.db)
conn = sqlite3.connect('test.db')
cursor = conn.cursor()

# 2. Création d'une table
cursor.execute('''CREATE TABLE IF NOT EXISTS test_table
                  (id INTEGER PRIMARY KEY, value TEXT)''')

# 3. Insertion de données
cursor.execute("INSERT INTO test_table (value) VALUES ('Hello, SQLite!')")
conn.commit()  # Important pour sauvegarder !

# 4. Lecture des données
cursor.execute("SELECT * FROM test_table")
print(cursor.fetchall())  # Doit afficher [(1, 'Hello, SQLite!')]

# 5. Fermeture de la connexion
conn.close()
