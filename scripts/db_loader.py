import sqlite3
import csv
from pathlib import Path

def load_to_db(csv_path: Path, db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS patents (
            doc_number TEXT PRIMARY KEY,
            title TEXT,
            country TEXT,
            date_publ TEXT,
            applicants TEXT,
            inventors TEXT,
            classifications TEXT
        )
    """)
    
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute("""
                INSERT OR REPLACE INTO patents 
                (doc_number, title, country, date_publ, applicants, inventors, classifications)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (row["doc_number"], row["title"], row["country"], row["date_publ"],
                  row["applicants"], row["inventors"], row["classifications"]))
    
    conn.commit()
    conn.close()
    print(f"Loaded {csv_path} into {db_path}")