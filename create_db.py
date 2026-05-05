import os
from sqlalchemy import create_engine, text
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

def main():
    host = os.getenv("PATENTS_DB_HOST", "localhost")
    user = os.getenv("PATENTS_DB_USER", "root")
    password = os.getenv("PATENTS_DB_PASSWORD", "")
    db_name = os.getenv("PATENTS_DB_NAME", "patents_db")
    
    print(f"Connecting to MySQL server at {host}...")
    try:
        # Connect without DB to create it
        engine_no_db = create_engine(f"mysql+pymysql://{user}:{password}@{host}/")
        with engine_no_db.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
        
        print(f"Database '{db_name}' ready.")
        
        # Connect to the DB to run schema.sql
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db_name}")
        
        schema_path = BASE_DIR / "schema.sql"
        if schema_path.exists():
            with open(schema_path, "r", encoding="utf-8") as f:
                # Basic split by semicolon to run statements iteratively
                sql_statements = f.read().split(";")
                
            with engine.begin() as conn:
                for statement in sql_statements:
                    if statement.strip():
                        conn.execute(text(statement.strip()))
            print("Schema loaded successfully via SQLAlchemy.")
        else:
            print("schema.sql not found.")
    except Exception as e:
        print(f"Failed to create database or tables: {e}")

if __name__ == "__main__":
    main()