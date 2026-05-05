import os
from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent

def get_engine():
    host = os.getenv("PATENTS_DB_HOST", "localhost")
    user = os.getenv("PATENTS_DB_USER", "root")
    password = os.getenv("PATENTS_DB_PASSWORD", "")
    db_name = os.getenv("PATENTS_DB_NAME", "patents_db")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db_name}")

def prepare_date_columns(dataframe, columns):
    for column in columns:
        if column in dataframe.columns:
            dataframe[column] = pd.to_datetime(dataframe[column], format="%Y-%m-%d", errors="coerce").dt.date
    return dataframe

def insert_data_in_batches(engine, table, csv_file, columns, batch_size=10000, date_cols=None):
    if not csv_file.exists():
        print(f"Skipping {table}, {csv_file.name} not found.")
        return

    placeholders = ", ".join([f":{col}" for col in columns])
    sql = text(f"INSERT IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})")
    
    print(f"Loading {table} in batches of {batch_size} via SQLAlchemy...")
    with engine.begin() as conn:
        for i, chunk in enumerate(pd.read_csv(csv_file, chunksize=batch_size, dtype=str)):
            if date_cols:
                chunk = prepare_date_columns(chunk, date_cols)
            
            chunk = chunk.where(pd.notnull(chunk), None)
            
            # Using SQLAlchemy execute with a list of dictionaries parameters
            data_to_insert = chunk.to_dict(orient="records")
            conn.execute(sql, data_to_insert)
            if i % 5 == 0:
                print(f"  Inserted {(i+1)*batch_size} rows into {table}...")

def main():
    print("Starting data loading phase...")
    engine = get_engine()
    
    try:
        with engine.begin() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            conn.execute(text("SET UNIQUE_CHECKS=0;"))
            
        insert_data_in_batches(engine, "patents", BASE_DIR / "patents.csv", ["patent_id", "title", "description", "filing_date", "publication_date", "main_classification", "locarno_classification", "cpc_section"], date_cols=["filing_date", "publication_date"])
        insert_data_in_batches(engine, "inventors", BASE_DIR / "inventors.csv", ["inventor_id", "full_name", "country"])
        insert_data_in_batches(engine, "companies", BASE_DIR / "companies.csv", ["company_id", "company_name"])
        insert_data_in_batches(engine, "patent_inventors", BASE_DIR / "patent_inventors.csv", ["patent_id", "inventor_id"])
        insert_data_in_batches(engine, "patent_companies", BASE_DIR / "patent_companies.csv", ["patent_id", "company_id"])
        insert_data_in_batches(engine, "g_abstract", BASE_DIR / "g_abstract.csv", ["patent_id", "abstract_text"])

        with engine.begin() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
            conn.execute(text("SET UNIQUE_CHECKS=1;"))
            
        print("Data loaded successfully into MySQL database.")
    except Exception as error:
        print(f"Error during load: {error}")

if __name__ == "__main__":
    main()