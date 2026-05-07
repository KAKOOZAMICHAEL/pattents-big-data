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

def insert_data_in_batches(conn, table, csv_file, batch_size=50000, date_cols=None):
    """
    Bulk insert data using pandas.to_sql() with method='multi' for optimal performance.
    
    Args:
        conn: SQLAlchemy connection object (shared across all inserts)
        table: Target table name
        csv_file: Path to CSV file
        batch_size: Rows per chunk (default 50000 for optimal performance)
        date_cols: List of column names to parse as dates
    """
    if not csv_file.exists():
        print(f"Skipping {table}, {csv_file.name} not found.")
        return

    print(f"Loading {table} using pandas.to_sql with {batch_size} row chunks...")
    rows_inserted = 0
    
    for i, chunk in enumerate(pd.read_csv(csv_file, chunksize=batch_size, dtype=str)):
        if date_cols:
            chunk = prepare_date_columns(chunk, date_cols)
        
        chunk = chunk.where(pd.notnull(chunk), None)
        
        # Use pandas to_sql with method='multi' for 10-20x faster bulk inserts
        chunk.to_sql(table, con=conn, if_exists='append', index=False, method='multi')
        rows_inserted += len(chunk)
        print(f"  Inserted {rows_inserted} rows into {table}...")

def main():
    print("Starting data loading phase...")
    engine = get_engine()
    
    try:
        # Open single connection and disable constraints for ALL inserts at once
        with engine.begin() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            conn.execute(text("SET UNIQUE_CHECKS=0;"))
            
            # All inserts use the same connection with constraints disabled
            insert_data_in_batches(conn, "patents", BASE_DIR / "patents.csv", date_cols=["filing_date", "publication_date"])
            insert_data_in_batches(conn, "inventors", BASE_DIR / "inventors.csv")
            insert_data_in_batches(conn, "companies", BASE_DIR / "companies.csv")
            insert_data_in_batches(conn, "patent_inventors", BASE_DIR / "patent_inventors.csv")
            insert_data_in_batches(conn, "patent_companies", BASE_DIR / "patent_companies.csv")
            insert_data_in_batches(conn, "g_abstract", BASE_DIR / "g_abstract.csv")

            # Restore constraint checking before transaction commits
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
            conn.execute(text("SET UNIQUE_CHECKS=1;"))
            
        print("Data loaded successfully into MySQL database.")
    except Exception as error:
        print(f"Error during load: {error}")

if __name__ == "__main__":
    main()