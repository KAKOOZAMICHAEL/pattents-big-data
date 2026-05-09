import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text, insert
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent

def local_socket_param(host):
    socket_path = os.getenv("PATENTS_DB_SOCKET", "/run/mysqld/mysqld.sock")
    if host in ("localhost", "127.0.0.1") and Path(socket_path).exists():
        return f"?unix_socket={socket_path}"
    return ""


def get_engine():
    host = os.getenv("PATENTS_DB_HOST", "localhost")
    user = os.getenv("PATENTS_DB_USER", "root")
    password = os.getenv("PATENTS_DB_PASSWORD", "")
    db_name = os.getenv("PATENTS_DB_NAME", "patents_db")
    socket_param = local_socket_param(host)
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db_name}{socket_param}")


def prepare_date_columns(dataframe, columns):
    for column in columns:
        if column in dataframe.columns:
            dataframe[column] = pd.to_datetime(dataframe[column], format="%Y-%m-%d", errors="coerce").dt.date
    return dataframe


def deduplicate_chunk(chunk, primary_key_cols):
    """
    Deduplicate a chunk by keeping only the first occurrence of each primary key.
    
    Args:
        chunk: DataFrame to deduplicate
        primary_key_cols: List of column names that form the primary key
    
    Returns:
        Deduplicated DataFrame, count of duplicates removed
    """
    if isinstance(primary_key_cols, str):
        primary_key_cols = [primary_key_cols]
    
    original_len = len(chunk)
    chunk_dedup = chunk.drop_duplicates(subset=primary_key_cols, keep='first')
    duplicates_removed = original_len - len(chunk_dedup)
    return chunk_dedup, duplicates_removed


def insert_batch_with_fallback(conn, table, chunk, date_cols=None):
    """
    Try bulk insert with to_sql() + method='multi'. If it fails with any error,
    fall back to row-by-row INSERT IGNORE.
    
    Args:
        conn: SQLAlchemy connection object
        table: Target table name
        chunk: DataFrame to insert
        date_cols: List of date columns to convert
    
    Returns:
        Tuple (inserted_count, skipped_count)
    """
    if date_cols:
        chunk = prepare_date_columns(chunk, date_cols)
    
    chunk = chunk.where(pd.notnull(chunk), None)
    
    try:
        # Try bulk insert with small chunksize
        chunk.to_sql(table, con=conn, if_exists='append', index=False, method='multi', chunksize=500)
        return len(chunk), 0
    except Exception as e:
        print(f"  Bulk insert failed for {table}: {str(e)[:80]}... Falling back to row-by-row INSERT IGNORE.")
        inserted = 0
        skipped = 0
        
        # Fall back to row-by-row INSERT IGNORE for maximum resilience
        for idx, row in chunk.iterrows():
            try:
                # Build INSERT IGNORE statement using SQLAlchemy's text() with named parameters
                col_names = ', '.join(f'`{col}`' for col in chunk.columns)
                placeholders = ', '.join(f':{col}' for col in chunk.columns)
                insert_sql = f"INSERT IGNORE INTO `{table}` ({col_names}) VALUES ({placeholders})"
                
                # Convert row to dictionary for named parameter binding
                row_dict = {col: row[col] if pd.notna(row[col]) else None for col in chunk.columns}
                
                conn.execute(text(insert_sql), row_dict)
                inserted += 1
            except Exception as row_error:
                # Silently skip rows that fail (duplicates, constraint violations, etc.)
                skipped += 1
        
        return inserted, skipped


def insert_data_in_batches(conn, table, csv_file, primary_key_cols, batch_size=10000, date_cols=None):
    """
    Load CSV data with deduplication and graceful error handling.
    
    Args:
        conn: SQLAlchemy connection object
        table: Target table name
        csv_file: Path to CSV file
        primary_key_cols: Column(s) for deduplication (string or list)
        batch_size: Rows to read at a time from CSV (default 10000)
        date_cols: List of column names to parse as dates
    
    Returns:
        Tuple (total_inserted, total_skipped)
    """
    if not csv_file.exists():
        print(f"Skipping {table}, {csv_file.name} not found.")
        return 0, 0

    print(f"Loading {table} from {csv_file.name}...")
    total_inserted = 0
    total_skipped = 0
    chunk_count = 0

    for chunk in pd.read_csv(csv_file, chunksize=batch_size, dtype=str):
        chunk_count += 1
        
        # Deduplicate this chunk
        chunk_dedup, removed = deduplicate_chunk(chunk, primary_key_cols)
        total_skipped += removed
        
        if not chunk_dedup.empty:
            inserted, skipped = insert_batch_with_fallback(conn, table, chunk_dedup, date_cols)
            total_inserted += inserted
            total_skipped += skipped
        
        if chunk_count % 2 == 0:
            print(f"  {table} chunk {chunk_count}: inserted {total_inserted} rows, skipped {total_skipped} duplicates so far...")

    print(f"Table {table}: inserted {total_inserted} rows, skipped {total_skipped} duplicates.")
    return total_inserted, total_skipped


def main():
    print("Starting data loading phase...")
    engine = get_engine()
    
    try:
        # Open single connection and disable constraints for ALL inserts at once
        with engine.begin() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            conn.execute(text("SET UNIQUE_CHECKS=0;"))
            
            files_to_load = [
                ("patents", BASE_DIR / "patents.csv", "patent_id", ["filing_date", "publication_date"]),
                ("inventors", BASE_DIR / "inventors.csv", "inventor_id", None),
                ("companies", BASE_DIR / "companies.csv", "company_id", None),
                ("patent_inventors", BASE_DIR / "patent_inventors.csv", ["patent_id", "inventor_id"], None),
                ("patent_companies", BASE_DIR / "patent_companies.csv", ["patent_id", "company_id"], None),
                ("g_abstract", BASE_DIR / "g_abstract.csv", "patent_id", None),
            ]

            for table, csv_path, primary_keys, date_cols in files_to_load:
                inserted, skipped = insert_data_in_batches(conn, table, csv_path, primary_keys, date_cols=date_cols)
                
                # Delete CSV file after successful load
                if (inserted > 0 or skipped > 0) and csv_path.exists():
                    try:
                        os.remove(csv_path)
                        print(f"Deleted {csv_path.name} from disk to free space.")
                    except OSError as remove_error:
                        print(f"Warning: Could not delete {csv_path.name}: {remove_error}")

            # Restore constraint checking before transaction commits
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
            conn.execute(text("SET UNIQUE_CHECKS=1;"))
            
        print("Data loaded successfully into MySQL database.")
    except Exception as error:
        print(f"Error during load: {error}")
        sys.exit(1)

if __name__ == "__main__":
    main()
