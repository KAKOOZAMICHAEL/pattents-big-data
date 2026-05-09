import sqlite3
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

def main():
    print("Starting data extraction...")
    
    # Paths to the TSV files
    patent_file = BASE_DIR / "g_patent" / "g_patent.tsv"
    inventor_file = BASE_DIR / "g_inventor" / "g_inventor_disambiguated.tsv"
    assignee_file = BASE_DIR / "g_assignee" / "g_assignee_disambiguated.tsv"
    abstract_file = BASE_DIR / "g_abstract" / "g_patent_abstract.tsv"
    cpc_file = BASE_DIR / "g_cpc" / "g_cpc_current.tsv"
    
    out_dir = BASE_DIR / "extracted"
    out_dir.mkdir(exist_ok=True)

    valid_db_path = BASE_DIR / "valid_patents_temp.db"
    if valid_db_path.exists():
        valid_db_path.unlink()

    conn = sqlite3.connect(str(valid_db_path))
    conn.execute("CREATE TABLE valid_patents(patent_id TEXT PRIMARY KEY)")
    conn.commit()

    print(f"1. Processing {patent_file.name} using temporary SQLite DB at {valid_db_path.name}...")
    chunk_size = 100000
    patent_cols = ['"patent_id"', '"patent_type"', '"patent_date"', '"patent_title"']
    first_chunk = True
    total_rows = 0
    chunk_count = 0

    for chunk in pd.read_csv(patent_file, sep="\t", chunksize=chunk_size, usecols=patent_cols, dtype=str, on_bad_lines='skip', engine='python', quoting=3, quotechar='"'):
        chunk_count += 1
        total_rows += len(chunk)

        # Filter to 2004-2024
        chunk['"patent_date"'] = chunk['"patent_date"'].str.strip('"')
        chunk['"patent_date"'] = pd.to_datetime(chunk['"patent_date"'], errors='coerce')
        mask = (chunk['"patent_date"'].dt.year >= 2004) & (chunk['"patent_date"'].dt.year <= 2024)
        filtered = chunk[mask].copy()

        valid_ids = filtered['"patent_id"'].str.strip('"').dropna().unique().tolist()
        if valid_ids:
            conn.executemany("INSERT OR IGNORE INTO valid_patents(patent_id) VALUES (?)", [(pid,) for pid in valid_ids])
            conn.commit()

        filtered['"patent_date"'] = filtered['"patent_date"'].dt.strftime('%Y-%m-%d')
        filtered.to_csv(out_dir / "ext_patents.csv", index=False, mode='w' if first_chunk else 'a', header=first_chunk)
        first_chunk = False

        if chunk_count % 10 == 0:
            print(f"  Patent chunk {chunk_count}: processed {total_rows} rows so far, saved {len(valid_ids)} valid IDs from this chunk.")

    valid_count = conn.execute("SELECT COUNT(*) FROM valid_patents").fetchone()[0]
    print(f"Stored {valid_count} valid patents in the temporary SQLite DB.")
    
    def fetch_valid_ids(chunk_ids, conn, batch_size=2000):
        valid_ids = set()
        for start in range(0, len(chunk_ids), batch_size):
            batch = chunk_ids[start:start + batch_size]
            placeholders = ",".join("?" for _ in batch)
            query = f"SELECT patent_id FROM valid_patents WHERE patent_id IN ({placeholders})"
            rows = conn.execute(query, batch)
            valid_ids.update(row[0] for row in rows)
        return valid_ids

    def process_dependent(infile, outfile, usecols=None):
        if not infile.exists():
            print(f"Warning: {infile} not found.")
            return
        print(f"Processing {infile.name}...")
        first = True
        chunk_count = 0
        total_rows = 0

        for chunk in pd.read_csv(infile, sep="\t", chunksize=chunk_size, dtype=str, usecols=usecols, on_bad_lines='skip', engine='python', quoting=3, quotechar='"'):
            chunk_count += 1
            total_rows += len(chunk)

            if '"patent_id"' in chunk.columns:
                chunk['"patent_id"'] = chunk['"patent_id"'].str.strip('"')
                unique_ids = chunk['"patent_id"'].dropna().unique().tolist()
                valid_ids = fetch_valid_ids(unique_ids, conn) if unique_ids else set()
                filtered = chunk[chunk['"patent_id"'].isin(valid_ids)]
            else:
                filtered = chunk

            if not filtered.empty:
                filtered.to_csv(outfile, index=False, mode='w' if first else 'a', header=first)
                first = False
            elif first:
                pd.DataFrame(columns=filtered.columns).to_csv(outfile, index=False, mode='w')
                first = False

            if chunk_count % 10 == 0:
                print(f"  {infile.name} chunk {chunk_count}: processed {total_rows} rows so far.")

    try:
        # Inventors
        process_dependent(inventor_file, out_dir / "ext_inventors.csv", 
                         ['"patent_id"', '"inventor_id"', '"disambig_inventor_name_first"', '"disambig_inventor_name_last"', '"location_id"'])
        
        # Assignees
        process_dependent(assignee_file, out_dir / "ext_assignees.csv",
                         ['"patent_id"', '"assignee_id"', '"disambig_assignee_organization"', '"disambig_assignee_individual_name_first"', '"disambig_assignee_individual_name_last"'])
                         
        # Abstracts
        process_dependent(abstract_file, out_dir / "ext_abstracts.csv", ['"patent_id"', '"patent_abstract"'])
        
        # CPC
        process_dependent(cpc_file, out_dir / "ext_cpc.csv", ['"patent_id"', '"cpc_section"', '"cpc_class"', '"cpc_subclass"'])
    finally:
        conn.close()
        if valid_db_path.exists():
            valid_db_path.unlink()
            print(f"Removed temporary SQLite DB {valid_db_path.name}.")

    print("Extraction complete.")

if __name__ == "__main__":
    main()