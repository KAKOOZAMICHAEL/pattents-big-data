import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

def main():
    print("Starting data extraction...")
    
    # Paths to the TSV files
    patent_file = BASE_DIR / "g_patent.tsv (1)" / "g_patent.tsv"
    inventor_file = BASE_DIR / "g_inventor_disambiguated.tsv (1)" / "g_inventor_disambiguated.tsv"
    assignee_file = BASE_DIR / "g_assignee_disambiguated.tsv (2)" / "g_assignee_disambiguated.tsv"
    abstract_file = BASE_DIR / "g_patent_abstract.tsv" / "g_patent_abstract.tsv"
    cpc_file = BASE_DIR / "g_cpc_current.tsv" / "g_cpc_current.tsv"
    
    out_dir = BASE_DIR / "extracted"
    out_dir.mkdir(exist_ok=True)
    
    valid_patents = set()
    
    print(f"1. Processing {patent_file.name}...")
    chunk_size = 100000
    
    patent_cols = ["patent_id", "patent_type", "patent_date", "patent_title"]
    
    first_chunk = True
    for chunk in pd.read_csv(patent_file, sep="\t", chunksize=chunk_size, usecols=patent_cols, dtype=str, on_bad_lines='skip'):
        # Filter to 2004-2024
        chunk['patent_date'] = pd.to_datetime(chunk['patent_date'], errors='coerce')
        mask = (chunk['patent_date'].dt.year >= 2004) & (chunk['patent_date'].dt.year <= 2024)
        
        filtered = chunk[mask].copy()
        
        # Store valid IDs
        valid_patents.update(filtered['patent_id'].dropna().tolist())
        
        # Save to intermediate CSV
        filtered['patent_date'] = filtered['patent_date'].dt.strftime('%Y-%m-%d')
        filtered.to_csv(out_dir / "ext_patents.csv", index=False, mode='w' if first_chunk else 'a', header=first_chunk)
        first_chunk = False
        
    print(f"Found {len(valid_patents)} valid patents.")
    
    def process_dependent(infile, outfile, usecols=None):
        if not infile.exists():
            print(f"Warning: {infile} not found.")
            return
        print(f"Processing {infile.name}...")
        first = True
        for chunk in pd.read_csv(infile, sep="\t", chunksize=chunk_size, dtype=str, usecols=usecols, on_bad_lines='skip'):
            if 'patent_id' in chunk.columns:
                filtered = chunk[chunk['patent_id'].isin(valid_patents)]
            else:
                filtered = chunk
                
            if not filtered.empty:
                filtered.to_csv(outfile, index=False, mode='w' if first else 'a', header=first)
                first = False
            elif first:
                pd.DataFrame(columns=filtered.columns).to_csv(outfile, index=False, mode='w')
                first = False

    # Inventors
    process_dependent(inventor_file, out_dir / "ext_inventors.csv", 
                     ["patent_id", "inventor_id", "disambig_inventor_name_first", "disambig_inventor_name_last", "location_id"])
    
    # Assignees
    process_dependent(assignee_file, out_dir / "ext_assignees.csv",
                     ["patent_id", "assignee_id", "disambig_assignee_organization", "disambig_assignee_individual_name_first", "disambig_assignee_individual_name_last"])
                     
    # Abstracts
    process_dependent(abstract_file, out_dir / "ext_abstracts.csv", ["patent_id", "patent_abstract"])
    
    # CPC
    process_dependent(cpc_file, out_dir / "ext_cpc.csv", ["patent_id", "cpc_section", "cpc_class", "cpc_subclass"])
    
    print("Extraction complete.")

if __name__ == "__main__":
    main()