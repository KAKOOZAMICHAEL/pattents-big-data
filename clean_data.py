import pandas as pd
from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parent

def clean_company_name(name):
    if pd.isna(name) or name == "":
        return "Unknown Company"
    name = str(name).strip()
    name = re.sub(r'(?i)\b(ltd\.?|inc\.?|corp\.?|corporation|llc\.?|limited)\b', '', name)
    name = " ".join(name.split())
    return name if name else "Unknown Company"

def standardize_country(code):
    if pd.isna(code) or code == "":
        return "Unknown"
    code = str(code).strip().upper()
    if code in ["US", "U.S.", "USA", "UNITED STATES"]:
        return "US"
    return code

def main():
    print("Starting data cleaning...")
    in_dir = BASE_DIR / "extracted"
    out_dir = BASE_DIR
    
    chunk_size = 100000

    # 1. Patents & CPC
    print("Extracting CPC for patents...")
    cpc_map = {}
    if (in_dir / "ext_cpc.csv").exists():
        for chunk in pd.read_csv(in_dir / "ext_cpc.csv", chunksize=chunk_size, dtype=str):
            chunk = chunk.dropna(subset=['patent_id', 'cpc_section'])
            chunk = chunk.drop_duplicates(subset=['patent_id'])
            for _, row in chunk.iterrows():
                if row['patent_id'] not in cpc_map:
                    cpc_map[row['patent_id']] = row['cpc_section']

    print("Cleaning patents...")
    first = True
    if (in_dir / "ext_patents.csv").exists():
        for chunk in pd.read_csv(in_dir / "ext_patents.csv", chunksize=chunk_size, dtype=str):
            df = pd.DataFrame()
            df['patent_id'] = chunk['patent_id']
            df['title'] = chunk['patent_title'].fillna("Unknown Title").str.strip()
            df['description'] = "" 
            df['filing_date'] = pd.to_datetime(chunk['patent_date'], errors='coerce').dt.strftime('%Y-%m-%d')
            df['publication_date'] = df['filing_date']
            df['main_classification'] = ""
            df['locarno_classification'] = ""
            df['cpc_section'] = df['patent_id'].map(cpc_map).fillna('')
            
            df = df.drop_duplicates(subset=['patent_id'])
            df.to_csv(out_dir / "patents.csv", index=False, mode='w' if first else 'a', header=first)
            first = False

    # 2. Inventors & Patent-Inventors
    print("Cleaning inventors...")
    inventors_seen = set()
    first_inv = True
    first_pi = True
    if (in_dir / "ext_inventors.csv").exists():
        for chunk in pd.read_csv(in_dir / "ext_inventors.csv", chunksize=chunk_size, dtype=str):
            chunk['first'] = chunk['disambig_inventor_name_first'].fillna("")
            chunk['last'] = chunk['disambig_inventor_name_last'].fillna("")
            chunk['full_name'] = (chunk['first'] + " " + chunk['last']).str.strip()
            chunk.loc[chunk['full_name'] == "", 'full_name'] = "Unknown Inventor"
            
            chunk['inventor_id'] = chunk['inventor_id'].fillna("unknown_inv")
            chunk['country'] = chunk['location_id'].apply(standardize_country)
            
            inv = chunk[['inventor_id', 'full_name', 'country']].drop_duplicates(subset=['inventor_id'])
            inv_new = inv[~inv['inventor_id'].isin(inventors_seen)]
            if not inv_new.empty:
                inventors_seen.update(inv_new['inventor_id'])
                inv_new.to_csv(out_dir / "inventors.csv", index=False, mode='w' if first_inv else 'a', header=first_inv)
                first_inv = False
                
            pi = chunk[['patent_id', 'inventor_id']].dropna().drop_duplicates()
            pi.to_csv(out_dir / "patent_inventors.csv", index=False, mode='w' if first_pi else 'a', header=first_pi)
            first_pi = False

    # 3. Companies & Patent-Companies
    print("Cleaning companies...")
    companies_seen = set()
    first_comp = True
    first_pc = True
    if (in_dir / "ext_assignees.csv").exists():
        for chunk in pd.read_csv(in_dir / "ext_assignees.csv", chunksize=chunk_size, dtype=str):
            chunk['company_name'] = chunk['disambig_assignee_organization'].fillna("")
            mask = chunk['company_name'] == ""
            chunk.loc[mask, 'company_name'] = (chunk.loc[mask, 'disambig_assignee_individual_name_first'].fillna("") + " " + chunk.loc[mask, 'disambig_assignee_individual_name_last'].fillna("")).str.strip()
            
            chunk['company_name'] = chunk['company_name'].apply(clean_company_name)
            chunk['company_id'] = chunk['assignee_id'].fillna("unknown_comp")
            
            comp = chunk[['company_id', 'company_name']].drop_duplicates(subset=['company_id'])
            comp_new = comp[~comp['company_id'].isin(companies_seen)]
            if not comp_new.empty:
                companies_seen.update(comp_new['company_id'])
                comp_new.to_csv(out_dir / "companies.csv", index=False, mode='w' if first_comp else 'a', header=first_comp)
                first_comp = False
                
            pc = chunk[['patent_id', 'company_id']].dropna().drop_duplicates()
            pc.to_csv(out_dir / "patent_companies.csv", index=False, mode='w' if first_pc else 'a', header=first_pc)
            first_pc = False

    # 4. Abstracts
    print("Cleaning abstracts...")
    first_abs = True
    if (in_dir / "ext_abstracts.csv").exists():
        for chunk in pd.read_csv(in_dir / "ext_abstracts.csv", chunksize=chunk_size, dtype=str):
            chunk['abstract_text'] = chunk['patent_abstract'].fillna("").str.strip()
            ab = chunk[['patent_id', 'abstract_text']].dropna().drop_duplicates(subset=['patent_id'])
            ab.to_csv(out_dir / "g_abstract.csv", index=False, mode='w' if first_abs else 'a', header=first_abs)
            first_abs = False

    print("Cleaning complete.")

if __name__ == "__main__":
    main()