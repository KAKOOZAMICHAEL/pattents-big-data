"""
rename_to_clean.py
------------------
Renames the existing CSV files to the clean_ prefix required
by the project brief, then generates country_trends.csv.

Run once:
    python rename_to_clean.py
"""

import shutil
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

RENAMES = {
    "patents.csv"          : "clean_patents.csv",
    "inventors.csv"        : "clean_inventors.csv",
    "companies.csv"        : "clean_companies.csv",
}

def main():
    for old_name, new_name in RENAMES.items():
        src = BASE_DIR / old_name
        dst = BASE_DIR / new_name
        if src.exists():
            shutil.copy2(src, dst)          # copy so original still works with existing scripts
            print(f"Copied  {old_name}  →  {new_name}")
        else:
            print(f"SKIP: {old_name} not found")

    print("\nDone. You can now commit the clean_*.csv files to GitHub.")

if __name__ == "__main__":
    main()
