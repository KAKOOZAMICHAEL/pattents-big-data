"""
generate_country_trends.py
--------------------------
Generates country_trends.csv directly from inventors.csv and
patent_inventors.csv so it works without a MySQL connection.

Run:
    python generate_country_trends.py
Output:
    country_trends.csv
"""

import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def main():
    inventors_path       = BASE_DIR / "inventors.csv"
    patent_inventors_path = BASE_DIR / "patent_inventors.csv"

    if not inventors_path.exists():
        print(f"ERROR: {inventors_path} not found. Make sure inventors.csv is in the same folder.")
        return
    if not patent_inventors_path.exists():
        print(f"ERROR: {patent_inventors_path} not found. Make sure patent_inventors.csv is in the same folder.")
        return

    inventors       = pd.read_csv(inventors_path)
    patent_inventors = pd.read_csv(patent_inventors_path)

    # Merge to link each patent to the inventor's country
    merged = patent_inventors.merge(inventors[["inventor_id", "country"]], on="inventor_id", how="left")

    # Count distinct patents per country
    country_trends = (
        merged.groupby("country")["patent_id"]
        .nunique()
        .reset_index()
        .rename(columns={"patent_id": "patent_count"})
        .sort_values("patent_count", ascending=False)
    )

    # Add percentage share column
    total = country_trends["patent_count"].sum()
    country_trends["share_pct"] = (country_trends["patent_count"] / total * 100).round(2)

    output_path = BASE_DIR / "country_trends.csv"
    country_trends.to_csv(output_path, index=False)
    print(f"Saved {len(country_trends)} rows to {output_path}")
    print(country_trends.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
