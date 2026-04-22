import json
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent


def load_data(json_file):
    """Load the extracted JSON data."""
    with Path(json_file).open("r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def normalize_country(country):
    """Normalize country values for consistent analytics."""
    if pd.isna(country) or country in {"N/A", "", None}:
        return "Unknown"

    normalized = " ".join(str(country).split()).upper()
    aliases = {
        "US": "USA",
        "U.S.": "USA",
        "UNITED STATES": "USA",
        "UNITED STATES OF AMERICA": "USA",
        "UNKNOWN": "Unknown",
    }
    return aliases.get(normalized, normalized)


def clean_text(text, default="N/A"):
    """Collapse whitespace without changing the original casing."""
    if pd.isna(text) or text in {"", None}:
        return default

    normalized = " ".join(str(text).split())
    return normalized if normalized else default


def normalize_patents(data):
    """Normalize extracted patent data into relational tables."""
    patents_list = []
    all_inventors = []
    all_companies = []
    patent_inventors = []
    patent_companies = []

    for patent in data:
        patent_id = clean_text(patent.get("patent_id"))
        patents_list.append(
            {
                "patent_id": patent_id,
                "title": clean_text(patent.get("title")),
                "description": clean_text(patent.get("description")),
                "filing_date": clean_text(patent.get("filing_date")),
                "publication_date": clean_text(patent.get("publication_date")),
                "main_classification": clean_text(patent.get("main_classification")),
                "locarno_classification": clean_text(patent.get("locarno_classification")),
            }
        )

        for inventor in patent.get("inventors", []):
            inventor_row = {
                "full_name": clean_text(inventor.get("full_name"), default="Unknown Inventor"),
                "country": normalize_country(inventor.get("country")),
            }
            all_inventors.append(inventor_row)
            patent_inventors.append({"patent_id": patent_id, **inventor_row})

        for company in patent.get("assignees", []):
            company_row = {
                "company_name": clean_text(company.get("company_name"), default="Unknown Company"),
            }
            all_companies.append(company_row)
            patent_companies.append({"patent_id": patent_id, **company_row})

    patents_df = pd.DataFrame(patents_list).drop_duplicates(subset=["patent_id"])
    patents_df = patents_df.sort_values("patent_id").reset_index(drop=True)

    inventors_df = pd.DataFrame(all_inventors).drop_duplicates() if all_inventors else pd.DataFrame(columns=["full_name", "country"])
    inventors_df = inventors_df.sort_values(["full_name", "country"]).reset_index(drop=True)
    inventors_df["inventor_id"] = range(1, len(inventors_df) + 1)

    companies_df = pd.DataFrame(all_companies).drop_duplicates() if all_companies else pd.DataFrame(columns=["company_name"])
    companies_df = companies_df.sort_values(["company_name"]).reset_index(drop=True)
    companies_df["company_id"] = range(1, len(companies_df) + 1)

    if patent_inventors:
        patent_inventors_df = pd.DataFrame(patent_inventors).drop_duplicates()
        patent_inventors_df = patent_inventors_df.merge(
            inventors_df[["full_name", "country", "inventor_id"]],
            on=["full_name", "country"],
            how="left",
        )
        patent_inventors_df = patent_inventors_df[["patent_id", "inventor_id"]].dropna().drop_duplicates()
        patent_inventors_df["inventor_id"] = patent_inventors_df["inventor_id"].astype(int)
        patent_inventors_df = patent_inventors_df.sort_values(["patent_id", "inventor_id"]).reset_index(drop=True)
    else:
        patent_inventors_df = pd.DataFrame(columns=["patent_id", "inventor_id"])

    if patent_companies:
        patent_companies_df = pd.DataFrame(patent_companies).drop_duplicates()
        patent_companies_df = patent_companies_df.merge(
            companies_df[["company_name", "company_id"]],
            on="company_name",
            how="left",
        )
        patent_companies_df = patent_companies_df[["patent_id", "company_id"]].dropna().drop_duplicates()
        patent_companies_df["company_id"] = patent_companies_df["company_id"].astype(int)
        patent_companies_df = patent_companies_df.sort_values(["patent_id", "company_id"]).reset_index(drop=True)
    else:
        patent_companies_df = pd.DataFrame(columns=["patent_id", "company_id"])

    return patents_df, inventors_df, companies_df, patent_inventors_df, patent_companies_df


def main(json_path=None, output_dir=None):
    """Clean extracted patent data and emit normalized CSV tables."""
    json_path = Path(json_path) if json_path else BASE_DIR / "patents_data.json"
    output_dir = Path(output_dir) if output_dir else BASE_DIR

    data = load_data(json_path)
    patents_df, inventors_df, companies_df, patent_inventors_df, patent_companies_df = normalize_patents(data)

    patents_df.to_csv(output_dir / "patents.csv", index=False)
    inventors_df.to_csv(output_dir / "inventors.csv", index=False)
    companies_df.to_csv(output_dir / "companies.csv", index=False)
    patent_inventors_df.to_csv(output_dir / "patent_inventors.csv", index=False)
    patent_companies_df.to_csv(output_dir / "patent_companies.csv", index=False)

    print("Clean datasets saved:")
    print("- patents.csv")
    print("- inventors.csv")
    print("- companies.csv")
    print("- patent_inventors.csv")
    print("- patent_companies.csv")


if __name__ == "__main__":
    main()