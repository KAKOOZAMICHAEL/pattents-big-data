import os
from pathlib import Path

import mysql.connector
import pandas as pd
from mysql.connector import Error


BASE_DIR = Path(__file__).resolve().parent


def run_query_and_export(cursor, query, description, filename_base, output_dir):
    """Run a query and export its results to CSV and JSON."""
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    if not results:
        print(f"No results for {description}, skipping export.")
        return

    df = pd.DataFrame(results, columns=columns)
    df = df.map(lambda value: value.strip() if isinstance(value, str) else value)
    df.fillna("N/A", inplace=True)

    csv_file = output_dir / f"{filename_base}.csv"
    json_file = output_dir / f"{filename_base}.json"
    df.to_csv(csv_file, index=False)
    df.to_json(json_file, orient="records", indent=4)
    print(f"Exported {description} to {csv_file} and {json_file}")


def main():
    """Export analytics from the MySQL patent database."""
    host = os.getenv("PATENTS_DB_HOST", "localhost")
    user = os.getenv("PATENTS_DB_USER", "root")
    password = os.getenv("PATENTS_DB_PASSWORD", "")
    db_name = os.getenv("PATENTS_DB_NAME", "patents_db")
    output_dir = BASE_DIR / "outputs"
    output_dir.mkdir(exist_ok=True)

    queries = [
        ("SELECT COUNT(*) AS total_patents FROM patents", "Total Patents", "total_patents"),
        ("SELECT COUNT(*) AS total_inventors FROM inventors", "Total Inventors", "total_inventors"),
        ("SELECT COUNT(*) AS total_companies FROM companies", "Total Companies", "total_companies"),
        (
            "SELECT country, COUNT(*) AS inventor_count FROM inventors GROUP BY country ORDER BY inventor_count DESC",
            "Inventors per Country",
            "inventors_per_country",
        ),
        (
            """
            SELECT c.company_name, COUNT(DISTINCT pc.patent_id) AS patent_count
            FROM companies c
            LEFT JOIN patent_companies pc ON c.company_id = pc.company_id
            GROUP BY c.company_id, c.company_name
            ORDER BY patent_count DESC, c.company_name
            LIMIT 10
            """,
            "Top 10 Companies",
            "top_companies",
        ),
        (
            """
            SELECT YEAR(filing_date) AS year, COUNT(*) AS patent_count
            FROM patents
            WHERE filing_date IS NOT NULL
            GROUP BY YEAR(filing_date)
            ORDER BY year
            """,
            "Patents per Year",
            "patents_per_year",
        ),
        (
            """
            SELECT main_classification, COUNT(*) AS patent_count
            FROM patents
            WHERE main_classification IS NOT NULL
            GROUP BY main_classification
            ORDER BY patent_count DESC, main_classification
            LIMIT 10
            """,
            "Top Classifications",
            "top_classifications",
        ),
        (
            """
            SELECT ROUND(AVG(inventor_count), 2) AS avg_inventors_per_patent
            FROM (
                SELECT patent_id, COUNT(*) AS inventor_count
                FROM patent_inventors
                GROUP BY patent_id
            ) AS inventor_counts
            """,
            "Average Inventors per Patent",
            "avg_inventors_per_patent",
        ),
    ]

    try:
        connection = mysql.connector.connect(host=host, user=user, password=password, database=db_name)
        cursor = connection.cursor()

        for query, description, filename in queries:
            run_query_and_export(cursor, query, description, filename, output_dir)

        print("\nAll exports completed. Files are ready for downstream analysis.")
    except Error as error:
        print(f"Error connecting to database: {error}")
    finally:
        if "connection" in locals() and connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == "__main__":
    main()