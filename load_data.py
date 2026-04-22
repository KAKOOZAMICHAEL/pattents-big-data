import os
from pathlib import Path

import mysql.connector
import pandas as pd
from mysql.connector import Error


BASE_DIR = Path(__file__).resolve().parent


def load_csv_to_df(file_path):
    """Load a CSV file into a DataFrame."""
    return pd.read_csv(file_path)


def prepare_date_columns(dataframe, columns):
    """Convert YYYYMMDD strings into date objects accepted by MySQL."""
    for column in columns:
        if column in dataframe.columns:
            dataframe[column] = pd.to_datetime(dataframe[column], format="%Y%m%d", errors="coerce").dt.date
    return dataframe


def insert_data(cursor, table, data_df, columns):
    """Insert data into a table with idempotent behavior."""
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

    for _, row in data_df.iterrows():
        values = tuple(None if pd.isna(row[col]) else row[col] for col in columns)
        cursor.execute(sql, values)


def main():
    """Load normalized CSV outputs into MySQL."""
    host = os.getenv("PATENTS_DB_HOST", "localhost")
    user = os.getenv("PATENTS_DB_USER", "root")
    password = os.getenv("PATENTS_DB_PASSWORD", "")
    db_name = os.getenv("PATENTS_DB_NAME", "patents_db")

    patents_df = prepare_date_columns(load_csv_to_df(BASE_DIR / "patents.csv"), ["filing_date", "publication_date"])
    inventors_df = load_csv_to_df(BASE_DIR / "inventors.csv")
    companies_df = load_csv_to_df(BASE_DIR / "companies.csv")
    patent_inventors_df = load_csv_to_df(BASE_DIR / "patent_inventors.csv")
    patent_companies_df = load_csv_to_df(BASE_DIR / "patent_companies.csv")

    try:
        connection = mysql.connector.connect(host=host, user=user, password=password, database=db_name)
        cursor = connection.cursor()

        insert_data(
            cursor,
            "patents",
            patents_df,
            [
                "patent_id",
                "title",
                "description",
                "filing_date",
                "publication_date",
                "main_classification",
                "locarno_classification",
            ],
        )
        insert_data(cursor, "inventors", inventors_df, ["inventor_id", "full_name", "country"])
        insert_data(cursor, "companies", companies_df, ["company_id", "company_name"])
        insert_data(cursor, "patent_inventors", patent_inventors_df, ["patent_id", "inventor_id"])
        insert_data(cursor, "patent_companies", patent_companies_df, ["patent_id", "company_id"])

        connection.commit()
        print("Data loaded successfully into MySQL database.")
    except Error as error:
        print(f"Error: {error}")
    finally:
        if "connection" in locals() and connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == "__main__":
    main()