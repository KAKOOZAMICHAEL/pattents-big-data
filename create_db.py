import os

import mysql.connector
from mysql.connector import Error


def create_database(cursor, db_name):
    """Create the target database if it does not exist."""
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print(f"Database '{db_name}' created or already exists.")


def create_tables(cursor, db_name):
    """Create normalized patent tables with explicit link tables."""
    cursor.execute(f"USE {db_name}")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS patents (
            patent_id VARCHAR(32) PRIMARY KEY,
            title VARCHAR(500) NOT NULL,
            description TEXT,
            filing_date DATE NULL,
            publication_date DATE NULL,
            main_classification VARCHAR(100),
            locarno_classification VARCHAR(100)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS inventors (
            inventor_id INT PRIMARY KEY,
            full_name VARCHAR(255) NOT NULL,
            country VARCHAR(100) NOT NULL,
            UNIQUE KEY uniq_inventor_name_country (full_name, country)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS companies (
            company_id INT PRIMARY KEY,
            company_name VARCHAR(255) NOT NULL,
            UNIQUE KEY uniq_company_name (company_name)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS patent_inventors (
            patent_id VARCHAR(32) NOT NULL,
            inventor_id INT NOT NULL,
            PRIMARY KEY (patent_id, inventor_id),
            FOREIGN KEY (patent_id) REFERENCES patents(patent_id) ON DELETE CASCADE,
            FOREIGN KEY (inventor_id) REFERENCES inventors(inventor_id) ON DELETE CASCADE
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS patent_companies (
            patent_id VARCHAR(32) NOT NULL,
            company_id INT NOT NULL,
            PRIMARY KEY (patent_id, company_id),
            FOREIGN KEY (patent_id) REFERENCES patents(patent_id) ON DELETE CASCADE,
            FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE CASCADE
        )
        """
    )

    create_index_if_missing(cursor, "patents", "idx_patents_filing_date", "filing_date")
    create_index_if_missing(cursor, "patents", "idx_patents_main_classification", "main_classification")
    print("Tables created successfully with referential integrity.")


def create_index_if_missing(cursor, table_name, index_name, column_name):
    """
    Create an index when it does not exist.
    MySQL does not support CREATE INDEX IF NOT EXISTS syntax.
    """
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND index_name = %s
        """,
        (table_name, index_name),
    )
    exists = cursor.fetchone()[0] > 0
    if not exists:
        cursor.execute(f"CREATE INDEX {index_name} ON {table_name}({column_name})")


def main():
    """Create the MySQL schema using environment-configured credentials."""
    host = os.getenv("PATENTS_DB_HOST", "localhost")
    user = os.getenv("PATENTS_DB_USER", "root")
    password = os.getenv("PATENTS_DB_PASSWORD", "")
    db_name = os.getenv("PATENTS_DB_NAME", "patents_db")

    try:
        connection = mysql.connector.connect(host=host, user=user, password=password)
        if connection.is_connected():
            cursor = connection.cursor()
            create_database(cursor, db_name)
            create_tables(cursor, db_name)
            connection.commit()
    except Error as error:
        print(f"Error connecting to MySQL: {error}")
    finally:
        if "connection" in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")


if __name__ == "__main__":
    main()