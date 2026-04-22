"""
Main Data Engineering Pipeline
Executes the complete workflow: Sample -> Extract -> Clean -> Database -> Analyze -> Export
"""

import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

# Add scripts folder to path
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

# Import modules
import analyze_db
import clean_data
import create_db
import export_results
import extract_data
import load_data
from scripts import sampler

import mysql.connector
from mysql.connector import Error as MySQLError


class DataPipeline:
    """Main pipeline orchestrator"""

    def __init__(self):
        self.start_time = datetime.now()
        self.steps_completed = []
        self.errors = []

        # Configuration
        self.config = {
            "xml_file": BASE_DIR / os.getenv("PATENT_XML_FILE", "ipg230103.xml"),
            "sample_xml_file": BASE_DIR / "sample_patents.xml",
            "json_output": BASE_DIR / "patents_data.json",
            "patents_csv": BASE_DIR / "patents.csv",
            "inventors_csv": BASE_DIR / "inventors.csv",
            "companies_csv": BASE_DIR / "companies.csv",
            "patent_inventors_csv": BASE_DIR / "patent_inventors.csv",
            "patent_companies_csv": BASE_DIR / "patent_companies.csv",
            "db_name": os.getenv("PATENTS_DB_NAME", "patents_db"),
            "db_host": os.getenv("PATENTS_DB_HOST", "localhost"),
            "db_user": os.getenv("PATENTS_DB_USER", "root"),
            "db_password": os.getenv("PATENTS_DB_PASSWORD", ""),
            "sample_count": int(os.getenv("PATENT_SAMPLE_COUNT", "100")),
        }

    def log_step(self, step_name, status="STARTING"):
        """Log pipeline step"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{'='*80}")
        print(f"[{timestamp}] STEP: {step_name}")
        print(f"Status: {status}")
        print(f"{'='*80}\n")

    def log_success(self, step_name):
        """Log successful step completion"""
        self.steps_completed.append(step_name)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[OK] [{timestamp}] SUCCESS: {step_name} completed")

    def log_error(self, step_name, error):
        """Log error and continue"""
        self.errors.append((step_name, str(error)))
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[ERROR] [{timestamp}] ERROR in {step_name}:")
        print(f"   {error}")
        traceback.print_exc()

    def step_1_sample_xml(self):
        """Step 1: Sample the XML file"""
        step_name = "Step 1: Sample XML File"
        self.log_step(step_name)

        try:
            if not self.config["xml_file"].exists():
                raise FileNotFoundError(f"XML file not found: {self.config['xml_file']}")

            print(f"Input file: {self.config['xml_file']}")
            print(f"Output file: {self.config['sample_xml_file']}")
            print(f"Sampling {self.config['sample_count']} patents...\n")

            sampler.create_sample(
                self.config["xml_file"],
                self.config["sample_xml_file"],
                num_patents=self.config["sample_count"],
            )

            print(f"Sample size: {self.config['sample_count']} patents")
            self.log_success(step_name)
            return True
        except Exception as e:
            self.log_error(step_name, e)
            return False

    def step_2_extract_data(self):
        """Step 2: Extract data from XML"""
        step_name = "Step 2: Extract Data from XML"
        self.log_step(step_name)

        try:
            if not self.config["sample_xml_file"].exists():
                raise FileNotFoundError(f"Sample XML file not found: {self.config['sample_xml_file']}")

            print(f"Input file: {self.config['sample_xml_file']}")
            print(f"Output file: {self.config['json_output']}")
            print("Extracting patent data...\n")

            extract_data.main(self.config["sample_xml_file"], self.config["json_output"])

            if self.config["json_output"].exists():
                with self.config["json_output"].open("r", encoding="utf-8") as file_handle:
                    data = json.load(file_handle)
                print(f"[OK] Extracted {len(data)} patents")
                self.log_success(step_name)
                return True
            else:
                raise FileNotFoundError(f"Output JSON not created: {self.config['json_output']}")
        except Exception as e:
            self.log_error(step_name, e)
            return False

    def step_3_clean_data(self):
        """Step 3: Clean data and create CSVs"""
        step_name = "Step 3: Clean and Normalize Data"
        self.log_step(step_name)

        try:
            if not self.config["json_output"].exists():
                raise FileNotFoundError(f"JSON file not found: {self.config['json_output']}")

            print(f"Input file: {self.config['json_output']}")
            print("Output files:")
            print(f"  - {self.config['patents_csv']}")
            print(f"  - {self.config['inventors_csv']}")
            print(f"  - {self.config['companies_csv']}")
            print(f"  - {self.config['patent_inventors_csv']}")
            print(f"  - {self.config['patent_companies_csv']}\n")
            print("Cleaning data...\n")

            clean_data.main(self.config["json_output"], BASE_DIR)

            required_files = [
                self.config["patents_csv"],
                self.config["inventors_csv"],
                self.config["companies_csv"],
                self.config["patent_inventors_csv"],
                self.config["patent_companies_csv"],
            ]

            missing = [file_path for file_path in required_files if not file_path.exists()]
            if missing:
                raise FileNotFoundError(f"Missing output files: {missing}")

            print(f"[OK] Created all CSV files successfully")
            self.log_success(step_name)
            return True
        except Exception as e:
            self.log_error(step_name, e)
            return False

    def step_4_create_database(self):
        """Step 4: Create database and tables"""
        step_name = "Step 4: Create Database and Tables"
        self.log_step(step_name)

        try:
            print(f"Database: {self.config['db_name']}")
            print(f"Host: {self.config['db_host']}")
            print(f"User: {self.config['db_user']}\n")
            print("Creating database and tables...\n")

            connection = mysql.connector.connect(
                host=self.config["db_host"],
                user=self.config["db_user"],
                password=self.config["db_password"],
            )
            cursor = connection.cursor()

            create_db.create_database(cursor, self.config["db_name"])
            create_db.create_tables(cursor, self.config["db_name"])

            connection.commit()
            cursor.close()
            connection.close()

            print(f"[OK] Database created and tables initialized")
            self.log_success(step_name)
            return True
        except MySQLError as e:
            self.log_error(step_name, f"MySQL Error: {e}")
            return False
        except Exception as e:
            self.log_error(step_name, e)
            return False

    def step_5_load_data(self):
        """Step 5: Load data into database"""
        step_name = "Step 5: Load Data into Database"
        self.log_step(step_name)

        try:
            print(f"Database: {self.config['db_name']}")
            print("Loading CSV files into database...\n")

            connection = mysql.connector.connect(
                host=self.config["db_host"],
                user=self.config["db_user"],
                password=self.config["db_password"],
                database=self.config["db_name"],
            )
            cursor = connection.cursor()

            print("Loading patents...")
            patents_df = load_data.prepare_date_columns(
                load_data.load_csv_to_df(self.config["patents_csv"]),
                ["filing_date", "publication_date"],
            )
            load_data.insert_data(
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
            print(f"  [OK] Loaded {len(patents_df)} patents")

            print("Loading inventors...")
            inventors_df = load_data.load_csv_to_df(self.config["inventors_csv"])
            load_data.insert_data(cursor, "inventors", inventors_df, ["inventor_id", "full_name", "country"])
            print(f"  [OK] Loaded {len(inventors_df)} inventors")

            print("Loading companies...")
            companies_df = load_data.load_csv_to_df(self.config["companies_csv"])
            load_data.insert_data(cursor, "companies", companies_df, ["company_id", "company_name"])
            print(f"  [OK] Loaded {len(companies_df)} companies")

            print("Loading patent-inventor links...")
            patent_inventors_df = load_data.load_csv_to_df(self.config["patent_inventors_csv"])
            load_data.insert_data(cursor, "patent_inventors", patent_inventors_df, ["patent_id", "inventor_id"])
            print(f"  [OK] Loaded {len(patent_inventors_df)} patent-inventor links")

            print("Loading patent-company links...")
            patent_companies_df = load_data.load_csv_to_df(self.config["patent_companies_csv"])
            load_data.insert_data(cursor, "patent_companies", patent_companies_df, ["patent_id", "company_id"])
            print(f"  [OK] Loaded {len(patent_companies_df)} patent-company links")

            connection.commit()
            cursor.close()
            connection.close()

            print(f"\n[OK] All data loaded successfully into database")
            self.log_success(step_name)
            return True
        except MySQLError as e:
            self.log_error(step_name, f"MySQL Error: {e}")
            return False
        except Exception as e:
            self.log_error(step_name, e)
            return False

    def step_6_analyze_data(self):
        """Step 6: Run analysis queries"""
        step_name = "Step 6: Run Analysis"
        self.log_step(step_name)

        try:
            print(f"Database: {self.config['db_name']}")
            print("Running analysis queries...\n")

            connection = mysql.connector.connect(
                host=self.config["db_host"],
                user=self.config["db_user"],
                password=self.config["db_password"],
                database=self.config["db_name"],
            )
            cursor = connection.cursor()

            queries = [
                ("SELECT COUNT(*) FROM patents", "Total Patents"),
                ("SELECT COUNT(*) FROM inventors", "Total Inventors"),
                ("SELECT COUNT(*) FROM companies", "Total Companies"),
                ("SELECT COUNT(*) FROM patent_inventors", "Total Patent-Inventor Links"),
                ("SELECT COUNT(*) FROM patent_companies", "Total Patent-Company Links"),
                (
                    "SELECT country, COUNT(*) AS count FROM inventors GROUP BY country ORDER BY count DESC LIMIT 5",
                    "Top 5 Countries by Inventor Count",
                ),
                (
                    """
                    SELECT YEAR(filing_date) AS filing_year, COUNT(*) AS patent_count
                    FROM patents
                    WHERE filing_date IS NOT NULL
                    GROUP BY YEAR(filing_date)
                    ORDER BY filing_year
                    LIMIT 10
                    """,
                    "Patent Filing Trend",
                ),
            ]

            for query, description in queries:
                print(f"\n{description}:")
                cursor.execute(query)
                results = cursor.fetchall()
                if results:
                    for row in results:
                        print(f"  {row}")
                else:
                    print("  No results")

            cursor.close()
            connection.close()

            print(f"\n[OK] Analysis completed")
            self.log_success(step_name)
            return True
        except MySQLError as e:
            self.log_error(step_name, f"MySQL Error: {e}")
            return False
        except Exception as e:
            self.log_error(step_name, e)
            return False

    def step_7_export_results(self):
        """Step 7: Export results to CSV and JSON"""
        step_name = "Step 7: Export Results"
        self.log_step(step_name)

        try:
            print(f"Database: {self.config['db_name']}")
            print("Exporting analysis results...\n")

            (BASE_DIR / "outputs").mkdir(exist_ok=True)

            connection = mysql.connector.connect(
                host=self.config["db_host"],
                user=self.config["db_user"],
                password=self.config["db_password"],
                database=self.config["db_name"],
            )
            cursor = connection.cursor()

            export_results.main()

            cursor.close()
            connection.close()

            print(f"\n[OK] Results exported to outputs/")
            self.log_success(step_name)
            return True
            
        except MySQLError as e:
            self.log_error(step_name, f"MySQL Error: {e}")
            return False
        except Exception as e:
            self.log_error(step_name, e)
            return False

    def print_summary(self):
        """Print pipeline execution summary"""
        elapsed = datetime.now() - self.start_time

        print(f"\n\n{'='*80}")
        print("PIPELINE EXECUTION SUMMARY")
        print(f"{'='*80}\n")

        print(f"Total Time: {elapsed.total_seconds():.2f} seconds\n")

        print(f"Steps Completed ({len(self.steps_completed)}):")
        for i, step in enumerate(self.steps_completed, 1):
            print(f"  {i}. [OK] {step}")

        if self.errors:
            print(f"\nErrors Encountered ({len(self.errors)}):")
            for step, error in self.errors:
                print(f"  [ERROR] {step}")
                print(f"    {error}")
        else:
            print(f"\n[OK] Pipeline completed successfully with no errors!")

        print(f"\n{'='*80}\n")

    def run(self):
        """Execute complete pipeline"""
        print(f"\n{'#'*80}")
        print("# DATA ENGINEERING PIPELINE")
        print(f"# Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#'*80}\n")

        all_passed = all([
            self.step_1_sample_xml(),
            self.step_2_extract_data(),
            self.step_3_clean_data(),
            self.step_4_create_database(),
            self.step_5_load_data(),
            self.step_6_analyze_data(),
            self.step_7_export_results(),
        ])

        self.print_summary()

        return 0 if all_passed else 1


def main():
    """Main entry point"""
    pipeline = DataPipeline()
    exit_code = pipeline.run()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
