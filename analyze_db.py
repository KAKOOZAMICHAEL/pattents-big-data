import mysql.connector
from mysql.connector import Error

def run_query(cursor, query, description):
    """Run a query and print results."""
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        print(f"\n{description}:")
        if results:
            for row in results:
                print(row)
        else:
            print("No results.")
    except Error as e:
        print(f"Error running query '{description}': {e}")

def main():
    # Database connection
    host = 'localhost'
    user = 'root'
    password = ''  # Update as needed
    db_name = 'patents_db'
    
    queries = [
        ("SELECT COUNT(*) FROM patents", "1. Total Patents"),
        ("SELECT COUNT(*) FROM inventors", "1. Total Inventors"),
        ("SELECT COUNT(*) FROM companies", "1. Total Companies"),
        ("SELECT country, COUNT(*) AS inventor_count FROM inventors GROUP BY country ORDER BY inventor_count DESC", "2. Number of Inventors per Country"),
        ("SELECT c.company_name, COUNT(DISTINCT r.patent_id) AS patent_count FROM companies c LEFT JOIN relationships r ON c.company_id = r.company_id GROUP BY c.company_id, c.company_name ORDER BY patent_count DESC LIMIT 10", "3. Top 10 Companies by Number of Patents"),
        ("SELECT YEAR(filing_date) AS year, COUNT(*) AS patent_count FROM patents GROUP BY YEAR(filing_date) ORDER BY year", "4. Patents per Year"),
        ("SELECT COUNT(*) AS patents_without_companies FROM patents WHERE patent_id NOT IN (SELECT DISTINCT patent_id FROM relationships)", "5. Number of Patents without Companies"),
        ("SELECT (SELECT COUNT(*) FROM patents WHERE patent_id NOT IN (SELECT DISTINCT patent_id FROM relationships)) / COUNT(*) * 100 AS percentage FROM patents LIMIT 1", "5. Percentage of Patents without Companies"),
        ("SELECT AVG(inventor_count) AS avg_inventors_per_patent FROM (SELECT patent_id, COUNT(DISTINCT inventor_id) AS inventor_count FROM relationships GROUP BY patent_id) AS sub", "6. Average Number of Inventors per Patent")
    ]
    
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
        )
        cursor = connection.cursor()
        
        for query, desc in queries:
            run_query(cursor, query, desc)
        
    except Error as e:
        print(f"Error connecting to database: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    main()