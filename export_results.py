import pandas as pd
import json
import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
import analyze_db

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "outputs"

def local_socket_param(host):
    socket_path = os.getenv("PATENTS_DB_SOCKET", "/run/mysqld/mysqld.sock")
    if host in ("localhost", "127.0.0.1") and Path(socket_path).exists():
        return f"?unix_socket={socket_path}"
    return ""


def get_engine():
    host = os.getenv("PATENTS_DB_HOST", "localhost")
    user = os.getenv("PATENTS_DB_USER", "root")
    password = os.getenv("PATENTS_DB_PASSWORD", "")
    db_name = os.getenv("PATENTS_DB_NAME", "patents_db")
    socket_param = local_socket_param(host)
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db_name}{socket_param}")

def populate_summary_tables(engine):
    print("Populating pre-aggregated summary tables...")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM patent_yearly_summary"))
        conn.execute(text("DELETE FROM company_yearly_summary"))
        conn.execute(text("DELETE FROM monthly_volume_summary"))

        conn.execute(text(
            "INSERT INTO patent_yearly_summary (year, cpc_section, country, count) "
            "SELECT YEAR(p.filing_date) AS year, p.cpc_section, i.country, COUNT(DISTINCT p.patent_id) AS count "
            "FROM patents p "
            "JOIN patent_inventors pi ON p.patent_id = pi.patent_id "
            "JOIN inventors i ON pi.inventor_id = i.inventor_id "
            "WHERE p.filing_date IS NOT NULL AND p.cpc_section != '' "
            "GROUP BY YEAR(p.filing_date), p.cpc_section, i.country"
        ))

        conn.execute(text(
            "INSERT INTO patent_yearly_summary (year, cpc_section, country, count) "
            "SELECT YEAR(p.filing_date) AS year, p.cpc_section, 'ALL' AS country, COUNT(DISTINCT p.patent_id) AS count "
            "FROM patents p "
            "WHERE p.filing_date IS NOT NULL AND p.cpc_section != '' "
            "GROUP BY YEAR(p.filing_date), p.cpc_section"
        ))

        conn.execute(text(
            "INSERT INTO company_yearly_summary (year, company_id, count, type) "
            "SELECT YEAR(p.filing_date) AS year, c.company_id, COUNT(DISTINCT p.patent_id) AS count, "
            "CASE "
            "WHEN c.company_name REGEXP 'Univ|College|Institute' THEN 'University' "
            "WHEN c.company_name REGEXP 'Gov|National|Department' THEN 'Government' "
            "ELSE 'Corporate' END AS type "
            "FROM patents p "
            "JOIN patent_companies pc ON p.patent_id = pc.patent_id "
            "JOIN companies c ON pc.company_id = c.company_id "
            "WHERE p.filing_date IS NOT NULL "
            "GROUP BY YEAR(p.filing_date), c.company_id, type"
        ))

        conn.execute(text(
            "INSERT INTO monthly_volume_summary (month, count) "
            "SELECT DATE_FORMAT(p.filing_date, '%Y-%m-01') AS month, COUNT(*) AS count "
            "FROM patents p "
            "WHERE p.filing_date IS NOT NULL "
            "GROUP BY month"
        ))

    print("Summary tables populated.")


def export_reports():
    print("Starting report generation...")
    OUTPUT_DIR.mkdir(exist_ok=True)
    engine = get_engine()
    populate_summary_tables(engine)
    
    try:
        # 1. Top Inventors
        print("Exporting top inventors...")
        inv_df = analyze_db.get_top_inventors_global_ranking(engine)
        inv_df.to_csv(OUTPUT_DIR / "top_inventors.csv", index=False)
        
        # 2. Top Companies
        print("Exporting top companies...")
        comp_df = analyze_db.get_top_companies_market_share(engine)
        comp_df.to_csv(OUTPUT_DIR / "top_companies.csv", index=False)
        
        # 3. Country Trends
        print("Exporting country trends...")
        trends_df = analyze_db.get_top_countries_by_patent_output(engine)
        trends_df.to_csv(OUTPUT_DIR / "country_trends.csv", index=False)
        
        # 4. Patent Forecasts (LinearRegression)
        print("Exporting patent forecasts...")
        _, forecast_df = analyze_db.predict_patent_volume_forecasting(engine)
        forecast_data = forecast_df.to_dict(orient="records") if not forecast_df.empty else []
        with open(OUTPUT_DIR / "patent_forecasts.json", "w") as f:
            json.dump(forecast_data, f, indent=4)
            
        # 5. Technology Clusters (K-Means)
        print("Exporting technology clusters...")
        clusters_df = analyze_db.cluster_country_innovation_trajectory(engine)
        clusters_df.to_csv(OUTPUT_DIR / "technology_clusters.csv", index=False)
        
        # 6. Full JSON Report Summary
        print("Generating full JSON summary...")
        summary = {
            "status": "success",
            "top_inventors_count": len(inv_df),
            "top_companies_count": len(comp_df),
            "forecast_years_predicted": len(forecast_df),
            "clusters_generated": len(clusters_df['cluster'].unique()) if not clusters_df.empty else 0,
            "message": "Pipeline phase 4 reports successfully generated."
        }
        with open(OUTPUT_DIR / "report_summary.json", "w") as f:
            json.dump(summary, f, indent=4)
            
        print("Reports successfully exported to outputs/ directory.")
    except Exception as e:
        print(f"Error during report export: {e}")
        sys.exit(1)

if __name__ == "__main__":
    export_reports()