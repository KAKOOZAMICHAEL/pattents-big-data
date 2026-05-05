import pandas as pd
import json
import os
from pathlib import Path
from sqlalchemy import create_engine
import analyze_db

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "outputs"

def get_engine():
    host = os.getenv("PATENTS_DB_HOST", "localhost")
    user = os.getenv("PATENTS_DB_USER", "root")
    password = os.getenv("PATENTS_DB_PASSWORD", "")
    db_name = os.getenv("PATENTS_DB_NAME", "patents_db")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db_name}")

def export_reports():
    print("Starting report generation...")
    OUTPUT_DIR.mkdir(exist_ok=True)
    engine = get_engine()
    
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
        
        # 4. Patent Forecasts (Prophet)
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

if __name__ == "__main__":
    export_reports()