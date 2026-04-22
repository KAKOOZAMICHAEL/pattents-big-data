import os
from pathlib import Path

import matplotlib.pyplot as plt
import mysql.connector
import pandas as pd
import streamlit as st


BASE_DIR = Path(__file__).resolve().parent

st.set_page_config(
    page_title="Patent Dataset Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


def parse_patent_dates(patents_df):
    """Create dashboard-friendly year columns."""
    for column in ["filing_date", "publication_date"]:
        patents_df[column] = pd.to_datetime(patents_df[column], format="%Y%m%d", errors="coerce")

    patents_df["filing_year"] = patents_df["filing_date"].dt.year
    patents_df["publication_year"] = patents_df["publication_date"].dt.year
    return patents_df


def compute_analytics(patents_df, inventors_df, companies_df, patent_inventors_df, patent_companies_df):
    """Build reusable analytics from normalized tables."""
    patents_df = parse_patent_dates(patents_df.copy())

    inventors_country = (
        inventors_df.groupby("country", dropna=False)
        .size()
        .reset_index(name="inventor_count")
        .sort_values(["inventor_count", "country"], ascending=[False, True])
    )

    top_companies = (
        patent_companies_df.groupby("company_id")
        .size()
        .reset_index(name="patent_count")
        .merge(companies_df, on="company_id", how="left")
        [["company_name", "patent_count"]]
        .sort_values(["patent_count", "company_name"], ascending=[False, True])
    )

    filing_trend = (
        patents_df.dropna(subset=["filing_year"])
        .groupby("filing_year")
        .size()
        .reset_index(name="patent_count")
        .sort_values("filing_year")
    )

    publication_trend = (
        patents_df.dropna(subset=["publication_year"])
        .groupby("publication_year")
        .size()
        .reset_index(name="patent_count")
        .sort_values("publication_year")
    )

    top_classifications = (
        patents_df[patents_df["main_classification"].notna() & (patents_df["main_classification"] != "N/A")]
        .groupby("main_classification")
        .size()
        .reset_index(name="patent_count")
        .sort_values(["patent_count", "main_classification"], ascending=[False, True])
    )

    inventor_load = (
        patent_inventors_df.groupby("patent_id")
        .size()
        .reset_index(name="inventor_count")
    )
    average_inventors = round(inventor_load["inventor_count"].mean(), 2) if not inventor_load.empty else 0.0

    top_inventors = (
        patent_inventors_df.groupby("inventor_id")
        .size()
        .reset_index(name="patent_count")
        .merge(inventors_df, on="inventor_id", how="left")
        [["full_name", "country", "patent_count"]]
        .sort_values(["patent_count", "full_name"], ascending=[False, True])
    )

    patents_with_companies = set(patent_companies_df["patent_id"].unique())
    all_patents = set(patents_df["patent_id"].unique())
    patents_without = len(all_patents - patents_with_companies)
    percentage_without = round((patents_without / len(patents_df) * 100), 2) if len(patents_df) else 0.0

    return {
        "total_patents": len(patents_df),
        "total_inventors": len(inventors_df),
        "total_companies": len(companies_df),
        "patents_without": patents_without,
        "percentage_without": percentage_without,
        "average_inventors": average_inventors,
        "inventors_country": inventors_country,
        "top_companies": top_companies,
        "filing_trend": filing_trend,
        "publication_trend": publication_trend,
        "top_classifications": top_classifications,
        "top_inventors": top_inventors,
        "patents": patents_df,
    }


def db_config():
    """Read MySQL connection settings from environment variables."""
    return {
        "host": os.getenv("PATENTS_DB_HOST", "localhost"),
        "user": os.getenv("PATENTS_DB_USER", "root"),
        "password": os.getenv("PATENTS_DB_PASSWORD", ""),
        "database": os.getenv("PATENTS_DB_NAME", "patents_db"),
    }


@st.cache_data
def load_from_csv():
    """Load patent analytics from generated CSV files."""
    patents_df = pd.read_csv(BASE_DIR / "patents.csv")
    inventors_df = pd.read_csv(BASE_DIR / "inventors.csv")
    companies_df = pd.read_csv(BASE_DIR / "companies.csv")
    patent_inventors_df = pd.read_csv(BASE_DIR / "patent_inventors.csv")
    patent_companies_df = pd.read_csv(BASE_DIR / "patent_companies.csv")
    return compute_analytics(patents_df, inventors_df, companies_df, patent_inventors_df, patent_companies_df)


@st.cache_data
def load_from_mysql():
    """Load normalized patent tables from MySQL."""
    config = db_config()
    connection = mysql.connector.connect(**config)
    try:
        patents_df = pd.read_sql("SELECT * FROM patents", connection)
        inventors_df = pd.read_sql("SELECT * FROM inventors", connection)
        companies_df = pd.read_sql("SELECT * FROM companies", connection)
        patent_inventors_df = pd.read_sql("SELECT * FROM patent_inventors", connection)
        patent_companies_df = pd.read_sql("SELECT * FROM patent_companies", connection)
    finally:
        connection.close()

    for column in ["filing_date", "publication_date"]:
        if column in patents_df.columns:
            patents_df[column] = pd.to_datetime(patents_df[column], errors="coerce").dt.strftime("%Y%m%d")

    return compute_analytics(patents_df, inventors_df, companies_df, patent_inventors_df, patent_companies_df)


def load_dashboard_data():
    """Prefer MySQL as the primary source, with CSV fallback for portability."""
    try:
        data = load_from_mysql()
        return data, "MySQL"
    except Exception as db_error:
        try:
            data = load_from_csv()
            st.warning(f"MySQL unavailable, using generated CSV files instead. Details: {db_error}")
            return data, "CSV"
        except FileNotFoundError as csv_error:
            st.error(
                "No data source is available. Run `python pipeline.py` and ensure MySQL is reachable "
                f"or the generated CSVs exist. Missing file: {csv_error}"
            )
            st.stop()
        except Exception as csv_error:
            st.error(f"Failed to load dashboard data from MySQL and CSV files: {csv_error}")
            st.stop()


data, source_name = load_dashboard_data()
patents_with_companies = data["total_patents"] - data["patents_without"]
pie_data = pd.DataFrame(
    {
        "Category": ["With Companies", "Without Companies"],
        "Count": [patents_with_companies, data["patents_without"]],
    }
)

st.title("Patent Dataset Dashboard")
st.markdown(
    "Patent analytics built from the normalized ETL pipeline. "
    f"Current source: **{source_name}**."
)

with st.container():
    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
    kpi1.metric("Total Patents", data["total_patents"])
    kpi2.metric("Total Inventors", data["total_inventors"])
    kpi3.metric("Total Companies", data["total_companies"])
    kpi4.metric("Patents without Companies", data["patents_without"], f"{data['percentage_without']}%")
    kpi5.metric("Avg Inventors / Patent", data["average_inventors"])

st.markdown("---")

st.header("Geography And Ownership")
geo_col, company_col = st.columns(2)
with geo_col:
    st.subheader("Inventors by Country")
    st.bar_chart(data["inventors_country"].head(15).set_index("country"))
    st.dataframe(data["inventors_country"].head(15), use_container_width=True)
with company_col:
    st.subheader("Top Companies by Patent Count")
    st.bar_chart(data["top_companies"].head(15).set_index("company_name"))
    st.dataframe(data["top_companies"].head(15), use_container_width=True)

st.markdown("---")

st.header("Patent Trends")
trend_col, trend_side_col = st.columns([2, 1])
with trend_col:
    st.subheader("Filing Trend Over Time")
    st.line_chart(data["filing_trend"].set_index("filing_year"))
    st.subheader("Publication Trend Over Time")
    st.line_chart(data["publication_trend"].set_index("publication_year"))
with trend_side_col:
    st.subheader("Company Coverage")
    fig, ax = plt.subplots()
    pie_data.set_index("Category").plot.pie(
        y="Count",
        autopct="%1.1f%%",
        legend=False,
        ylabel="",
        ax=ax,
    )
    st.pyplot(fig)

st.markdown("---")

st.header("Distinct Patent Patterns")
pattern_col, inventor_col = st.columns(2)
with pattern_col:
    st.subheader("Top Patent Classifications")
    st.bar_chart(data["top_classifications"].head(15).set_index("main_classification"))
    st.dataframe(data["top_classifications"].head(15), use_container_width=True)
with inventor_col:
    st.subheader("Most Prolific Inventors")
    st.dataframe(data["top_inventors"].head(15), use_container_width=True)

st.markdown("---")

st.header("Patent Detail Sample")
detail_columns = [
    "patent_id",
    "title",
    "filing_date",
    "publication_date",
    "main_classification",
    "locarno_classification",
]
st.dataframe(data["patents"][detail_columns].head(50), use_container_width=True)
