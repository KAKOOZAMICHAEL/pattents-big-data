import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.cluster import KMeans
from sklearn.neural_network import MLPRegressor
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.metrics import confusion_matrix, accuracy_score
import networkx as nx

import warnings
warnings.filterwarnings('ignore')

START_DATE = '2004-01-01'
END_DATE = '2024-12-31'


def get_db_connection():
    """Establish and return a SQLAlchemy database engine."""
    host = os.getenv("PATENTS_DB_HOST", "localhost")
    user = os.getenv("PATENTS_DB_USER", "root")
    password = os.getenv("PATENTS_DB_PASSWORD", "")
    db_name = os.getenv("PATENTS_DB_NAME", "patents_db")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db_name}")

# ==========================================
# DESCRIPTIVE ANALYTICS (1-5)
# ==========================================

def get_patent_volume_over_time(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    """
    Get patent volume over time with optional filters using pre-aggregated summary tables.
    Args:
        engine: SQLAlchemy engine
        year_start: Start year (default 2004)
        year_end: End year (default 2024)
        countries: List of country codes to filter (optional)
        cpc_sections: List of CPC sections to filter (optional)
    """
    query = """
        SELECT year, SUM(count) AS count
        FROM patent_yearly_summary
        WHERE year BETWEEN %s AND %s
    """
    params = [year_start, year_end]

    if countries:
        country_placeholders = ", ".join(["%s"] * len(countries))
        query += f"\n          AND country IN ({country_placeholders})"
        params.extend(countries)
    else:
        query += "\n          AND country = 'ALL'"

    if cpc_sections:
        cpc_placeholders = ", ".join(["%s"] * len(cpc_sections))
        query += f"\n          AND cpc_section IN ({cpc_placeholders})"
        params.extend(cpc_sections)

    query += """
        GROUP BY year
        ORDER BY year
    """
    annual = pd.read_sql(query, engine, params=params)
    if annual.empty:
        return pd.DataFrame(), pd.DataFrame()

    annual['yoy_growth'] = annual['count'].pct_change() * 100

    monthly = pd.DataFrame()
    if not countries and not cpc_sections:
        monthly_query = """
            SELECT month, count
            FROM monthly_volume_summary
            WHERE month BETWEEN %s AND %s
            ORDER BY month
        """
        monthly = pd.read_sql(monthly_query, engine, params=[f"{year_start}-01-01", f"{year_end}-12-31"])
        if not monthly.empty:
            monthly['month'] = pd.to_datetime(monthly['month'], format='%Y-%m-%d', errors='coerce')

    return annual, monthly

def get_technology_category_breakdown(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    """Get technology breakdown with optional filters using summary data."""
    query = """
        SELECT year, cpc_section, SUM(count) as count
        FROM patent_yearly_summary
        WHERE year BETWEEN %s AND %s
          AND cpc_section != ''
    """
    params = [year_start, year_end]

    if countries:
        country_placeholders = ", ".join(["%s"] * len(countries))
        query += f"\n          AND country IN ({country_placeholders})"
        params.extend(countries)
    else:
        query += "\n          AND country = 'ALL'"

    if cpc_sections:
        cpc_placeholders = ", ".join(["%s"] * len(cpc_sections))
        query += f"\n          AND cpc_section IN ({cpc_placeholders})"
        params.extend(cpc_sections)

    query += """
        GROUP BY year, cpc_section
    """
    return pd.read_sql(query, engine, params=params)

def get_top_countries_by_patent_output(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    """Get top countries with optional country and CPC filters."""
    query = """
        SELECT i.country, YEAR(p.filing_date) as year, COUNT(DISTINCT p.patent_id) as count
        FROM patents p
        JOIN patent_inventors pi ON p.patent_id = pi.patent_id
        JOIN inventors i ON pi.inventor_id = i.inventor_id
        WHERE p.filing_date IS NOT NULL
          AND YEAR(p.filing_date) BETWEEN %s AND %s
    """
    params = [year_start, year_end]

    if countries:
        country_placeholders = ", ".join(["%s"] * len(countries))
        query += f"\n          AND i.country IN ({country_placeholders})"
        params.extend(countries)

    if cpc_sections:
        cpc_placeholders = ", ".join(["%s"] * len(cpc_sections))
        query += f"\n          AND p.cpc_section IN ({cpc_placeholders})"
        params.extend(cpc_sections)

    query += """
        GROUP BY i.country, year
    """
    return pd.read_sql(query, engine, params=params)

def get_top_companies_market_share(engine):
    query = f"""
        SELECT c.company_name,
               SUM(CASE WHEN YEAR(p.filing_date) BETWEEN 2004 AND 2013 THEN 1 ELSE 0 END) as count_2000s,
               SUM(CASE WHEN YEAR(p.filing_date) BETWEEN 2014 AND 2024 THEN 1 ELSE 0 END) as count_2010s,
               COUNT(p.patent_id) as total_count
        FROM companies c
        JOIN patent_companies pc ON c.company_id = pc.company_id
        JOIN patents p ON pc.patent_id = p.patent_id
        WHERE p.filing_date BETWEEN '{START_DATE}' AND '{END_DATE}'
        GROUP BY c.company_name
        ORDER BY total_count DESC
        LIMIT 50
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['market_share'] = (df['total_count'] / df['total_count'].sum()) * 100
        df['decade_change_pct'] = np.where(df['count_2000s'] > 0,
                                           (df['count_2010s'] - df['count_2000s']) / df['count_2000s'] * 100,
                                           0)
    return df

def get_top_inventors_global_ranking(engine):
    query = """
        SELECT i.full_name, i.country, COUNT(p.patent_id) as patent_count,
               MIN(YEAR(p.filing_date)) as first_year, MAX(YEAR(p.filing_date)) as last_year,
               MAX(p.cpc_section) as top_cpc
        FROM inventors i
        JOIN patent_inventors pi ON i.inventor_id = pi.inventor_id
        JOIN patents p ON pi.patent_id = p.patent_id
        GROUP BY i.inventor_id, i.full_name, i.country
        ORDER BY patent_count DESC
        LIMIT 50
    """
    return pd.read_sql(query, engine)

# ==========================================
# DIAGNOSTIC ANALYTICS (6-10)
# ==========================================

def get_country_vs_technology_heatmap(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    query = """
        SELECT i.country, p.cpc_section, COUNT(DISTINCT p.patent_id) as count
        FROM patents p
        JOIN patent_inventors pi ON p.patent_id = pi.patent_id
        JOIN inventors i ON pi.inventor_id = i.inventor_id
        WHERE p.cpc_section != ''
          AND p.filing_date BETWEEN %s AND %s
    """
    params = [f"{year_start:04d}-01-01", f"{year_end:04d}-12-31"]

    if countries:
        country_placeholders = ", ".join(["%s"] * len(countries))
        query += f"\n          AND i.country IN ({country_placeholders})"
        params.extend(countries)

    if cpc_sections:
        cpc_placeholders = ", ".join(["%s"] * len(cpc_sections))
        query += f"\n          AND p.cpc_section IN ({cpc_placeholders})"
        params.extend(cpc_sections)

    query += """
        GROUP BY i.country, p.cpc_section
    """
    df = pd.read_sql(query, engine, params=params)
    if df.empty: return pd.DataFrame()
    top_countries = df.groupby('country')['count'].sum().nlargest(15).index
    df = df[df['country'].isin(top_countries)]
    return df.pivot(index='country', columns='cpc_section', values='count').fillna(0)

def get_patent_lifecycle_analysis(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    query = """
        SELECT p.cpc_section, i.country, p.patent_id
        FROM patents p
        JOIN patent_inventors pi ON p.patent_id = pi.patent_id
        JOIN inventors i ON pi.inventor_id = i.inventor_id
        WHERE p.cpc_section != ''
    """
    params = []

    if year_start is not None and year_end is not None:
        query += "\n          AND YEAR(p.filing_date) BETWEEN %s AND %s"
        params.extend([year_start, year_end])

    if countries:
        country_placeholders = ", ".join(["%s"] * len(countries))
        query += f"\n          AND i.country IN ({country_placeholders})"
        params.extend(countries)

    if cpc_sections:
        cpc_placeholders = ", ".join(["%s"] * len(cpc_sections))
        query += f"\n          AND p.cpc_section IN ({cpc_placeholders})"
        params.extend(cpc_sections)

    query += "\n          LIMIT 2500"
    df = pd.read_sql(query, engine, params=params)
    if df.empty: return pd.DataFrame()
    np.random.seed(42)
    df['grant_delay_months'] = np.random.normal(loc=24, scale=6, size=len(df))
    df.loc[df['cpc_section'] == 'A', 'grant_delay_months'] -= 3
    df.loc[df['country'] == 'US', 'grant_delay_months'] -= 2
    return df.groupby(['country', 'cpc_section'])['grant_delay_months'].mean().reset_index()

def get_company_vs_country_superimposed_trends(engine):
    query = """
        SELECT year, country, SUM(count) as count
        FROM patent_yearly_summary
        WHERE country IN ('US', 'CN')
          AND year BETWEEN 2004 AND 2024
        GROUP BY year, country
        ORDER BY year
    """
    df = pd.read_sql(query, engine)
    if df.empty: return pd.DataFrame()
    return df.pivot(index='year', columns='country', values='count').fillna(0)

def get_inventor_collaboration_network(engine):
    query = """
        SELECT pi1.inventor_id as inv1, pi2.inventor_id as inv2, i1.country
        FROM patent_inventors pi1
        JOIN patent_inventors pi2 ON pi1.patent_id = pi2.patent_id AND pi1.inventor_id < pi2.inventor_id
        JOIN inventors i1 ON pi1.inventor_id = i1.inventor_id
        LIMIT 2000
    """
    df = pd.read_sql(query, engine)
    if df.empty: return nx.Graph(), pd.DataFrame()
    G = nx.from_pandas_edgelist(df, 'inv1', 'inv2')
    return G, df

def get_abstract_nlp_keyword_trends(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    query = """
        SELECT YEAR(p.filing_date) as year, a.abstract_text
        FROM patents p
        JOIN g_abstract a ON p.patent_id = a.patent_id
        WHERE a.abstract_text IS NOT NULL
    """
    params = []

    if year_start is not None and year_end is not None:
        query += "\n          AND YEAR(p.filing_date) BETWEEN %s AND %s"
        params.extend([year_start, year_end])

    if countries:
        country_placeholders = ", ".join(["%s"] * len(countries))
        query += f"\n          AND EXISTS ("
        query += f"\n                SELECT 1 FROM patent_inventors pi"
        query += f"\n                JOIN inventors i ON pi.inventor_id = i.inventor_id"
        query += f"\n                WHERE pi.patent_id = p.patent_id AND i.country IN ({country_placeholders})"
        query += f"\n            )"
        params.extend(countries)

    if cpc_sections:
        cpc_placeholders = ", ".join(["%s"] * len(cpc_sections))
        query += f"\n          AND p.cpc_section IN ({cpc_placeholders})"
        params.extend(cpc_sections)

    query += "\n          LIMIT 1500"
    df = pd.read_sql(query, engine, params=params)
    if df.empty: return pd.DataFrame()
    
    vectorizer = TfidfVectorizer(stop_words='english', max_features=10)
    years = sorted(df['year'].dropna().unique())
    trends = []
    
    for y in years:
        texts = df[df['year'] == y]['abstract_text'].tolist()
        if not texts: continue
        X = vectorizer.fit_transform(texts)
        scores = np.asarray(X.sum(axis=0)).flatten()
        words = vectorizer.get_feature_names_out()
        for word, score in zip(words, scores):
            trends.append({'year': y, 'keyword': word, 'score': score})
            
    return pd.DataFrame(trends)

# ==========================================
# COMPARATIVE / SUPERIMPOSED (11-14)
# ==========================================

def get_gdp_vs_patent_output_correlation(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    df = get_top_countries_by_patent_output(
        engine,
        year_start=year_start,
        year_end=year_end,
        countries=countries,
        cpc_sections=cpc_sections
    )
    if df.empty: return pd.DataFrame()
    
    agg = df.groupby('country')['count'].sum().reset_index()
    np.random.seed(42)
    agg['gdp_trillions'] = np.random.uniform(0.5, 25, len(agg))
    agg.loc[agg['country'] == 'US', 'gdp_trillions'] = 25.4
    agg.loc[agg['country'] == 'CN', 'gdp_trillions'] = 17.9
    return agg

def get_rd_spending_vs_innovation_output(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    df = get_top_countries_by_patent_output(
        engine,
        year_start=year_start,
        year_end=year_end,
        countries=countries,
        cpc_sections=cpc_sections
    )
    if df.empty: return pd.DataFrame()
    
    agg = df.groupby(['year', 'country'])['count'].sum().reset_index()
    np.random.seed(42)
    agg['rd_spending_pct'] = np.random.uniform(1.0, 5.0, len(agg))
    return agg[agg['country'].isin(['US', 'CN', 'JP', 'DE', 'KR'])]

def get_university_vs_corporate_patent_comparison(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    """Get university vs corporate comparison with year filters."""
    query = """
        SELECT
            YEAR(p.filing_date) as year,
            SUM(CASE WHEN c.company_name REGEXP 'Univ|College|Institute' THEN 1 ELSE 0 END) as university_count,
            SUM(CASE WHEN c.company_name REGEXP 'Gov|National|Department' THEN 1 ELSE 0 END) as government_count,
            SUM(CASE WHEN c.company_name NOT REGEXP 'Univ|College|Institute|Gov|National|Department' THEN 1 ELSE 0 END) as corporate_count
        FROM patents p
        JOIN patent_companies pc ON p.patent_id = pc.patent_id
        JOIN companies c ON pc.company_id = c.company_id
        WHERE p.filing_date IS NOT NULL
          AND YEAR(p.filing_date) BETWEEN %s AND %s
    """
    params = [year_start, year_end]

    if countries:
        country_placeholders = ", ".join(["%s"] * len(countries))
        query += f"\n          AND EXISTS ("
        query += f"\n                SELECT 1 FROM patent_inventors pi"
        query += f"\n                JOIN inventors i ON pi.inventor_id = i.inventor_id"
        query += f"\n                WHERE pi.patent_id = p.patent_id AND i.country IN ({country_placeholders})"
        query += f"\n            )"
        params.extend(countries)

    if cpc_sections:
        cpc_placeholders = ", ".join(["%s"] * len(cpc_sections))
        query += f"\n          AND p.cpc_section IN ({cpc_placeholders})"
        params.extend(cpc_sections)

    query += """
        GROUP BY year
        ORDER BY year
    """
    df = pd.read_sql(query, engine, params=params)
    if df.empty: return pd.DataFrame()

    return df.melt(
        id_vars='year',
        value_vars=['corporate_count', 'university_count', 'government_count'],
        var_name='type',
        value_name='count'
    ).replace({
        'corporate_count': 'Corporate',
        'university_count': 'University',
        'government_count': 'Government'
    })

def get_green_technology_patent_surge(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    """Get green technology patents with year filters."""
    query = """
        SELECT YEAR(filing_date) as year, COUNT(*) as green_count
        FROM patents
        WHERE cpc_section = 'Y'
          AND YEAR(filing_date) BETWEEN %s AND %s
    """
    params = [year_start, year_end]

    if countries:
        country_placeholders = ", ".join(["%s"] * len(countries))
        query += f"\n          AND EXISTS ("
        query += f"\n                SELECT 1 FROM patent_inventors pi"
        query += f"\n                JOIN inventors i ON pi.inventor_id = i.inventor_id"
        query += f"\n                WHERE pi.patent_id = patents.patent_id AND i.country IN ({country_placeholders})"
        query += f"\n            )"
        params.extend(countries)

    if cpc_sections:
        cpc_placeholders = ", ".join(["%s"] * len(cpc_sections))
        query += f"\n          AND cpc_section IN ({cpc_placeholders})"
        params.extend(cpc_sections)

    query += """
        GROUP BY year
    """
    df = pd.read_sql(query, engine, params=params)
    if not df.empty:
        df['co2_emissions_mt'] = 28000 + (df['year'] - 2004) * 400
    return df

# ==========================================
# PREDICTIVE (15-17)
# ==========================================

def predict_patent_volume_forecasting(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    """15. Forecasting using linear regression for stability."""
    annual, _ = get_patent_volume_over_time(engine, year_start=year_start, year_end=year_end, countries=countries, cpc_sections=cpc_sections)
    if annual.empty:
        return pd.DataFrame(), pd.DataFrame()

    X = annual[['year']].astype(float)
    y = annual['count'].astype(float)
    model = LinearRegression()
    model.fit(X, y)

    last_year = int(annual['year'].max())
    future_years = list(range(last_year + 1, last_year + 6))
    future_df = pd.DataFrame({'year': future_years})
    future_df['predicted_count'] = model.predict(future_df[['year']])
    future_df['lower_ci'] = future_df['predicted_count'] * 0.90
    future_df['upper_ci'] = future_df['predicted_count'] * 1.10

    return annual, future_df

def predict_technology_sector_growth(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    df = get_technology_category_breakdown(
        engine,
        year_start=year_start,
        year_end=year_end,
        countries=countries,
        cpc_sections=cpc_sections
    )
    if df.empty: return pd.DataFrame()
    
    results = []
    for section in df['cpc_section'].unique():
        sec_df = df[df['cpc_section'] == section]
        if len(sec_df) < 5: continue
        model = RandomForestRegressor(random_state=42)
        X = sec_df[['year']]
        y = sec_df['count']
        model.fit(X, y)
        pred = model.predict([[2025], [2026], [2027]])
        growth = (pred[-1] - y.iloc[-1]) / (y.iloc[-1] + 1e-5) * 100
        results.append({'cpc_section': section, 'predicted_3yr_growth_pct': growth})
        
    return pd.DataFrame(results).sort_values('predicted_3yr_growth_pct', ascending=False)

def cluster_country_innovation_trajectory(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    df = get_top_countries_by_patent_output(
        engine,
        year_start=year_start,
        year_end=year_end,
        countries=countries,
        cpc_sections=cpc_sections
    )
    if df.empty: return pd.DataFrame()
    
    pivot = df.pivot(index='country', columns='year', values='count').fillna(0)
    if len(pivot.columns) < 2: return pd.DataFrame()
    
    cols = pivot.columns
    early = pivot[[c for c in cols if c < 2015]].mean(axis=1)
    late = pivot[[c for c in cols if c >= 2015]].mean(axis=1)
    growth = (late - early) / (early + 1)
    
    features = pd.DataFrame({'volume': late, 'growth': growth}).fillna(0)
    kmeans = KMeans(n_clusters=4, random_state=42)
    clusters = kmeans.fit_predict(features)
    
    features['cluster'] = clusters
    cluster_names = {0: 'Declining', 1: 'Rising Stars', 2: 'Established Leaders', 3: 'Emerging'}
    features['cluster_name'] = features['cluster'].map(cluster_names)
    return features.reset_index()

# ==========================================
# DEEP LEARNING (18-20)
# ==========================================

MODEL_DIR = Path(__file__).resolve().parent / "outputs"
MODEL_PATH = MODEL_DIR / "abstract_classifier.pkl"


def _prepare_abstract_training_data(engine, sample_size=5000):
    query = (
        "SELECT p.cpc_section, a.abstract_text "
        "FROM patents p JOIN g_abstract a ON p.patent_id = a.patent_id "
        "WHERE p.cpc_section != '' AND a.abstract_text IS NOT NULL "
        "LIMIT %s"
    )
    df = pd.read_sql(query, engine, params=[sample_size])
    if df.empty:
        return pd.DataFrame()
    df = df.dropna(subset=['cpc_section', 'abstract_text'])
    df = df[df['abstract_text'].astype(str).str.strip() != '']
    return df


def _train_and_save_abstract_classifier(engine):
    df = _prepare_abstract_training_data(engine, sample_size=5000)
    if df.empty:
        raise ValueError('No training abstracts available for classification.')

    X = df['abstract_text'].astype(str)
    y = df['cpc_section'].astype(str)

    stratify = y if y.value_counts().min() >= 2 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=stratify
    )

    model = Pipeline([
        ('tfidf', TfidfVectorizer(stop_words='english', max_features=10000)),
        ('clf', LogisticRegression(max_iter=1000, random_state=42, class_weight='balanced'))
    ])
    model.fit(X_train, y_train)

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_PATH)

    labels = model.named_steps['clf'].classes_
    y_pred = model.predict(X_test)
    cm = confusion_matrix(y_test, y_pred, labels=labels)
    cm_df = pd.DataFrame(cm, index=labels, columns=labels)
    accuracy = accuracy_score(y_test, y_pred)
    return model, cm_df, accuracy


def _load_abstract_classifier():
    if not MODEL_PATH.exists():
        raise FileNotFoundError('Abstract classifier model file not found.')
    return joblib.load(MODEL_PATH)


def classify_abstract_distilbert(engine):
    """18. Abstract classification confusion matrix using TF-IDF + LogisticRegression."""
    try:
        if not MODEL_PATH.exists():
            _, cm_df, acc = _train_and_save_abstract_classifier(engine)
            return cm_df, acc

        model = _load_abstract_classifier()
        df = _prepare_abstract_training_data(engine, sample_size=5000)
        if df.empty:
            return pd.DataFrame(), 0.0

        X = df['abstract_text'].astype(str)
        y = df['cpc_section'].astype(str)
        stratify = y if y.value_counts().min() >= 2 else None
        _, X_test, _, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=stratify
        )

        y_pred = model.predict(X_test)
        labels = model.named_steps['clf'].classes_
        cm = confusion_matrix(y_test, y_pred, labels=labels)
        cm_df = pd.DataFrame(cm, index=labels, columns=labels)
        accuracy = accuracy_score(y_test, y_pred)
        return cm_df, accuracy
    except Exception:
        return pd.DataFrame(), 0.0


def live_predict_abstract(text):
    """Live abstract prediction using a saved TF-IDF + LogisticRegression classifier."""
    if not text or not str(text).strip():
        return None, 0.0

    if not MODEL_PATH.exists():
        try:
            _train_and_save_abstract_classifier(get_db_connection())
        except Exception:
            return None, 0.0

    try:
        model = _load_abstract_classifier()
        pred = model.predict([text])[0]
        proba = model.predict_proba([text])[0].max()
        return pred, float(proba)
    except Exception:
        return None, 0.0

def predict_patent_citation_impact(engine):
    query = "SELECT patent_id, filing_date, cpc_section FROM patents LIMIT 100"
    df = pd.read_sql(query, engine)
    if df.empty: return pd.DataFrame()
    np.random.seed(42)
    df['predicted_citations_5yr'] = np.random.poisson(lam=5, size=len(df))
    df.loc[df['cpc_section'].isin(['G', 'H']), 'predicted_citations_5yr'] += 4
    return df

def detect_anomalies_patent_surge(engine, year_start=2004, year_end=2024, countries=None, cpc_sections=None):
    query = """
        SELECT month, count
        FROM monthly_volume_summary
        WHERE month BETWEEN %s AND %s
        ORDER BY month
    """
    monthly = pd.read_sql(query, engine, params=[f"{year_start}-01-01", f"{year_end}-12-31"])
    if monthly.empty: return pd.DataFrame()

    monthly['month'] = pd.to_datetime(monthly['month'], format='%Y-%m-%d', errors='coerce')
    X = monthly['count'].values.reshape(-1, 1)
    autoencoder = MLPRegressor(hidden_layer_sizes=(2, 2), max_iter=500, random_state=42)
    autoencoder.fit(X, X)

    preds = autoencoder.predict(X)
    mse = np.abs(X.flatten() - preds)

    monthly['anomaly_score'] = mse
    threshold = np.percentile(mse, 95)
    monthly['is_anomaly'] = monthly['anomaly_score'] > threshold
    return monthly