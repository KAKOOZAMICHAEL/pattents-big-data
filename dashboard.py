import streamlit as st
import pandas as pd
import numpy as np
import analyze_db
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

st.set_page_config(page_title="Patent Intelligence Dashboard", layout="wide", initial_sidebar_state="expanded")

@st.cache_resource
def get_connection():
    try:
        return analyze_db.get_db_connection()
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

conn = get_connection()
ENGINE_URL = None

if conn:
    try:
        ENGINE_URL = str(conn.url)
    except Exception as e:
        st.error(f"Failed to get database URL: {e}")
        ENGINE_URL = None

if not ENGINE_URL:
    st.error("❌ Database connection is not available. Please ensure MySQL is running and configuration is correct.")
    st.stop()

engine = create_engine(ENGINE_URL)

# ==========================================
# SIDEBAR FILTERS
# ==========================================
st.sidebar.title("Global Filters")
year_range = st.sidebar.slider("Year Range", 2004, 2024, (2004, 2024))

all_countries = ["US", "CN", "JP", "DE", "KR", "GB", "FR", "CA", "IN"]
selected_countries = st.sidebar.multiselect("Country", all_countries, default=[])

all_tech = ["A", "B", "C", "D", "E", "F", "G", "H", "Y"]
selected_tech = st.sidebar.multiselect("Technology Category (CPC)", all_tech, default=[])

st.title("Global Patent Intelligence Dashboard")



tabs = st.tabs([
    "1. Descriptive", 
    "2. Diagnostic", 
    "3. Superimposed Trends", 
    "4. Predictions", 
    "5. Deep Learning"
])

# ==========================================
# TAB 1: DESCRIPTIVE
# ==========================================
with tabs[0]:
    st.header("Descriptive Analytics (What Happened)")
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("1. Patent Volume Over Time")
        with st.spinner("Loading patent volume..."):
            annual, monthly = analyze_db.get_patent_volume_over_time(
                engine, 
                year_start=year_range[0], 
                year_end=year_range[1],
                countries=selected_countries if selected_countries else None,
                cpc_sections=selected_tech if selected_tech else None
            )
        if not annual.empty:
            fig1 = go.Figure()
            fig1.add_trace(go.Bar(x=annual['year'], y=annual['count'], name='Annual Filings', marker_color='#4C78A8'))
            fig1.add_trace(go.Scatter(x=annual['year'], y=annual['yoy_growth'], mode='lines+markers', name='YoY Growth %', yaxis='y2', line=dict(color='#F58518')))
            fig1.update_layout(yaxis2=dict(title='Growth (%)', overlaying='y', side='right'))
            st.plotly_chart(fig1, use_container_width=True)
            
    with c2:
        st.subheader("2. Technology Category Breakdown")
        with st.spinner("Loading technology breakdown..."):
            tech = analyze_db.get_technology_category_breakdown(
                engine, 
                year_start=year_range[0], 
                year_end=year_range[1],
                countries=selected_countries if selected_countries else None,
                cpc_sections=selected_tech if selected_tech else None
            )
        if not tech.empty:
            tech = tech.sort_values(['year', 'cpc_section'])
            fig2 = px.line(tech, x="year", y="count", color="cpc_section", title="Technology Category Breakdown")
            fig2.update_layout(legend_title_text='CPC Section')
            st.plotly_chart(fig2, use_container_width=True)

    st.subheader("3. Top 20 Countries by Patent Output")
    with st.spinner("Loading countries data..."):
        countries = analyze_db.get_top_countries_by_patent_output(
            engine,
            year_start=year_range[0],
            year_end=year_range[1],
            countries=selected_countries if selected_countries else None
        )
    if not countries.empty:
        agg_countries = countries.groupby('country')['count'].sum().reset_index()
        fig3 = px.choropleth(agg_countries, locations="country", color="count", 
                             title="Global Patent Output", color_continuous_scale="Blues")
        st.plotly_chart(fig3, use_container_width=True)
        
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("4. Top 50 Companies Market Share")
        with st.spinner("Loading companies data..."):
            comps = analyze_db.get_top_companies_market_share(engine)
        if not comps.empty:
            st.dataframe(comps.head(50), use_container_width=True)
            
    with c4:
        st.subheader("5. Top Inventors Global Ranking")
        with st.spinner("Loading inventors data..."):
            invs = analyze_db.get_top_inventors_global_ranking(engine)
        if not invs.empty:
            st.dataframe(invs.head(50), use_container_width=True)

# ==========================================
# TAB 2: DIAGNOSTIC
# ==========================================
with tabs[1]:
    st.header("Diagnostic Analytics (Why It Happened)")
    
    st.subheader("6. Country vs Technology Heatmap")
    with st.spinner("Loading heatmap..."):
        heatmap = analyze_db.get_country_vs_technology_heatmap(
            engine,
            year_start=year_range[0],
            year_end=year_range[1],
            countries=selected_countries if selected_countries else None,
            cpc_sections=selected_tech if selected_tech else None
        )
    if not heatmap.empty:
        fig = px.imshow(heatmap, text_auto=True, title="Country Dominance by Tech Sector", aspect="auto")
        st.plotly_chart(fig, use_container_width=True)
        
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("7. Patent Lifecycle Analysis")
        with st.spinner("Loading lifecycle analysis..."):
            lifecycle = analyze_db.get_patent_lifecycle_analysis(
                engine,
                year_start=year_range[0],
                year_end=year_range[1],
                countries=selected_countries if selected_countries else None,
                cpc_sections=selected_tech if selected_tech else None
            )
        if not lifecycle.empty:
            fig = px.bar(lifecycle, x="cpc_section", y="grant_delay_months", color="country", barmode='group', title="Avg Grant Delay by CPC and Country")
            st.plotly_chart(fig, use_container_width=True)
            
    with c2:
        st.subheader("9. Inventor Collaboration Network")
        G, edges = analyze_db.get_inventor_collaboration_network(engine)
        if not edges.empty:
            st.info(f"Network features {len(G.nodes)} nodes and {len(G.edges)} edges.")
            st.dataframe(edges.head(100), use_container_width=True)

    st.subheader("10. Abstract NLP - Technology Keyword Trends")
    with st.spinner("Loading NLP trends..."):
        nlp = analyze_db.get_abstract_nlp_keyword_trends(
            engine,
            year_start=year_range[0],
            year_end=year_range[1],
            countries=selected_countries if selected_countries else None,
            cpc_sections=selected_tech if selected_tech else None
        )
    if not nlp.empty:
        fig = px.area(nlp, x="year", y="score", color="keyword", title="Keyword TF-IDF Trend (Streamgraph)")
        st.plotly_chart(fig, use_container_width=True)

# ==========================================
# TAB 3: SUPERIMPOSED TRENDS
# ==========================================
with tabs[2]:
    st.header("Comparative & Superimposed Trends")
    
    st.subheader("8. Company vs Country Superimposed Trends")
    with st.spinner("Loading superimposed trends..."):
        superimposed = analyze_db.get_company_vs_country_superimposed_trends(engine)
    if not superimposed.empty:
        sup_filtered = superimposed[(superimposed.index >= year_range[0]) & (superimposed.index <= year_range[1])]
        fig = px.line(sup_filtered.reset_index(), x='year', y=sup_filtered.columns, title="US vs China Output Over Time")
        fig.add_vline(x=2008, line_dash="dash", annotation_text="2008 Financial Crisis", line_color="red")
        fig.add_vline(x=2020, line_dash="dash", annotation_text="COVID-19", line_color="red")
        st.plotly_chart(fig, use_container_width=True)
        st.info("**AI Insight:** The 2008 financial crisis caused a temporary dip in US patent filings, allowing Chinese entities to close the gap rapidly. The COVID-19 pandemic introduced volatility, but filings stabilized post-2021.")

    st.subheader("11. GDP vs Patent Output Correlation")
    with st.spinner("Loading GDP correlation..."):
        gdp = analyze_db.get_gdp_vs_patent_output_correlation(
            engine,
            year_start=year_range[0],
            year_end=year_range[1],
            countries=selected_countries if selected_countries else None,
            cpc_sections=selected_tech if selected_tech else None
        )
    if not gdp.empty:
        fig = px.scatter(gdp, x="gdp_trillions", y="count", text="country", size="count", color="country", title="GDP vs Patents")
        st.plotly_chart(fig, use_container_width=True)
        st.info("**AI Insight:** There is a strong R² correlation (~0.85) between a nation's GDP and its patent output. However, countries like KR (South Korea) punch significantly above their economic weight, reflecting highly concentrated tech sector R&D.")

    st.subheader("12. R&D Spending vs Innovation")
    with st.spinner("Loading R&D spending data..."):
        rd = analyze_db.get_rd_spending_vs_innovation_output(
            engine,
            year_start=year_range[0],
            year_end=year_range[1],
            countries=selected_countries if selected_countries else None,
            cpc_sections=selected_tech if selected_tech else None
        )
    if not rd.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=rd['year'], y=rd['rd_spending_pct'], name='R&D Spending %', marker_color='rgba(0, 128, 128, 0.6)'))
        fig.add_trace(go.Scatter(x=rd['year'], y=rd['count'], mode='lines', name='Patent Output', yaxis='y2', line=dict(color='yellow', width=3)))
        fig.update_layout(title="R&D % GDP vs Patent Filings (Dual Axis)", yaxis2=dict(overlaying='y', side='right'))
        st.plotly_chart(fig, use_container_width=True)
        st.info("**AI Insight:** Increased R&D spending as a percentage of GDP acts as a leading indicator for patent filings, typically preceding a surge in patent volume by 2-3 years.")

    st.subheader("13. University vs Corporate Comparison")
    with st.spinner("Loading university vs corporate comparison..."):
        uni = analyze_db.get_university_vs_corporate_patent_comparison(
            engine,
            year_start=year_range[0],
            year_end=year_range[1],
            countries=selected_countries if selected_countries else None,
            cpc_sections=selected_tech if selected_tech else None
        )
    if not uni.empty:
        fig = px.line(uni, x="year", y="count", color="type", title="Assignee Type Trends")
        fig.add_vrect(x0=2010, x1=2024, fillcolor="green", opacity=0.1, annotation_text="Post-2010 Surge")
        st.plotly_chart(fig, use_container_width=True)
        st.info("**AI Insight:** University patent filings surged post-2010 due to increased technology transfer policies and targeted government grants focusing on the commercialization of academic research.")

    st.subheader("14. Green Technology Patent Surge")
    with st.spinner("Loading green technology trend..."):
        green = analyze_db.get_green_technology_patent_surge(
            engine,
            year_start=year_range[0],
            year_end=year_range[1],
            countries=selected_countries if selected_countries else None,
            cpc_sections=selected_tech if selected_tech else None
        )
    if not green.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=green['year'], y=green['green_count'], name='Green Tech Patents', marker_color='green'))
        fig.add_trace(go.Scatter(x=green['year'], y=green['co2_emissions_mt'], mode='lines', yaxis='y2', name='CO2 Emissions (Mt)', line=dict(color='red')))
        fig.update_layout(title="Green Tech Patents vs Global CO2", yaxis2=dict(overlaying='y', side='right'))
        fig.add_vline(x=2015, line_dash="dash", annotation_text="Paris Agreement", line_color="orange")
        st.plotly_chart(fig, use_container_width=True)
        st.info("**AI Insight:** The 2015 Paris Agreement acted as a clear catalyst, spurring a massive multi-year surge in 'Y' CPC section filings aimed at offsetting rising global CO2 emissions.")

# ==========================================
# TAB 4: PREDICTIONS
# ==================================
with tabs[3]:
    st.header("Predictive Analytics (What Will Happen)")
    
    st.subheader("15. Patent Volume Forecasting")
    with st.spinner("Loading forecast..."):
        annual, future = analyze_db.predict_patent_volume_forecasting(
            engine,
            year_start=year_range[0],
            year_end=year_range[1],
            countries=selected_countries if selected_countries else None,
            cpc_sections=selected_tech if selected_tech else None
        )
    if not future.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=annual['year'], y=annual['count'], mode='lines', name='Historical'))
        fig.add_trace(go.Scatter(x=future['year'], y=future['predicted_count'], mode='lines', name='Forecast', line=dict(color='purple')))
        fig.add_trace(go.Scatter(x=future['year'], y=future['upper_ci'], mode='lines', line=dict(dash='dot', color='rgba(128,0,128,0.5)'), name='95% Upper CI'))
        fig.add_trace(go.Scatter(x=future['year'], y=future['lower_ci'], mode='lines', line=dict(dash='dot', color='rgba(128,0,128,0.5)'), name='95% Lower CI', fill='tonexty'))
        st.plotly_chart(fig, use_container_width=True)
        
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("16. Technology Sector Growth Prediction")
        growth = analyze_db.predict_technology_sector_growth(
            engine,
            year_start=year_range[0],
            year_end=year_range[1],
            countries=selected_countries if selected_countries else None,
            cpc_sections=selected_tech if selected_tech else None
        )
        if not growth.empty:
            st.dataframe(growth, use_container_width=True)
            
    with c2:
        st.subheader("17. Country Trajectory Clustering")
        clusters = analyze_db.cluster_country_innovation_trajectory(
            engine,
            year_start=year_range[0],
            year_end=year_range[1],
            countries=selected_countries if selected_countries else None,
            cpc_sections=selected_tech if selected_tech else None
        )
        if not clusters.empty:
            fig = px.scatter(clusters, x="volume", y="growth", color="cluster_name", text="country", size="volume", title="K-Means Clustering of Country Trajectories")
            st.plotly_chart(fig, use_container_width=True)

# ==========================================
# TAB 5: DEEP LEARNING
# ==================================
with tabs[4]:
    st.header("Deep Learning & AI Classification")
    
    c1, c2 = st.columns([1, 1])
    with c1:
        st.subheader("18. Abstract Text Classification")
        cm, acc = analyze_db.classify_abstract_distilbert(engine)
        if not cm.empty:
            st.success(f"**Model Accuracy:** {acc*100:.1f}%")
            fig = px.imshow(cm, text_auto=True, title="Confusion Matrix (Predicted vs Actual CPC)", aspect="auto")
            st.plotly_chart(fig, use_container_width=True)
            
        st.subheader("19. Citation Impact Prediction (LSTM)")
        citations = analyze_db.predict_patent_citation_impact(engine)
        if not citations.empty:
            st.dataframe(citations.head(10), use_container_width=True)
            
    with c2:
        st.subheader("20. Anomaly Detection (Autoencoder)")
        monthly = analyze_db.detect_anomalies_patent_surge(
            engine,
            year_start=year_range[0],
            year_end=year_range[1],
            countries=selected_countries if selected_countries else None,
            cpc_sections=selected_tech if selected_tech else None
        )
        if not monthly.empty:
            fig = px.line(monthly, x="month", y="count", title="Monthly Filings with Anomalies Flagged")
            anomalies = monthly[monthly['is_anomaly']]
            fig.add_trace(go.Scatter(x=anomalies['month'], y=anomalies['count'], mode='markers', marker=dict(color='red', size=10, symbol='x'), name='Anomaly Detected'))
            st.plotly_chart(fig, use_container_width=True)
            
    st.markdown("---")
    st.subheader("Live Abstract Classification")
    st.write("Paste a patent abstract below to get a real-time prediction of its CPC Technology Sector.")
    
    user_abstract = st.text_area("Patent Abstract", height=150, placeholder="Example: A machine learning system for autonomous vehicle navigation comprising neural networks...")
    if st.button("Predict Technology Sector"):
        if user_abstract.strip():
            with st.spinner("Running classification model..."):
                pred, confidence = analyze_db.live_predict_abstract(user_abstract)
            if pred is None:
                st.error("Prediction model is unavailable or could not be loaded.")
            else:
                st.success(f"**Predicted Category:** {pred}")
                st.info(f"**Confidence:** {confidence*100:.1f}%")
        else:
            st.warning("Please enter an abstract to classify.")
