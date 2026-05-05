# Big Data Pipeline - Patent Intelligence

This pipeline processes 8+ million patent records from USPTO TSV files, handles massive dataset constraints (optimized to run on 8GB RAM laptops via chunking), loads the data into a MySQL database via SQLAlchemy, and serves an interactive 20-analytics Streamlit dashboard.

## Setup Instructions

1. **Install Dependencies**:
   Ensure you have Python 3.9+ installed. Run:
   ```bash
   pip install -r requirements.txt
   ```

2. **Prepare Data**:
   Ensure your USPTO `.tsv` files are extracted and placed in the root directory under their respective folders:
   - `g_patent.tsv (1)/g_patent.tsv`
   - `g_inventor_disambiguated.tsv (1)/g_inventor_disambiguated.tsv`
   - `g_assignee_disambiguated.tsv (2)/g_assignee_disambiguated.tsv`
   - `g_patent_abstract.tsv/g_patent_abstract.tsv`
   - `g_cpc_current.tsv/g_cpc_current.tsv`

3. **Start MySQL Database**:
   Ensure MySQL is running locally on `localhost` with user `root` and no password. 
   If your database is hosted elsewhere, set the following environment variables:
   - `PATENTS_DB_HOST`
   - `PATENTS_DB_USER`
   - `PATENTS_DB_PASSWORD`
   - `PATENTS_DB_NAME` (Defaults to `patents_db`)

4. **Run the Full Pipeline**:
   The entire ETL pipeline, model predictions (Prophet, K-Means), and report generation is completely reproducible and optimized. Just run:
   ```bash
   python pipeline.py
   ```
   This will automatically:
   - Create the database and schema
   - Extract raw TSV data in 100k chunks
   - Clean, format, and deduplicate entities into CSVs
   - Execute bulk inserts into MySQL using SQLAlchemy
   - Export CSV and JSON machine learning reports (Prophet, etc.) to the `outputs/` folder.

5. **Start the Dashboard**:
   Once the pipeline finishes, fire up the dark-themed analytics UI:
   ```bash
   streamlit run dashboard.py
   ```

## Key Technologies Used
- **Database & Modeling**: SQLAlchemy, Scikit-Learn, Prophet, PyTorch, Transformers (DistilBERT).
- **Data Engineering**: Pandas (Chunked operations).
- **Visualization**: Streamlit, Plotly, NetworkX.
