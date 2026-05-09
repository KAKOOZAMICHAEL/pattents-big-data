-- ============================================================
-- schema.sql  –  patents_db
-- Global Patent Intelligence Data Pipeline
-- ============================================================

-- Performance settings for bulk inserts
SET innodb_buffer_pool_size = 1G;
SET bulk_insert_buffer_size = 256M;

CREATE DATABASE IF NOT EXISTS patents_db;
USE patents_db;

-- ------------------------------------------------------------
-- 1. patents
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS patents (
    patent_id            VARCHAR(32)  PRIMARY KEY,
    title                TEXT         NOT NULL,
    description          TEXT,
    filing_date          DATE         NULL,
    publication_date     DATE         NULL,
    main_classification  VARCHAR(100),
    locarno_classification VARCHAR(100),
    cpc_section          VARCHAR(10)
);

CREATE INDEX IF NOT EXISTS idx_patents_filing_date ON patents (filing_date);
CREATE INDEX IF NOT EXISTS idx_patents_cpc_section ON patents (cpc_section);
CREATE INDEX IF NOT EXISTS idx_patents_filing_cpc ON patents (filing_date, cpc_section);
CREATE INDEX IF NOT EXISTS idx_patents_id ON patents (patent_id);

-- ------------------------------------------------------------
-- 2. inventors
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventors (
    inventor_id  VARCHAR(64)  PRIMARY KEY,
    full_name    VARCHAR(500) NOT NULL,
    country      VARCHAR(100) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_inventors_country ON inventors (country);

-- ------------------------------------------------------------
-- 3. companies  (assignees)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS companies (
    company_id   VARCHAR(64)  PRIMARY KEY,
    company_name VARCHAR(500) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_companies_name ON companies (company_name);

-- ------------------------------------------------------------
-- 4. patent_inventors  (many-to-many link)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS patent_inventors (
    patent_id   VARCHAR(32) NOT NULL,
    inventor_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (patent_id, inventor_id),
    FOREIGN KEY (patent_id)   REFERENCES patents   (patent_id)   ON DELETE CASCADE,
    FOREIGN KEY (inventor_id) REFERENCES inventors (inventor_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_patent_inventors_inv_pat ON patent_inventors (inventor_id, patent_id);

-- ------------------------------------------------------------
-- 5. patent_companies  (many-to-many link)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS patent_companies (
    patent_id  VARCHAR(32) NOT NULL,
    company_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (patent_id, company_id),
    FOREIGN KEY (patent_id)  REFERENCES patents   (patent_id)  ON DELETE CASCADE,
    FOREIGN KEY (company_id) REFERENCES companies (company_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_patent_companies_comp_pat ON patent_companies (company_id, patent_id);

-- ------------------------------------------------------------
-- 6. g_abstract
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g_abstract (
    patent_id VARCHAR(32) PRIMARY KEY,
    abstract_text LONGTEXT,
    FOREIGN KEY (patent_id) REFERENCES patents (patent_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_g_abstract_patent_id ON g_abstract (patent_id);

-- ------------------------------------------------------------
-- 7. patent_yearly_summary
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS patent_yearly_summary (
    year INT NOT NULL,
    cpc_section VARCHAR(10) NOT NULL,
    country VARCHAR(100) NOT NULL,
    count INT NOT NULL,
    PRIMARY KEY (year, cpc_section, country)
);

CREATE INDEX IF NOT EXISTS idx_patent_yearly_summary_year ON patent_yearly_summary (year);
CREATE INDEX IF NOT EXISTS idx_patent_yearly_summary_country ON patent_yearly_summary (country);
CREATE INDEX IF NOT EXISTS idx_patent_yearly_summary_cpc ON patent_yearly_summary (cpc_section);

-- ------------------------------------------------------------
-- 8. company_yearly_summary
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS company_yearly_summary (
    year INT NOT NULL,
    company_id VARCHAR(64) NOT NULL,
    count INT NOT NULL,
    type VARCHAR(50) NOT NULL,
    PRIMARY KEY (year, company_id)
);

CREATE INDEX IF NOT EXISTS idx_company_yearly_summary_company_id ON company_yearly_summary (company_id);
CREATE INDEX IF NOT EXISTS idx_company_yearly_summary_type ON company_yearly_summary (type);

-- ------------------------------------------------------------
-- 9. monthly_volume_summary
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monthly_volume_summary (
    month DATE PRIMARY KEY,
    count INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_monthly_volume_summary_month ON monthly_volume_summary (month);
