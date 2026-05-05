-- ============================================================
-- schema.sql  –  patents_db
-- Global Patent Intelligence Data Pipeline
-- ============================================================

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

-- ------------------------------------------------------------
-- 6. g_abstract
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS g_abstract (
    patent_id VARCHAR(32) PRIMARY KEY,
    abstract_text LONGTEXT,
    FOREIGN KEY (patent_id) REFERENCES patents (patent_id) ON DELETE CASCADE
);
