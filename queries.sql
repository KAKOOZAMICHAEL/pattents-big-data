-- ============================================================
-- queries.sql  –  patents_db  Analytics Queries
-- Global Patent Intelligence Data Pipeline
-- ============================================================

USE patents_db;

-- ------------------------------------------------------------
-- Q1: Top Inventors  –  Who has the most patents?
-- ------------------------------------------------------------
SELECT
    i.full_name,
    i.country,
    COUNT(DISTINCT pi.patent_id) AS patent_count
FROM inventors i
JOIN patent_inventors pi ON i.inventor_id = pi.inventor_id
GROUP BY i.inventor_id, i.full_name, i.country
ORDER BY patent_count DESC
LIMIT 10;


-- ------------------------------------------------------------
-- Q2: Top Companies  –  Which companies own the most patents?
-- ------------------------------------------------------------
SELECT
    c.company_name,
    COUNT(DISTINCT pc.patent_id) AS patent_count
FROM companies c
LEFT JOIN patent_companies pc ON c.company_id = pc.company_id
GROUP BY c.company_id, c.company_name
ORDER BY patent_count DESC
LIMIT 10;


-- ------------------------------------------------------------
-- Q3: Countries  –  Which countries produce the most patents?
-- ------------------------------------------------------------
SELECT
    i.country,
    COUNT(DISTINCT pi.patent_id) AS patent_count
FROM inventors i
JOIN patent_inventors pi ON i.inventor_id = pi.inventor_id
GROUP BY i.country
ORDER BY patent_count DESC;


-- ------------------------------------------------------------
-- Q4: Trends Over Time  –  How many patents are created each year?
-- ------------------------------------------------------------
SELECT
    YEAR(filing_date)  AS filing_year,
    COUNT(*)           AS patent_count
FROM patents
WHERE filing_date IS NOT NULL
GROUP BY YEAR(filing_date)
ORDER BY filing_year;


-- ------------------------------------------------------------
-- Q5: JOIN Query  –  Combine patents with inventors and companies
-- ------------------------------------------------------------
SELECT
    p.patent_id,
    p.title,
    p.filing_date,
    p.main_classification,
    i.full_name      AS inventor_name,
    i.country        AS inventor_country,
    c.company_name
FROM patents p
LEFT JOIN patent_inventors pi ON p.patent_id  = pi.patent_id
LEFT JOIN inventors        i  ON pi.inventor_id = i.inventor_id
LEFT JOIN patent_companies pc ON p.patent_id  = pc.patent_id
LEFT JOIN companies        c  ON pc.company_id = c.company_id
ORDER BY p.filing_date DESC
LIMIT 100;


-- ------------------------------------------------------------
-- Q6: CTE Query  –  Patents with inventor count and company flag
--     (breaks a complex query into readable steps)
-- ------------------------------------------------------------
WITH inventor_counts AS (
    -- Step 1: count how many inventors each patent has
    SELECT
        patent_id,
        COUNT(inventor_id) AS num_inventors
    FROM patent_inventors
    GROUP BY patent_id
),
company_flags AS (
    -- Step 2: flag whether a patent has at least one company
    SELECT
        patent_id,
        CASE WHEN COUNT(company_id) > 0 THEN 'Yes' ELSE 'No' END AS has_company
    FROM patent_companies
    GROUP BY patent_id
),
patent_summary AS (
    -- Step 3: join everything onto the patents table
    SELECT
        p.patent_id,
        p.title,
        YEAR(p.filing_date)          AS filing_year,
        p.main_classification,
        COALESCE(ic.num_inventors, 0) AS num_inventors,
        COALESCE(cf.has_company, 'No') AS has_company
    FROM patents p
    LEFT JOIN inventor_counts ic ON p.patent_id = ic.patent_id
    LEFT JOIN company_flags   cf ON p.patent_id = cf.patent_id
)
-- Final result: summarise by year
SELECT
    filing_year,
    COUNT(*)                                         AS total_patents,
    ROUND(AVG(num_inventors), 2)                     AS avg_inventors,
    SUM(CASE WHEN has_company = 'Yes' THEN 1 ELSE 0 END) AS patents_with_company,
    SUM(CASE WHEN has_company = 'No'  THEN 1 ELSE 0 END) AS patents_without_company
FROM patent_summary
GROUP BY filing_year
ORDER BY filing_year;


-- ------------------------------------------------------------
-- Q7: Ranking Query  –  Rank inventors using window functions
-- ------------------------------------------------------------
SELECT
    inventor_rank,
    full_name,
    country,
    patent_count,
    country_rank
FROM (
    SELECT
        i.full_name,
        i.country,
        COUNT(DISTINCT pi.patent_id)                              AS patent_count,
        -- Global rank across all inventors
        RANK()       OVER (ORDER BY COUNT(DISTINCT pi.patent_id) DESC) AS inventor_rank,
        -- Rank within each country
        RANK()       OVER (
            PARTITION BY i.country
            ORDER BY COUNT(DISTINCT pi.patent_id) DESC
        )                                                          AS country_rank,
        -- Running total of patents as we go down the ranked list
        SUM(COUNT(DISTINCT pi.patent_id)) OVER (
            ORDER BY COUNT(DISTINCT pi.patent_id) DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                          AS running_total
    FROM inventors i
    JOIN patent_inventors pi ON i.inventor_id = pi.inventor_id
    GROUP BY i.inventor_id, i.full_name, i.country
) ranked
ORDER BY inventor_rank
LIMIT 20;
