-- Load the four CSV exports from data/processed/warehouse into PostgreSQL first.
-- These views are the semantic layer used by Power BI.

CREATE OR REPLACE VIEW mart.vw_disease_overview AS
SELECT
    dd.year_be,
    dd.year_ce,
    dd.year_label,
    dst.district_name,
    dse.disease_name,
    f.total_cases,
    f.population,
    f.incidence_rate_per_100k,
    f.source_records
FROM mart.fact_disease_cases AS f
JOIN mart.dim_date AS dd ON dd.date_key = f.date_key
JOIN mart.dim_district AS dst ON dst.district_key = f.district_key
JOIN mart.dim_disease AS dse ON dse.disease_key = f.disease_key;

CREATE OR REPLACE VIEW mart.vw_year_disease AS
SELECT
    year_be,
    year_ce,
    disease_name,
    SUM(total_cases) AS total_cases,
    SUM(source_records) AS source_records,
    SUM(population) AS population_sum,
    CASE
        WHEN SUM(population) > 0
        THEN ROUND(SUM(total_cases) * 100000.0 / SUM(population), 2)
    END AS incidence_rate_per_100k
FROM mart.vw_disease_overview
GROUP BY year_be, year_ce, disease_name;

CREATE OR REPLACE VIEW mart.vw_district_ranking AS
SELECT
    year_be,
    district_name,
    SUM(total_cases) AS total_cases,
    SUM(source_records) AS source_records,
    RANK() OVER (
        PARTITION BY year_be ORDER BY SUM(total_cases) DESC
    ) AS cases_rank
FROM mart.vw_disease_overview
GROUP BY year_be, district_name;
