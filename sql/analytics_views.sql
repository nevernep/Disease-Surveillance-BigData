-- Semantic layer used by Power BI. Applied automatically by
-- spark/load_warehouse.py after sql/ddl.sql and the table loads.
--
-- Incidence rules:
--   * the denominator comes from mart.fact_population (every district in
--     scope), never from districts that happened to report a case;
--   * a Bangkok-wide rate is shown only when all Bangkok districts have
--     population for that year; otherwise it is NULL (no partial denominators);
--   * rates are not split by age group or sex: population by age/sex is not loaded;
--   * yearly rates are CUMULATIVE over the months that have data in that year
--     (months_covered / period_label), not annual rates: compare years only
--     over matching months.

-- Drop first so a view's columns can change shape (CREATE OR REPLACE cannot).
DROP VIEW IF EXISTS
    mart.vw_disease_overview,
    mart.vw_monthly_trend,
    mart.vw_year_disease,
    mart.vw_district_disease_year,
    mart.vw_year_period,
    mart.vw_district_ranking,
    mart.vw_demographics;

-- Case-level analytical view (all dimensions resolved to labels).
CREATE OR REPLACE VIEW mart.vw_disease_overview AS
SELECT
    d.month_start,
    d.year_be,
    d.year_ce,
    d.quarter,
    d.month,
    d.month_label,
    dst.district_name,
    dse.disease_name,
    ag.age_group_key,
    ag.age_group,
    sx.sex,
    sx.sex_label,
    f.total_cases,
    f.source_records
FROM mart.fact_disease_cases AS f
JOIN mart.dim_date      AS d   ON d.date_key = f.date_key
JOIN mart.dim_district  AS dst ON dst.district_key = f.district_key
JOIN mart.dim_disease   AS dse ON dse.disease_key = f.disease_key
JOIN mart.dim_age_group AS ag  ON ag.age_group_key = f.age_group_key
JOIN mart.dim_sex       AS sx  ON sx.sex = f.sex;

-- Monthly trend per disease, zero-filled for months without cases.
CREATE OR REPLACE VIEW mart.vw_monthly_trend AS
SELECT
    d.month_start,
    d.month_label,
    d.year_be,
    dse.disease_name,
    COALESCE(SUM(f.total_cases), 0) AS total_cases
FROM mart.dim_date AS d
CROSS JOIN mart.dim_disease AS dse
LEFT JOIN mart.fact_disease_cases AS f
    ON f.date_key = d.date_key
   AND f.disease_key = dse.disease_key
GROUP BY d.month_start, d.month_label, d.year_be, dse.disease_name;

-- Months with data per year (dim_date runs from the first to the last onset month).
CREATE OR REPLACE VIEW mart.vw_year_period AS
SELECT
    year_be,
    COUNT(*) AS months_covered,
    MIN(month_label) FILTER (WHERE month_start = first_month)
        || ' – ' ||
    MIN(month_label) FILTER (WHERE month_start = last_month) AS period_label
FROM (
    SELECT d.*,
           MIN(month_start) OVER (PARTITION BY year_be) AS first_month,
           MAX(month_start) OVER (PARTITION BY year_be) AS last_month
    FROM mart.dim_date AS d
) AS months
GROUP BY year_be;

-- Bangkok-wide cumulative cases and incidence per disease and year.
CREATE OR REPLACE VIEW mart.vw_year_disease AS
WITH cases AS (
    SELECT d.year_be, f.disease_key,
           SUM(f.total_cases) AS total_cases,
           SUM(f.source_records) AS source_records
    FROM mart.fact_disease_cases AS f
    JOIN mart.dim_date AS d ON d.date_key = f.date_key
    GROUP BY d.year_be, f.disease_key
),
population AS (
    SELECT p.year_be,
           SUM(p.population) AS population,
           COUNT(*) FILTER (WHERE dst.is_bangkok_district) AS districts_with_population
    FROM mart.fact_population AS p
    JOIN mart.dim_district AS dst ON dst.district_key = p.district_key
    GROUP BY p.year_be
),
bangkok AS (
    SELECT COUNT(*) AS districts FROM mart.dim_district WHERE is_bangkok_district
)
SELECT
    c.year_be,
    c.year_be - 543 AS year_ce,
    yp.months_covered,
    yp.period_label,
    dse.disease_name,
    c.total_cases,
    c.source_records,
    p.population,
    COALESCE(p.districts_with_population, 0) AS districts_with_population,
    CASE
        WHEN p.districts_with_population = b.districts AND p.population > 0
        THEN ROUND(c.total_cases * 100000.0 / p.population, 2)
    END AS cumulative_incidence_per_100k
FROM cases AS c
JOIN mart.dim_disease AS dse ON dse.disease_key = c.disease_key
JOIN mart.vw_year_period AS yp ON yp.year_be = c.year_be
CROSS JOIN bangkok AS b
LEFT JOIN population AS p ON p.year_be = c.year_be;

-- Cumulative cases and incidence per year, district and disease (district denominator).
CREATE OR REPLACE VIEW mart.vw_district_disease_year AS
SELECT
    d.year_be,
    yp.months_covered,
    yp.period_label,
    dst.district_name,
    dse.disease_name,
    SUM(f.total_cases) AS total_cases,
    p.population,
    CASE
        WHEN p.population > 0
        THEN ROUND(SUM(f.total_cases) * 100000.0 / p.population, 2)
    END AS cumulative_incidence_per_100k
FROM mart.fact_disease_cases AS f
JOIN mart.dim_date     AS d   ON d.date_key = f.date_key
JOIN mart.vw_year_period AS yp ON yp.year_be = d.year_be
JOIN mart.dim_district AS dst ON dst.district_key = f.district_key
JOIN mart.dim_disease  AS dse ON dse.disease_key = f.disease_key
LEFT JOIN mart.fact_population AS p
    ON p.year_be = d.year_be
   AND p.district_key = f.district_key
GROUP BY d.year_be, yp.months_covered, yp.period_label,
         dst.district_name, dse.disease_name, p.population;

CREATE OR REPLACE VIEW mart.vw_district_ranking AS
SELECT
    d.year_be,
    dst.district_name,
    SUM(f.total_cases) AS total_cases,
    SUM(f.source_records) AS source_records,
    RANK() OVER (
        PARTITION BY d.year_be ORDER BY SUM(f.total_cases) DESC
    ) AS cases_rank
FROM mart.fact_disease_cases AS f
JOIN mart.dim_date     AS d   ON d.date_key = f.date_key
JOIN mart.dim_district AS dst ON dst.district_key = f.district_key
GROUP BY d.year_be, dst.district_name;

-- Age/sex distribution: counts and share within each year × disease.
CREATE OR REPLACE VIEW mart.vw_demographics AS
SELECT
    d.year_be,
    dse.disease_name,
    ag.age_group_key,
    ag.age_group,
    sx.sex_label,
    SUM(f.total_cases) AS total_cases,
    ROUND(
        100.0 * SUM(f.total_cases)
        / SUM(SUM(f.total_cases)) OVER (PARTITION BY d.year_be, dse.disease_name),
        2
    ) AS share_pct
FROM mart.fact_disease_cases AS f
JOIN mart.dim_date      AS d   ON d.date_key = f.date_key
JOIN mart.dim_disease   AS dse ON dse.disease_key = f.disease_key
JOIN mart.dim_age_group AS ag  ON ag.age_group_key = f.age_group_key
JOIN mart.dim_sex       AS sx  ON sx.sex = f.sex
GROUP BY d.year_be, dse.disease_name, ag.age_group_key, ag.age_group, sx.sex_label;
