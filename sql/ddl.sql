-- Data Warehouse DDL (PostgreSQL) for the disease surveillance star schema.
--
-- Every table below except load_audit is derived data rebuilt by Spark on each
-- run, so the loader DROPs and re-CREATEs them inside its single transaction.
-- This keeps the schema in step with the code without separate migrations, and
-- a failed load rolls back to the previous tables (PostgreSQL DDL is transactional).

CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS
    mart.fact_disease_cases,
    mart.fact_population,
    mart.dim_date,
    mart.dim_district,
    mart.dim_disease,
    mart.dim_age_group,
    mart.dim_sex
CASCADE;  -- also drops the analytics views; the loader recreates them

-- Monthly calendar; months without cases are present so trends show zeros.
CREATE TABLE mart.dim_date (
    date_key     INTEGER  PRIMARY KEY,           -- yyyymm (Gregorian), stable across runs
    month_start  DATE     NOT NULL UNIQUE,
    year_be      INTEGER  NOT NULL,
    year_ce      INTEGER  NOT NULL,
    quarter      SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month        SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_label  TEXT     NOT NULL,
    CHECK (year_ce = year_be - 543),
    CHECK (date_key = year_ce * 100 + month)
);

-- All 50 Bangkok districts are always present (even with zero cases).
CREATE TABLE mart.dim_district (
    district_key         INTEGER PRIMARY KEY,
    district_name        TEXT    NOT NULL UNIQUE,
    is_bangkok_district  BOOLEAN NOT NULL
);

CREATE TABLE mart.dim_disease (
    disease_key   INTEGER PRIMARY KEY,
    disease_name  TEXT    NOT NULL UNIQUE
);

CREATE TABLE mart.dim_age_group (
    age_group_key  INTEGER PRIMARY KEY,
    age_group      TEXT    NOT NULL UNIQUE,
    min_age        INTEGER,
    max_age        INTEGER
);

CREATE TABLE mart.dim_sex (
    sex        CHAR(1) PRIMARY KEY CHECK (sex IN ('M', 'F', 'U')),
    sex_label  TEXT    NOT NULL
);

-- Grain: month × district × disease × age group × sex.
CREATE TABLE mart.fact_disease_cases (
    date_key        INTEGER NOT NULL REFERENCES mart.dim_date (date_key),
    district_key    INTEGER NOT NULL REFERENCES mart.dim_district (district_key),
    disease_key     INTEGER NOT NULL REFERENCES mart.dim_disease (disease_key),
    age_group_key   INTEGER NOT NULL REFERENCES mart.dim_age_group (age_group_key),
    sex             CHAR(1) NOT NULL REFERENCES mart.dim_sex (sex),
    total_cases     BIGINT  NOT NULL CHECK (total_cases > 0),
    source_records  BIGINT  NOT NULL CHECK (source_records > 0),
    PRIMARY KEY (date_key, district_key, disease_key, age_group_key, sex)
);

CREATE INDEX ix_fact_cases_district ON mart.fact_disease_cases (district_key);
CREATE INDEX ix_fact_cases_disease  ON mart.fact_disease_cases (disease_key);

-- Incidence denominator. Grain: year × district. Kept separate so disease,
-- age or sex filters never shrink the denominator to districts with cases.
-- Empty in cases-only mode.
CREATE TABLE mart.fact_population (
    year_be       INTEGER NOT NULL,
    district_key  INTEGER NOT NULL REFERENCES mart.dim_district (district_key),
    population    BIGINT  NOT NULL CHECK (population > 0),
    PRIMARY KEY (year_be, district_key)
);

-- One row per successful load, for lineage and troubleshooting (never dropped).
CREATE TABLE IF NOT EXISTS mart.load_audit (
    load_id     BIGSERIAL   PRIMARY KEY,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    source      TEXT        NOT NULL,
    row_counts  JSONB       NOT NULL
);
