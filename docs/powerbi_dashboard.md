# Power BI Dashboard

## 1. Data source

The `spark_processing` DAG loads the star schema into the PostgreSQL Data
Warehouse (`load_warehouse` task) after every successful Spark run. Connect
Power BI to it:

| Setting | Value |
|---|---|
| Connector | Get Data → PostgreSQL database |
| Server | `localhost:5433` |
| Database | `surveillance_dw` |
| Credentials | Database: `dw_user` / `warehouse-local-only` (from `.env`) |
| Tables | `mart.dim_date`, `mart.dim_district`, `mart.dim_disease`, `mart.dim_age_group`, `mart.dim_sex`, `mart.fact_disease_cases`, `mart.fact_population` |
| Optional views | `mart.vw_monthly_trend`, `mart.vw_year_disease`, `mart.vw_district_disease_year`, `mart.vw_district_ranking`, `mart.vw_demographics`, `mart.vw_disease_overview` |

Use **Import** mode for this dataset size. Refresh in Power BI after the DAG
finishes. `mart.load_audit` records when each load happened and its row counts.
If Power BI reports an SSL error, untick "Encrypt connections" in Data source
settings (the local database has no TLS).

The loader runs in a single transaction (DDL → COPY → row-count check → views
→ audit), so a failed load leaves the previous data intact. To reload manually
from the host: `python -m spark.load_warehouse --project-root .`

Fallback without the database: the same tables are also exported as CSV under
`data/processed/warehouse/`.

## 2. Model

| Table | Grain | Notes |
|---|---|---|
| `fact_disease_cases` | month × district × disease × age group × sex | `total_cases`, `source_records` |
| `fact_population` | year × district | incidence denominator; empty in cases-only mode |
| `dim_date` | month | `date_key` = yyyymm, `month_start` is a real date; zero-case months included |
| `dim_district` | district | all 50 Bangkok districts, even with zero cases |
| `dim_disease` | standardized disease | |
| `dim_age_group` | age band | 0-4, 5-9, 10-14, 15-24 … 65+, ไม่ระบุ |
| `dim_sex` | M / F / U | `sex_label` in Thai |

Create these single-direction, many-to-one relationships:

| From | To |
|---|---|
| `fact_disease_cases[date_key]` | `dim_date[date_key]` |
| `fact_disease_cases[district_key]` | `dim_district[district_key]` |
| `fact_disease_cases[disease_key]` | `dim_disease[disease_key]` |
| `fact_disease_cases[age_group_key]` | `dim_age_group[age_group_key]` |
| `fact_disease_cases[sex]` | `dim_sex[sex]` |
| `fact_population[district_key]` | `dim_district[district_key]` |

Do **not** relate `fact_population` to `dim_date`, `dim_disease`,
`dim_age_group` or `dim_sex`: the `Population` measure applies the year with
`TREATAS`, and the other filters must not shrink the denominator.

Mark `dim_date` as a date table using `dim_date[month_start]` so the
month-over-month and year-over-year measures work.

## 3. DAX measures

All measures are in [`powerbi/measures.dax`](../powerbi/measures.dax):
`Total Cases`, `Population`, `Incidence Rate per 100k`, `Population Coverage %`,
`Population Coverage Status`, `Case Share %`, `District Rank`, and month-over-month
and year-over-year changes.

`Incidence Rate per 100k` returns blank when the report is sliced by age group
or sex, because population by age/sex is not loaded. Use `Case Share %` for
age/sex breakdowns.

## 4. Recommended report pages

See [`powerbi/report_layout.md`](../powerbi/report_layout.md).

Always display a note that the incidence rate is unavailable where population
coverage is missing. The development sample is not representative of full
surveillance statistics.

Cases-only mode (`REQUIRE_POPULATION=false`, the Compose default) builds every
table; `fact_population` stays empty and incidence measures return blank. Do not
publish an incidence-rate visual until official population data has been
loaded and the pipeline has been run again.
