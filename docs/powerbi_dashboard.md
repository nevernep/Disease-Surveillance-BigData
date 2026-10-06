# Power BI Dashboard

## 1. Data source

After running the Spark pipeline, use the files under
`data/processed/warehouse/`:

- `dim_date.csv`
- `dim_district.csv`
- `dim_disease.csv`
- `fact_disease_cases.csv`

For production, load the Parquet directories into a warehouse or lakehouse
and expose the same four tables to Power BI. The CSV files are intended for
local development and validation.

## 2. Model relationships

Create these single-direction, many-to-one relationships:

| From | To |
|---|---|
| `fact_disease_cases[date_key]` | `dim_date[date_key]` |
| `fact_disease_cases[district_key]` | `dim_district[district_key]` |
| `fact_disease_cases[disease_key]` | `dim_disease[disease_key]` |

Mark `dim_date` as the date table only when a real date column is added to the
source grain. The current model is annual, so use `year_be` as the year axis.

## 3. DAX measures

```DAX
Total Cases = SUM(fact_disease_cases[total_cases])

Source Records = SUM(fact_disease_cases[source_records])

Population = SUM(fact_disease_cases[population])

Incidence Rate per 100k =
DIVIDE([Total Cases] * 100000, [Population])

District Rank =
RANKX(
    ALLSELECTED(dim_district[district_name]),
    [Total Cases],
    ,
    DESC,
    Dense
)
```

`Population` should be used with care: the fact is at year-district-disease
grain, so summing it across diseases repeats a district population. Use the
incidence measure for disease comparisons, or create a separate district-year
population fact if a population KPI is needed across diseases.

## 4. Recommended report pages

1. **Overview**: Cards for Total Cases, Incidence Rate, and Source Records;
   line chart by `year_be`; bar chart by disease.
2. **District surveillance**: ranked bar chart by district, map only when a
   trusted district geography is available, and a year slicer.
3. **Disease profile**: disease slicer, district comparison, and a matrix of
   year, disease, total cases, and incidence rate.
4. **Data quality**: import `data/processed/quality/data_quality_report.csv`
   and show failed rules, coverage, and missing population rows.

Always display a note that the incidence rate is unavailable where population
coverage is missing. The development sample is not representative of full
surveillance statistics.

To build a cases-only dashboard before population data is available, run:

```powershell
python -m spark.main --project-root . --allow-missing-population
```

This creates the same warehouse tables. `total_cases`, disease, district, and
year are available; `population` and `incidence_rate_per_100k` remain blank.
Do not publish an incidence-rate visual until official population data has
been loaded and the pipeline has been run again without this option.
