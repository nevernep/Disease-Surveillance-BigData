# Power BI Report Layout

Import `powerbi/theme.json` first, then connect to the PostgreSQL warehouse
(`localhost:5433`, database `surveillance_dw`, schema `mart`) as described in
`docs/powerbi_dashboard.md`. Create the relationships listed there, mark
`dim_date[month_start]` as the date table, and paste the measures from
`measures.dax`.

## Page 1: Overview

- Cards: `Total Cases`, `Incidence Rate per 100k`, `Population Coverage Status`
- Line chart: `dim_date[month_start]` (monthly) by `Total Cases`, legend `dim_disease[disease_name]`
- Clustered bar chart: `dim_disease[disease_name]` by `Total Cases`
- Slicers: `dim_date[year_be]`, disease

## Page 2: Trend

- Line chart: `dim_date[month_start]` by `Total Cases` for the selected disease
- Card or KPI: `Month over Month Cases %`, `Year over Year Cases %`
- Slicers: disease, district

## Page 3: District Surveillance

- Bar chart: `dim_district[district_name]` by `Total Cases`, sorted descending
- Table: district, `Total Cases`, `Incidence Rate per 100k`, `District Rank`
- Slicers: year, disease
- Map only when a trusted district geography is available

## Page 4: Demographics

- Stacked bar: `dim_age_group[age_group]` (sort by `age_group_key`) by `Total Cases`,
  legend `dim_sex[sex_label]`
- Matrix: age group × sex with `Case Share %`
- Note: incidence by age/sex is intentionally blank (no age/sex population)
- Slicers: year, disease

## Page 5: Data Quality

Import `data/processed/quality/data_quality_report.csv` as a separate table.
Show rule, observed, threshold, and status in a table, with a filter for
`status = FAIL`. Add a card showing the count of failed rules. `SKIP` means the
rule does not apply in the current mode (for example, population checks in
cases-only mode). Add `mart.load_audit` to show the last load time.

The development sample is not representative of full surveillance statistics.
Show this as a report subtitle or information banner.
