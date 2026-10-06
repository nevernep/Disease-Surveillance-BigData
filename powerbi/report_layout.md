# Power BI Report Layout

Import `powerbi/theme.json` first, then load the four CSV files from
`data/processed/warehouse/`. Create the relationships described in
`docs/powerbi_dashboard.md` and paste the measures from `measures.dax`.

## Page 1: Overview

- Card: `Total Cases`
- Card: `Incidence Rate per 100k`
- Card: `Population Coverage Status`
- Line chart: `dim_date[year_be]` by `Total Cases`
- Clustered bar chart: `dim_disease[disease_name]` by `Total Cases`
- Slicers: year and disease

## Page 2: District Surveillance

- Bar chart: `dim_district[district_name]` by `Total Cases`, sorted descending
- Table: district, total cases, incidence rate, district rank
- Slicers: year and disease

## Page 3: Disease Comparison

- Matrix: year by disease with total cases and incidence rate
- Line chart: year by disease with total cases as values
- Slicer: district

## Page 4: Data Quality

Import `data/processed/quality/data_quality_report.csv` as a separate table.
Show rule, observed, threshold, and status in a table, with a filter for
`status = FAIL`. Add a card showing the count of failed rules.

The development sample is not representative of full surveillance statistics.
Show this as a report subtitle or information banner.