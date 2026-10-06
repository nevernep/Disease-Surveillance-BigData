from datetime import date

from spark.schemas import BANGKOK_DISTRICTS
from spark.warehouse import build_warehouse_tables


def _standardized(spark):
    return spark.createDataFrame(
        [
            ("C1", date(2025, 1, 6), 2568, "บางกะปิ", "ไข้เลือดออก", 1.0, "M", 3),
            ("C2", date(2025, 1, 20), 2568, "บางกะปิ", "ไข้เลือดออก", 1.0, "M", 3),
            ("C3", date(2025, 3, 2), 2568, "ทุ่งครุ", "ไข้หวัดใหญ่", 2.0, "F", 9),
        ],
        "case_id string, report_date date, year_be int, district_name string, "
        "disease_name string, case_count double, sex string, age_group_key int",
    )


def _population(spark):
    return spark.createDataFrame(
        [(2568, "บางกะปิ", 140000.0), (2568, "ดุสิต", 90000.0)],
        "year_be int, district_name string, population double",
    )


def test_fact_grain_and_totals(spark):
    tables = build_warehouse_tables(_standardized(spark), _population(spark))
    fact = tables["fact_disease_cases"]

    # C1 and C2 share month/district/disease/age/sex -> one row.
    assert fact.count() == 2
    assert fact.agg({"total_cases": "sum"}).first()[0] == 4
    row = fact.filter("date_key = 202501").first()
    assert (row["total_cases"], row["source_records"]) == (2, 2)


def test_dim_date_is_zero_filled_monthly_calendar(spark):
    tables = build_warehouse_tables(_standardized(spark), _population(spark))
    dim_date = tables["dim_date"].orderBy("date_key").collect()

    # January..March even though February has no cases.
    assert [row["date_key"] for row in dim_date] == [202501, 202502, 202503]
    assert dim_date[0]["year_be"] == 2568
    assert dim_date[0]["month_label"] == "ม.ค. 2568"


def test_population_fact_includes_zero_case_districts(spark):
    tables = build_warehouse_tables(_standardized(spark), _population(spark))

    # ดุสิต has population but no cases: it must stay in the denominator.
    districts = tables["dim_district"]
    assert districts.count() == len(BANGKOK_DISTRICTS)
    population = tables["fact_population"].join(districts, "district_key")
    assert {row["district_name"] for row in population.collect()} == {
        "บางกะปิ", "ดุสิต",
    }


def test_static_demographic_dimensions(spark):
    tables = build_warehouse_tables(_standardized(spark), _population(spark))

    assert tables["dim_age_group"].count() == 10
    assert {row["sex"] for row in tables["dim_sex"].collect()} == {"M", "F", "U"}
