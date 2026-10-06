from datetime import date

from spark.curated import build_curated_dataset
from spark.data_quality import run_quality_checks
from spark.warehouse import build_warehouse_tables


def _standardized(spark):
    return spark.createDataFrame(
        [
            ("C1", date(2025, 1, 6), 2568, "บางกะปิ", "บางกะปิ", "ไข้เลือดออก",
             "ไข้เลือดออก", 1.0, "M", 30.0, 5, "f"),
            ("C2", date(2025, 2, 6), 2568, "ทุ่งครุ", "ทุ่งครุ", "ไข้หวัดใหญ่",
             "ไข้หวัดใหญ่", 1.0, "F", 70.0, 9, "f"),
        ],
        "case_id string, report_date date, year_be int, district_name_raw string, "
        "district_name string, disease_name_raw string, disease_name string, "
        "case_count double, sex string, age double, age_group_key int, "
        "source_file string",
    )


def _population(spark, rows):
    return spark.createDataFrame(
        rows, "year_be int, district_name string, population double"
    )


def _run(spark, settings, population, population_available):
    standardized = _standardized(spark)
    curated = build_curated_dataset(standardized, population)
    report, has_failure = run_quality_checks(
        spark=spark,
        raw=standardized,
        canonical=standardized,
        cleaned=standardized,
        standardized=standardized,
        curated=curated,
        removed_duplicates=0,
        settings=settings,
        population_available=population_available,
        warehouse=build_warehouse_tables(standardized, population),
    )
    return {row["rule"]: row for row in report.collect()}, has_failure


def test_cases_only_mode_skips_population_rule(spark, settings):
    rules, has_failure = _run(spark, settings, _population(spark, []), False)

    assert rules["missing_population_rate"]["status"] == "SKIP"
    assert not has_failure


def test_missing_population_rate_is_enforced_when_loaded(spark, settings):
    # Only บางกะปิ has population -> half of the gold rows are uncovered.
    population = _population(spark, [(2568, "บางกะปิ", 140000.0)])
    rules, has_failure = _run(spark, settings, population, True)

    assert rules["missing_population_rate"]["observed"] == "0.5000"
    assert rules["missing_population_rate"]["status"] == "FAIL"
    assert has_failure


def test_warehouse_reconciles_with_standardized(spark, settings):
    rules, _ = _run(spark, settings, _population(spark, []), False)

    assert rules["fact_cases_reconcile_with_standardized"]["status"] == "PASS"
    assert rules["fact_primary_key_unique"]["status"] == "PASS"
    assert rules["fact_date_key_in_dim_date"]["status"] == "PASS"
    assert rules["year_be_vs_report_date_mismatch_rows"]["observed"] == "0"
