from datetime import date

from pyspark.sql import functions as F

from spark.cleaners import (
    clean_disease_data,
    convert_year_to_be,
    quarantine_disease_data,
    to_numeric,
)


def test_to_numeric_handles_thousand_separator(spark):
    data = spark.createDataFrame([("1,234",), ("abc",)], ["raw"])

    results = data.select(
        to_numeric(F.col("raw")).alias("value")
    ).collect()

    assert results[0]["value"] == 1234.0
    assert results[1]["value"] is None


def test_convert_year_to_be(spark):
    data = spark.createDataFrame([(2025,), (2568,)], ["year"])

    results = data.select(
        convert_year_to_be(F.col("year")).alias("value")
    ).collect()

    assert [row["value"] for row in results] == [2568, 2568]


def _canonical_rows(spark):
    onset = date(2025, 1, 6)
    rows = [
        (2568, onset, "ไข้เลือดออก", "บางกะปิ", 1.0, 30.0),   # valid
        (2568, onset, None, "บางกะปิ", 1.0, 30.0),            # no disease
        (2568, onset, "ไข้เลือดออก", "บางกะปิ", 0.0, 30.0),   # zero count
        (2568, onset, "ไข้เลือดออก", "บางกะปิ", 1.0, 500.0),  # bad age
        (2568, None, "ไข้เลือดออก", "บางกะปิ", 1.0, 30.0),    # no onset date
        (None, onset, "ไข้เลือดออก", "บางกะปิ", 1.0, 30.0),   # no year
    ]
    return spark.createDataFrame(
        rows,
        "year_be int, report_date date, disease_name_raw string, "
        "district_name_raw string, case_count double, age double",
    )


def test_clean_removes_invalid_rows(spark, settings):
    assert clean_disease_data(_canonical_rows(spark), settings).count() == 1


def test_clean_and_quarantine_partition_every_row(spark, settings):
    data = _canonical_rows(spark)

    cleaned = clean_disease_data(data, settings).count()
    quarantine = quarantine_disease_data(data, settings)

    assert cleaned + quarantine.count() == data.count()
    reasons = {row["_quarantine_reason"] for row in quarantine.collect()}
    assert reasons == {
        "missing_disease_name",
        "invalid_case_count",
        "invalid_age",
        "invalid_or_missing_report_date",
        "invalid_year",
    }
