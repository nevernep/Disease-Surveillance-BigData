from pyspark.sql import functions as F

from spark.standardizers import (
    age_group_key,
    standardize_disease,
    standardize_district,
    standardize_sex,
)


def test_standardize_district(spark):
    data = spark.createDataFrame(
        [("เขตบางกะปิ",), ("ข. ลาดพร้าว",), ("ป้อมปราบฯ กทม.",)],
        ["raw"],
    )

    results = data.select(
        standardize_district(F.col("raw")).alias("value")
    ).collect()

    assert [row["value"] for row in results] == [
        "บางกะปิ",
        "ลาดพร้าว",
        "ป้อมปราบศัตรูพ่าย",
    ]


def test_standardize_disease(spark):
    data = spark.createDataFrame(
        [
            ("โรคไข้เลือดออก",), ("Dengue Fever",), ("COVID-19",),
            ("โควิด-19 (COVID-19)",),
        ],
        ["raw"],
    )

    results = data.select(
        standardize_disease(F.col("raw")).alias("value")
    ).collect()

    assert [row["value"] for row in results] == [
        "ไข้เลือดออก",
        "ไข้เลือดออก",
        "โควิด-19",
        "โควิด-19",
    ]


def test_standardize_diarrhea_across_years(spark):
    data = spark.createDataFrame(
        [("อุจจาระร่วง",), ("โรคอุจจาระร่วงเฉียบพลัน",), ("อุจจาระร่วงเฉียบพลัน",)],
        ["raw"],
    )

    results = data.select(
        standardize_disease(F.col("raw")).alias("value")
    ).collect()

    assert {row["value"] for row in results} == {"อุจจาระร่วงเฉียบพลัน"}


def test_standardize_2569_disease_names(spark):
    data = spark.createDataFrame(
        [
            ("ติดเชื้อไวรัสโคโรนา 2019 (covid-19)",),
            ("โรคปอดอักเสบหรือโรคปอดบวม",),
            ("ไข้เด็งกี่ (Dengue fever)",),
            ("ไข้เลือดออก (DHF)",),
            ("ไข้เลือดออกช็อค (DSS)",),
        ],
        ["raw"],
    )

    results = data.select(
        standardize_disease(F.col("raw")).alias("value")
    ).collect()

    assert [row["value"] for row in results] == [
        "โควิด-19", "ปอดบวม", "ไข้เลือดออก", "ไข้เลือดออก", "ไข้เลือดออก",
    ]


def test_standardize_pneumonia(spark):
    data = spark.createDataFrame(
        [("โรคปอดบวม",), ("ปอดอักเสบ",), ("Pneumonia",)], ["raw"]
    )

    results = data.select(
        standardize_disease(F.col("raw")).alias("value")
    ).collect()

    assert {row["value"] for row in results} == {"ปอดบวม"}


def test_standardize_sex(spark):
    data = spark.createDataFrame(
        [("ชาย",), ("Female",), ("ไม่ระบุ",)], ["raw"]
    )

    results = data.select(
        standardize_sex(F.col("raw")).alias("value")
    ).collect()

    assert [row["value"] for row in results] == ["M", "F", "U"]


def test_age_group_key_boundaries(spark):
    data = spark.createDataFrame(
        [(0.0,), (4.0,), (5.0,), (14.0,), (15.0,), (64.0,), (65.0,), (98.0,), (None,)],
        "age double",
    )

    results = data.select(age_group_key(F.col("age")).alias("key")).collect()

    assert [row["key"] for row in results] == [1, 1, 2, 3, 4, 8, 9, 9, 10]


def test_standardize_district_fixes_thai_typing_errors(spark):
    data = spark.createDataFrame(
        [("เขตดินเเดง",), ("วัังทองหลาง",), ("ป้อมปราบ ศัตรูพ่าย",), ("บางเเค",)],
        ["raw"],
    )

    results = data.select(
        standardize_district(F.col("raw")).alias("value")
    ).collect()

    assert [row["value"] for row in results] == [
        "ดินแดง", "วังทองหลาง", "ป้อมปราบศัตรูพ่าย", "บางแค",
    ]
