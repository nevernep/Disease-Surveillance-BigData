"""Build the dimensional warehouse tables consumed by BI tools."""

from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from spark.schemas import (
    AGE_GROUPS,
    BANGKOK_DISTRICTS,
    SEX_LABELS,
    THAI_MONTH_ABBREVIATIONS,
)

FACT_CASES_KEY = [
    "date_key", "district_key", "disease_key", "age_group_key", "sex",
]


def _month_label(month_column, year_be_column):
    labels = F.array(*[F.lit(label) for label in THAI_MONTH_ABBREVIATIONS])
    return F.concat_ws(" ", F.element_at(labels, month_column), year_be_column)


def build_dim_date(standardized: DataFrame) -> DataFrame:
    """Monthly calendar covering every month between the first and last case.

    Months with zero cases are kept so trend charts show gaps as zero, not
    missing points. date_key = yyyymm (Gregorian) is stable across runs.
    """

    bounds = standardized.agg(
        F.trunc(F.min("report_date"), "month").alias("first_month"),
        F.trunc(F.max("report_date"), "month").alias("last_month"),
    )

    months = bounds.select(
        F.explode(
            F.sequence(
                "first_month", "last_month", F.expr("interval 1 month")
            )
        ).alias("month_start")
    )

    return (
        months
        .withColumn("year_ce", F.year("month_start"))
        .withColumn("month", F.month("month_start"))
        .withColumn("year_be", F.col("year_ce") + F.lit(543))
        .withColumn(
            "date_key", (F.col("year_ce") * 100 + F.col("month")).cast("int")
        )
        .withColumn("quarter", F.quarter("month_start"))
        .withColumn("month_label", _month_label(F.col("month"), F.col("year_be")))
        .select(
            "date_key", "month_start", "year_be", "year_ce", "quarter",
            "month", "month_label",
        )
    )


def build_dim_district(
    standardized: DataFrame,
    population: DataFrame,
) -> DataFrame:
    """All 50 Bangkok districts plus any other observed name.

    Including zero-case districts matters: fact_population needs a key for
    every district so incidence denominators are not limited to districts
    that happened to report a case.
    """

    spark = standardized.sparkSession
    master = spark.createDataFrame(
        [(name,) for name in BANGKOK_DISTRICTS], ["district_name"]
    )
    names = (
        master
        .unionByName(standardized.select("district_name"))
        .unionByName(population.select("district_name"))
        .distinct()
        .withColumn(
            "is_bangkok_district",
            F.col("district_name").isin(BANGKOK_DISTRICTS),
        )
    )

    return (
        names
        .withColumn(
            "district_key",
            F.dense_rank().over(Window.orderBy("district_name")).cast("int"),
        )
        .select("district_key", "district_name", "is_bangkok_district")
    )


def build_dim_disease(standardized: DataFrame) -> DataFrame:
    return (
        standardized.select("disease_name")
        .distinct()
        .withColumn(
            "disease_key",
            F.dense_rank().over(Window.orderBy("disease_name")).cast("int"),
        )
        .select("disease_key", "disease_name")
    )


def build_dim_age_group(standardized: DataFrame) -> DataFrame:
    schema = StructType([
        StructField("age_group_key", IntegerType(), False),
        StructField("age_group", StringType(), False),
        StructField("min_age", IntegerType(), True),
        StructField("max_age", IntegerType(), True),
    ])
    return standardized.sparkSession.createDataFrame(AGE_GROUPS, schema)


def build_dim_sex(standardized: DataFrame) -> DataFrame:
    return standardized.sparkSession.createDataFrame(
        list(SEX_LABELS.items()), ["sex", "sex_label"]
    )


def build_warehouse_tables(
    standardized: DataFrame,
    population: DataFrame,
) -> Dict[str, DataFrame]:
    """Build the star schema from case-level standardized data.

    - fact_disease_cases: month × district × disease × age group × sex
    - fact_population:    year × district (incidence denominator)

    Population lives in its own fact so a disease/sex/age filter never shrinks
    the denominator to districts that reported a case.
    """

    dim_date = build_dim_date(standardized)
    dim_district = build_dim_district(standardized, population)
    dim_disease = build_dim_disease(standardized)

    cases = (
        standardized
        .withColumn(
            "date_key",
            (F.year("report_date") * 100 + F.month("report_date")).cast("int"),
        )
        .join(dim_district.select("district_key", "district_name"), "district_name")
        .join(dim_disease, "disease_name")
    )

    fact_disease_cases = (
        cases
        .groupBy(*FACT_CASES_KEY)
        .agg(
            F.round(F.sum("case_count")).cast("long").alias("total_cases"),
            F.count_distinct("case_id").cast("long").alias("source_records"),
        )
        .select(*FACT_CASES_KEY, "total_cases", "source_records")
    )

    fact_population = (
        population
        .join(dim_district.select("district_key", "district_name"), "district_name")
        .select(
            F.col("year_be").cast("int").alias("year_be"),
            "district_key",
            F.round("population").cast("long").alias("population"),
        )
    )

    return {
        "dim_date": dim_date,
        "dim_district": dim_district,
        "dim_disease": dim_disease,
        "dim_age_group": build_dim_age_group(standardized),
        "dim_sex": build_dim_sex(standardized),
        "fact_disease_cases": fact_disease_cases,
        "fact_population": fact_population,
    }
