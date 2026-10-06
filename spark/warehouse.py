"""Build the dimensional warehouse tables consumed by BI tools."""

from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_warehouse_tables(curated: DataFrame) -> Dict[str, DataFrame]:
    """Convert the Gold aggregate into a small star schema.

    The fact table keeps the Gold grain: one row per year, district and
    disease. Surrogate keys make the model stable and easy to relate in BI.
    """

    date_window = Window.orderBy("year_be")
    district_window = Window.orderBy("district_name")
    disease_window = Window.orderBy("disease_name")

    dim_date = (
        curated.select("year_be")
        .distinct()
        .withColumn("date_key", F.dense_rank().over(date_window).cast("int"))
        .withColumn("year_ce", (F.col("year_be") - 543).cast("int"))
        .withColumn("year_label", F.concat(F.col("year_be"), F.lit(" พ.ศ.")))
        .select("date_key", "year_be", "year_ce", "year_label")
    )

    dim_district = (
        curated.select("district_name")
        .distinct()
        .withColumn(
            "district_key",
            F.dense_rank().over(district_window).cast("int"),
        )
        .select("district_key", "district_name")
    )

    dim_disease = (
        curated.select("disease_name")
        .distinct()
        .withColumn(
            "disease_key",
            F.dense_rank().over(disease_window).cast("int"),
        )
        .select("disease_key", "disease_name")
    )

    fact_disease_cases = (
        curated.join(dim_date, on="year_be", how="inner")
        .join(dim_district, on="district_name", how="inner")
        .join(dim_disease, on="disease_name", how="inner")
        .select(
            "date_key",
            "district_key",
            "disease_key",
            "total_cases",
            "population",
            "incidence_rate_per_100k",
            "source_records",
        )
    )

    return {
        "dim_date": dim_date,
        "dim_district": dim_district,
        "dim_disease": dim_disease,
        "fact_disease_cases": fact_disease_cases,
    }