import pytest


# pyspark is imported inside the fixtures so test modules that don't need
# Spark (DAG tests in the Airflow-only CI job, extract tests) can run without it.

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("DiseasePart3Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "Asia/Bangkok")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture(scope="session")
def settings():
    from spark.config import Settings

    return Settings()
