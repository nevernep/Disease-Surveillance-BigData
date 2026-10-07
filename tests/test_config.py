from pathlib import Path

from spark.config import ProjectPaths, is_remote_uri


def test_local_paths_without_lake():
    paths = ProjectPaths(root=Path("/project"))

    assert not is_remote_uri(paths.clean_dir)
    assert Path(paths.clean_dir) == Path(
        "/project/data/processed/clean/disease_cleaned"
    )
    assert Path(paths.warehouse_table("dim_date")) == Path(
        "/project/data/processed/warehouse/dim_date"
    )


def test_lake_paths_use_s3a_uris():
    paths = ProjectPaths(
        root=Path("/project"), lake_uri="s3a://disease-surveillance/"
    )

    assert paths.lake_raw_disease_glob == (
        "s3a://disease-surveillance/raw/disease/year=*/disease_cases_*"
    )
    assert paths.lake_raw_population_glob == (
        "s3a://disease-surveillance/raw/population/year=*/"
        "population_summary_*.csv"
    )
    assert paths.clean_dir == (
        "s3a://disease-surveillance/processed/clean/disease_cleaned"
    )
    assert paths.gold_parquet == (
        "s3a://disease-surveillance/processed/gold/"
        "disease_with_population/curated_disease_data"
    )
    assert paths.warehouse_table("fact_disease_cases") == (
        "s3a://disease-surveillance/processed/warehouse/fact_disease_cases"
    )
    # BI CSV exports stay local even in lake mode.
    assert Path(paths.gold_csv).is_relative_to(Path("/project"))
    assert paths.warehouse_dir == Path("/project/data/processed/warehouse")
