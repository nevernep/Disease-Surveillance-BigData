import pytest

from spark.readers import read_population_from_lake


def test_population_from_lake_strips_bom_and_keeps_columns(spark, tmp_path):
    year_dir = tmp_path / "raw" / "population" / "year=2568"
    year_dir.mkdir(parents=True)
    (year_dir / "population_summary_2568.csv").write_text(
        "ปี,เขต,ประชากรรวม\n2568,บางกะปิ,140000\n2568,ทุ่งครุ,110000\n",
        encoding="utf-8-sig",
    )

    dataframe = read_population_from_lake(
        spark,
        str(tmp_path / "raw" / "population" / "year=*" / "population_summary_*.csv"),
    )

    assert {"ปี", "เขต", "ประชากรรวม"} <= set(dataframe.columns)
    assert dataframe.count() == 2


def test_population_from_lake_missing_raises_file_not_found(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        read_population_from_lake(
            spark,
            str(tmp_path / "raw" / "population" / "year=*" / "population_summary_*.csv"),
        )
