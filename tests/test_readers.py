import pytest

from spark.readers import (
    read_disease_json,
    read_population_from_lake,
    select_disease_files,
)

CSV_HEADER = (
    "ชื่อกลุ่มโรค,อายุ (เต็ม) ปี,อายุ (เต็ม) เดือน,อาย (เต็ม) วัน,เพศ,สถานภาพสมรส,"
    "สัญชาติ,อาชีพ,จังหวัด,อำเภอ/เขต,ตำบล/แขวง,วันที่เริ่มป่วย,สภาพผู้ป่วย,"
    "ประเภทผู้ป่วย,สถานที่รักษา"
)
CSV_ROW = (
    "ไข้หวัดใหญ่,49,0,0,ชาย,โสด,ไทย,อื่นๆ,กรุงเทพมหานคร,พระนคร,บ้านพานถม,"
    "1/6/2025,ยังรักษาอยู่,ผู้ป่วยนอก,โรงพยาบาลราชวิถี"
)


def test_select_disease_files_prefers_full_per_year():
    paths = [
        "s3a://b/raw/disease/year=2568/disease_cases_2568_sample.json",
        "s3a://b/raw/disease/year=2568/disease_cases_2568_full.csv",
        "s3a://b/raw/disease/year=2569/disease_cases_2569_sample.json",
        "s3a://b/raw/disease/year=2569/_metadata/disease_cases_2569_sample.manifest.json",
        "s3a://b/raw/disease/year=2569/notes.txt",
    ]

    assert select_disease_files(paths) == [
        "s3a://b/raw/disease/year=2568/disease_cases_2568_full.csv",
        "s3a://b/raw/disease/year=2569/disease_cases_2569_sample.json",
    ]


def test_full_csv_gets_unique_record_ids_even_for_identical_rows(spark, tmp_path):
    # Two identical rows = possibly two patients; a multi-line quoted field too.
    multiline = CSV_ROW.replace("โรงพยาบาลราชวิถี", '"โรงพยาบาล\nราชวิถี"')
    (tmp_path / "disease_cases_2568_full.csv").write_text(
        "\r\n".join([CSV_HEADER, CSV_ROW, CSV_ROW, multiline]) + "\r\n",
        encoding="utf-8-sig",
    )
    # A sample for the same year must be ignored in favour of the full file.
    (tmp_path / "disease_cases_2568_sample.json").write_text("[]", encoding="utf-8")

    dataframe = read_disease_json(spark, tmp_path)
    rows = dataframe.orderBy("_record_id").collect()

    assert "ชื่อกลุ่มโรค" in dataframe.columns  # BOM stripped from header
    assert len(rows) == 3
    assert [row["_record_id"] for row in rows] == [
        "disease_cases_2568_full.csv:1",
        "disease_cases_2568_full.csv:2",
        "disease_cases_2568_full.csv:3",
    ]
    assert rows[2]["สถานที่รักษา"] == "โรงพยาบาล\nราชวิถี"


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
