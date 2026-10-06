import pytest

from spark.load_warehouse import (
    LOAD_ORDER,
    build_copy_sql,
    count_csv_rows,
    read_csv_header,
)


def test_dimensions_load_before_facts():
    first_fact = min(
        index for index, table in enumerate(LOAD_ORDER)
        if table.startswith("fact_")
    )
    assert all(table.startswith("dim_") for table in LOAD_ORDER[:first_fact])
    assert all(table.startswith("fact_") for table in LOAD_ORDER[first_fact:])
    assert {"fact_disease_cases", "fact_population"} <= set(LOAD_ORDER)


def test_header_and_row_count_strip_bom(tmp_path):
    path = tmp_path / "dim_disease.csv"
    path.write_text(
        "disease_key,disease_name\n1,ปอดบวม\n2,ไข้เลือดออก\n",
        encoding="utf-8-sig",
    )

    assert read_csv_header(path) == ["disease_key", "disease_name"]
    assert count_csv_rows(path) == 2


def test_copy_sql_uses_header_columns():
    sql = build_copy_sql("dim_date", ["date_key", "year_be"])

    assert sql.startswith('COPY mart.dim_date ("date_key", "year_be") FROM STDIN')
    assert "HEADER true" in sql


def test_copy_sql_rejects_unsafe_column_names():
    with pytest.raises(ValueError):
        build_copy_sql("dim_date", ['date_key"); DROP TABLE x; --'])
