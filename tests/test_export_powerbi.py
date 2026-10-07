import openpyxl
import pandas as pd

from spark.export_powerbi import write_workbook


def test_workbook_tables_have_clean_names_distinct_from_sheets(tmp_path):
    frames = {
        "dim_sex": pd.DataFrame({"sex": ["M", "F"], "sex_label": ["ชาย", "หญิง"]}),
        "fact_population": pd.DataFrame(
            {"year_be": [2569], "district_key": [1], "population": [1000]}
        ),
    }
    output = tmp_path / "powerbi.xlsx"

    write_workbook(frames, output)

    workbook = openpyxl.load_workbook(output)
    tables = {
        table_name: (sheet.title, ref)
        for sheet in workbook.worksheets
        for table_name, ref in sheet.tables.items()
    }
    # Power BI lists Excel Tables and sheets side by side; distinct names mean
    # the clean name is always the Excel Table (header row guaranteed).
    assert tables == {
        "dim_sex": ("sheet_dim_sex", "A1:B3"),
        "fact_population": ("sheet_fact_population", "A1:C2"),
    }
    assert [cell.value for cell in workbook["sheet_dim_sex"][1]] == ["sex", "sex_label"]
