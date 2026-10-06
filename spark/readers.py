from pathlib import Path
from typing import List

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

from spark.cleaners import find_column
from spark.schemas import POPULATION_COLUMN_ALIASES

NESTED_RECORD_KEYS = ["data", "records", "result", "results", "items"]
PATH_NOT_FOUND_MARKERS = ("PATH_NOT_FOUND", "Path does not exist")
CSV_ENCODINGS = ["utf-8-sig", "utf-8", "cp874", "tis-620"]


def read_disease_json(
    spark: SparkSession,
    disease_directory: Path,
) -> DataFrame:
    """อ่านไฟล์ Disease แบบ JSON หรือ CSV จากโฟลเดอร์"""

    source_files = sorted(disease_directory.glob("disease_cases_*") )

    if not source_files:
        raise FileNotFoundError(
            f"ไม่พบไฟล์ Disease ใน {disease_directory}"
        )

    json_files = []
    csv_files = []
    for path in source_files:
        with path.open("r", encoding="utf-8-sig") as source:
            first_character = source.read(1)
        if first_character in {"[", "{"}:
            json_files.append(str(path))
        else:
            csv_files.append(str(path))

    dataframes = []
    if json_files:
        dataframes.append(
            spark.read
            .option("multiLine", True)
            .option("mode", "PERMISSIVE")
            .json(json_files)
        )
    if csv_files:
        dataframes.append(
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .csv(csv_files)
        )

    dataframe = dataframes[0]
    for next_dataframe in dataframes[1:]:
        dataframe = dataframe.unionByName(
            next_dataframe,
            allowMissingColumns=True,
        )

    dataframe = dataframe.withColumn("_source_file", F.input_file_name())

    dataframe = _flatten_nested_records(dataframe)

    if "_corrupt_record" in dataframe.columns:
        dataframe = dataframe.drop("_corrupt_record")

    return dataframe


def _read_lake(reader, path_glob: str, file_format: str) -> DataFrame:
    """อ่านไฟล์จาก Data Lake; แปลง path-not-found เป็น FileNotFoundError"""

    from pyspark.errors import AnalysisException

    try:
        return getattr(reader, file_format)(path_glob)
    except AnalysisException as error:
        if any(marker in str(error) for marker in PATH_NOT_FOUND_MARKERS):
            raise FileNotFoundError(f"ไม่พบไฟล์ใน Data Lake: {path_glob}") from error
        raise


def read_disease_from_lake(
    spark: SparkSession,
    path_glob: str,
) -> DataFrame:
    """อ่าน Raw JSON ที่ DAG land ไว้ใน raw/disease/year=*/ ของ Data Lake"""

    reader = (
        spark.read
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
    )
    dataframe = _read_lake(reader, path_glob, "json")
    dataframe = dataframe.withColumn("_source_file", F.input_file_name())
    dataframe = _flatten_nested_records(dataframe)

    if "_corrupt_record" in dataframe.columns:
        dataframe = dataframe.drop("_corrupt_record")

    return dataframe


def read_population_from_lake(
    spark: SparkSession,
    path_glob: str,
) -> DataFrame:
    """อ่าน population_summary_{year}.csv จาก raw/population/year=*/"""

    reader = (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
    )
    dataframe = _read_lake(reader, path_glob, "csv")

    # ไฟล์เขียนด้วย utf-8-sig: ตัด BOM ออกจากชื่อคอลัมน์แรก
    for column in dataframe.columns:
        cleaned = column.replace("\ufeff", "").strip()
        if cleaned != column:
            dataframe = dataframe.withColumnRenamed(column, cleaned)

    has_district = find_column(
        dataframe, POPULATION_COLUMN_ALIASES["district_name"]
    )
    has_population = find_column(
        dataframe, POPULATION_COLUMN_ALIASES["population"]
    )
    if has_district is None or has_population is None:
        raise FileNotFoundError(
            f"ไฟล์ Population ใน Data Lake ไม่มีคอลัมน์เขตและประชากร: {path_glob}"
        )

    return dataframe.withColumn("_source_file", F.input_file_name())


def _flatten_nested_records(dataframe: DataFrame) -> DataFrame:
    """กรณี JSON ห่อข้อมูลไว้ใน key เช่น data หรือ records"""

    for key in NESTED_RECORD_KEYS:
        if key not in dataframe.columns:
            continue

        field_type = dataframe.schema[key].dataType

        if isinstance(field_type, ArrayType):
            return (
                dataframe
                .select(
                    F.explode_outer(key).alias("_record"),
                    "_source_file",
                )
                .select("_record.*", "_source_file")
            )

    return dataframe


def read_population_files(
    spark: SparkSession,
    reference_directory: Path,
) -> DataFrame:
    """อ่านไฟล์ประชากรทั้ง CSV และ Excel แล้วรวมเป็น DataFrame เดียว"""

    files = sorted(
        list(reference_directory.glob("*.csv"))
        + list(reference_directory.glob("*.xlsx"))
    )

    if not files:
        raise FileNotFoundError(
            f"ไม่พบไฟล์ Population ใน {reference_directory}"
        )

    frames: List[pd.DataFrame] = []

    for path in files:
        if path.suffix.lower() == ".csv":
            frame = _read_csv_with_fallback(path)
        else:
            frame = pd.read_excel(path, engine="openpyxl")

        frame.columns = [
            str(column).strip() for column in frame.columns
        ]

        has_district = find_column(
            frame, POPULATION_COLUMN_ALIASES["district_name"]
        )
        has_population = find_column(
            frame, POPULATION_COLUMN_ALIASES["population"]
        )
        if has_district is None or has_population is None:
            print(
                f"ข้ามไฟล์ที่ไม่ใช่ Population Reference: {path.name}"
            )
            continue

        frame["_source_year"] = _extract_year_from_filename(path)
        frame["_source_file"] = path.name

        frames.append(frame)

    if not frames:
        # FileNotFoundError so --allow-missing-population also covers a
        # reference folder that only holds non-district files (e.g. Bueng Kum xlsx).
        raise FileNotFoundError(
            "ไม่พบไฟล์ Population Reference ที่มีคอลัมน์เขตและประชากร"
        )

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = combined.astype(object).where(pd.notna(combined), None)

    return spark.createDataFrame(combined)


def _read_csv_with_fallback(path: Path) -> pd.DataFrame:
    """ลองอ่าน CSV ด้วย Encoding ภาษาไทยหลายรูปแบบ"""

    last_error = None

    for encoding in CSV_ENCODINGS:
        try:
            return pd.read_csv(path, encoding=encoding)
        except UnicodeDecodeError as error:
            last_error = error

    raise RuntimeError(
        f"ไม่สามารถอ่านไฟล์ CSV {path}: {last_error}"
    )


def _extract_year_from_filename(path: Path):
    """ดึงปี พ.ศ. จากชื่อไฟล์ เช่น population_summary_2568.csv"""

    import re

    match = re.search(r"(\d{4})", path.stem)

    return int(match.group(1)) if match else None