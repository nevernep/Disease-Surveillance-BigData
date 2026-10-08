import re
from pathlib import Path
from typing import List

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

from spark.cleaners import find_column
# One file per year (newest full download, else the API sample); shared with
# the extract script and the raw-landing DAG.
from spark.raw_versions import select_disease_files  # noqa: F401 (re-exported)
from spark.schemas import POPULATION_COLUMN_ALIASES

NESTED_RECORD_KEYS = ["data", "records", "result", "results", "items"]
PATH_NOT_FOUND_MARKERS = ("PATH_NOT_FOUND", "Path does not exist")
CSV_ENCODINGS = ["utf-8-sig", "utf-8", "cp874", "tis-620"]

def _strip_bom_columns(dataframe: DataFrame) -> DataFrame:
    for column in dataframe.columns:
        cleaned = column.replace("\ufeff", "").strip()
        if cleaned != column:
            dataframe = dataframe.withColumnRenamed(column, cleaned)
    return dataframe


def _with_record_id(dataframe: DataFrame, source_path: str) -> DataFrame:
    """เพิ่ม _record_id = ชื่อไฟล์:ลำดับแถว ให้ไฟล์ที่ไม่มี _id (CSV ต้นฉบับ)

    ผู้ป่วยต่างคนอาจมีวัน เขต โรค เพศ อายุ ตรงกันทุกค่า หากใช้ hash ของค่าเหล่านี้
    เป็น case_id จะถูกตัดเป็นข้อมูลซ้ำผิด ๆ จึงใช้ลำดับแถวในไฟล์แทน
    (อ่านแบบ multiLine ทำให้ 1 ไฟล์ = 1 partition จึงได้เลขต่อเนื่องตามลำดับแถว)

    ใช้ monotonically_increasing_id ซึ่งทำงานใน JVM ทั้งหมด แทน rdd.zipWithIndex ที่ส่ง
    ทุกแถวผ่าน Python worker (กับข้อมูลเต็มทำให้ CPU เต็มจน Airflow heartbeat หมดเวลา)
    """

    file_name = source_path.rstrip("/").rsplit("/", 1)[-1]
    row_number = F.monotonically_increasing_id() + F.lit(1)

    return dataframe.withColumn(
        "_record_id",
        F.concat_ws(":", F.lit(file_name), row_number.cast("string")),
    )


def _read_disease_file(spark: SparkSession, path: str) -> DataFrame:
    if path.endswith(".csv"):
        dataframe = (
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .csv(path)
        )
        dataframe = _strip_bom_columns(dataframe)
        dataframe = dataframe.withColumn("_source_file", F.input_file_name())
        return _with_record_id(dataframe, path)

    dataframe = (
        spark.read
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .json(path)
    )
    dataframe = dataframe.withColumn("_source_file", F.input_file_name())
    return _flatten_nested_records(dataframe)


def read_disease_files(spark: SparkSession, paths: List[str]) -> DataFrame:
    """อ่านไฟล์ Disease ที่เลือกแล้ว (CSV และ/หรือ JSON) รวมเป็น DataFrame เดียว"""

    if not paths:
        raise FileNotFoundError("ไม่พบไฟล์ Disease ที่ตรงรูปแบบ disease_cases_{ปี}_{full|sample}")

    for path in paths:
        print(f"      ใช้ไฟล์: {path}")

    dataframe = None
    for path in paths:
        frame = _read_disease_file(spark, path)
        dataframe = (
            frame
            if dataframe is None
            else dataframe.unionByName(frame, allowMissingColumns=True)
        )

    if "_corrupt_record" in dataframe.columns:
        dataframe = dataframe.drop("_corrupt_record")

    return dataframe


def read_disease_json(
    spark: SparkSession,
    disease_directory: Path,
) -> DataFrame:
    """อ่านไฟล์ Disease จากโฟลเดอร์ในเครื่อง (ปีละ 1 ไฟล์ ใช้ไฟล์เต็มก่อน)"""

    paths = select_disease_files(
        str(path) for path in sorted(disease_directory.glob("disease_cases_*"))
    )
    if not paths:
        raise FileNotFoundError(f"ไม่พบไฟล์ Disease ใน {disease_directory}")

    return read_disease_files(spark, paths)


def _read_lake(reader, path_glob: str, file_format: str) -> DataFrame:
    """อ่านไฟล์จาก Data Lake; แปลง path-not-found เป็น FileNotFoundError"""

    from pyspark.errors import AnalysisException

    try:
        return getattr(reader, file_format)(path_glob)
    except AnalysisException as error:
        if any(marker in str(error) for marker in PATH_NOT_FOUND_MARKERS):
            raise FileNotFoundError(f"ไม่พบไฟล์ใน Data Lake: {path_glob}") from error
        raise


def list_lake_files(spark: SparkSession, path_glob: str) -> List[str]:
    """ขยาย glob บน Data Lake ผ่าน Hadoop FileSystem (เช่น s3a://)"""

    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path_glob)
    filesystem = hadoop_path.getFileSystem(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    statuses = filesystem.globStatus(hadoop_path) or []
    return sorted(
        status.getPath().toString() for status in statuses if status.isFile()
    )


def read_disease_from_lake(
    spark: SparkSession,
    path_glob: str,
) -> DataFrame:
    """อ่าน Raw ที่ DAG land ไว้ใน raw/disease/year=*/ (ปีละ 1 ไฟล์ ใช้ไฟล์เต็มก่อน)"""

    paths = select_disease_files(list_lake_files(spark, path_glob))
    if not paths:
        raise FileNotFoundError(f"ไม่พบไฟล์ใน Data Lake: {path_glob}")

    return read_disease_files(spark, paths)


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
    dataframe = _strip_bom_columns(dataframe)

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