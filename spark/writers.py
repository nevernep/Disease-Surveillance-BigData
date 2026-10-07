import shutil
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame

from spark.config import ProjectPaths, is_remote_uri


def remove_path(path: Path) -> None:
    """ลบไฟล์หรือโฟลเดอร์เดิมก่อนเขียนทับ"""

    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def write_parquet_directory(
    dataframe: DataFrame,
    output_path: "Path | str",
) -> None:
    """เขียน Parquet แบบหลาย part ลงเครื่องหรือ Data Lake (s3a://)"""

    if not is_remote_uri(output_path):
        output_path = Path(output_path)
        remove_path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

    (
        dataframe.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(output_path))
    )


def write_single_file(
    dataframe: DataFrame,
    target_file: Path,
    output_format: str,
) -> None:
    """เขียนเป็นไฟล์เดียวตามชื่อที่กำหนด เช่น curated_disease_data.csv"""

    if output_format not in {"csv", "parquet"}:
        raise ValueError(
            f"ไม่รองรับรูปแบบไฟล์: {output_format}"
        )

    target_file.parent.mkdir(parents=True, exist_ok=True)

    # Spark writes to the container's own filesystem first: listing a Docker
    # Desktop bind mount right after a large write can lag and show no part
    # file. Only the finished file is moved onto the target folder.
    temporary_root = Path(tempfile.mkdtemp(prefix="spark_single_file_"))
    temporary_path = temporary_root / "output"

    writer = dataframe.coalesce(1).write.mode("overwrite")

    if output_format == "csv":
        (
            writer
            .option("header", True)
            .option("encoding", "UTF-8")
            .csv(str(temporary_path))
        )
    else:
        writer.option("compression", "snappy").parquet(
            str(temporary_path)
        )

    part_files = list(
        temporary_path.glob(f"part-*.{output_format}")
    )

    try:
        if len(part_files) != 1:
            raise RuntimeError(
                f"คาดว่าจะพบ 1 part file ใน {temporary_path} "
                f"แต่พบ {len(part_files)} ไฟล์"
            )

        remove_path(target_file)
        shutil.move(str(part_files[0]), str(target_file))
    finally:
        remove_path(temporary_root)


def write_quality_report(
    dataframe: DataFrame,
    target_file: Path,
) -> None:
    """บันทึกรายงานคุณภาพข้อมูลเป็น CSV ไฟล์เดียว"""

    write_single_file(dataframe, target_file, "csv")


def write_error_log(
    dataframe: DataFrame,
    target_file: Path,
) -> None:
    """บันทึกเฉพาะรายการ DQ ที่ไม่ผ่านเพื่อใช้ติดตามแก้ไข"""

    write_single_file(dataframe.filter("status = 'FAIL'"), target_file, "csv")


def write_warehouse_tables(
    tables: dict[str, DataFrame],
    paths: ProjectPaths,
) -> None:
    """Write star-schema tables: local CSV for BI, Parquet locally or on the lake."""

    for table_name, dataframe in tables.items():
        write_single_file(
            dataframe,
            paths.warehouse_dir / f"{table_name}.csv",
            "csv",
        )
        write_parquet_directory(dataframe, paths.warehouse_table(table_name))