import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F


from spark.cleaners import (
    canonicalize_disease_data,
    clean_disease_data,
    quarantine_disease_data,
)
from spark.config import (
    ProjectPaths,
    Settings,
    create_spark_session,
)
from spark.curated import build_curated_dataset
from spark.data_quality import run_quality_checks
from spark.deduplication import deduplicate_cases
from spark.population import (
    canonicalize_population_data,
    fill_missing_years,
)
from spark.readers import (
    read_disease_from_lake,
    read_disease_json,
    read_population_files,
    read_population_from_lake,
)
from spark.standardizers import standardize_disease_data
from spark.warehouse import build_warehouse_tables
from spark.writers import (
    write_parquet_directory,
    write_error_log,
    write_quality_report,
    write_single_file,
    write_warehouse_tables,
)


def run_pipeline(
    project_root: Path,
    fail_on_dq: bool,
    allow_missing_population: bool = False,
) -> int:
    settings = Settings.from_env(project_root)
    paths = ProjectPaths(root=project_root, lake_uri=settings.data_lake_uri)
    spark = create_spark_session(settings)

    try:
        if paths.lake_uri:
            print(f"[1/10] อ่านข้อมูลผู้ป่วยจาก Data Lake: {paths.lake_raw_disease_glob}")
            raw_disease = read_disease_from_lake(spark, paths.lake_raw_disease_glob)
        else:
            print("[1/10] อ่านข้อมูลผู้ป่วยจากไฟล์ JSON ในเครื่อง")
            raw_disease = read_disease_json(spark, paths.raw_disease_dir)
        raw_disease.cache()

        print("[2/10] แปลงเป็น Canonical Schema")
        canonical = canonicalize_disease_data(raw_disease)

        print("[3/10] ทำความสะอาดข้อมูล")
        quarantine = quarantine_disease_data(canonical, settings)
        cleaned = clean_disease_data(canonical, settings)
        write_parquet_directory(quarantine, paths.quarantine_dir)

        print("[4/10] ตรวจและลบข้อมูลซ้ำ")
        deduplicated, removed_rows = deduplicate_cases(cleaned)
        deduplicated.cache()
        print(f"      ตัดข้อมูลซ้ำออก {removed_rows} แถว")

        write_parquet_directory(deduplicated, paths.clean_dir)

        print("[5/10] Standardize ชื่อเขตและชื่อโรค")
        standardized = standardize_disease_data(deduplicated)
        standardized.cache()

        write_parquet_directory(standardized, paths.standardized_dir)

        print("[6/10] เตรียมข้อมูลประชากร")
        population_available = True
        try:
            if paths.lake_uri:
                raw_population = read_population_from_lake(
                    spark, paths.lake_raw_population_glob
                )
            else:
                raw_population = read_population_files(
                    spark, paths.raw_reference_dir
                )
            population = canonicalize_population_data(
                raw_population, settings
            )
            population = fill_missing_years(population, standardized)
        except FileNotFoundError:
            if not allow_missing_population:
                raise
            print("      ไม่พบ Population: ทำ Dashboard จำนวนผู้ป่วยเท่านั้น")
            population_available = False
            population = (
                standardized
                .select("year_be", "district_name")
                .limit(0)
                .withColumn("population", F.lit(None).cast("double"))
            )
        population.cache()

        print("[7/10] สร้าง Gold Layer และคำนวณ Incidence Rate")
        curated = build_curated_dataset(standardized, population)
        curated.cache()

        write_single_file(curated, paths.gold_csv, "csv")
        write_parquet_directory(curated, paths.gold_parquet)

        print("[8/10] สร้าง Data Warehouse Star Schema")
        warehouse_tables = build_warehouse_tables(standardized, population)
        write_warehouse_tables(warehouse_tables, paths)

        print("[9/10] ตรวจสอบคุณภาพข้อมูล")
        report, has_failure = run_quality_checks(
            spark=spark,
            raw=raw_disease,
            canonical=canonical,
            cleaned=cleaned,
            standardized=standardized,
            curated=curated,
            removed_duplicates=removed_rows,
            settings=settings,
            population_available=population_available,
            warehouse=warehouse_tables,
        )

        report.show(truncate=False)
        write_quality_report(report, paths.quality_csv)
        write_error_log(report, paths.error_log_csv)
        write_parquet_directory(report.coalesce(1), paths.quality_parquet)

        print("[10/10] เสร็จสิ้น")
        print(f"      Data Lake     : {paths.lake_uri or '(local mode)'}")
        print(f"      Gold CSV      : {paths.gold_csv}")
        print(f"      Warehouse     : {paths.warehouse_dir}")
        print(f"      Gold Parquet  : {paths.gold_parquet}")
        print(f"      Quality Report: {paths.quality_csv}")

        if has_failure:
            print("พบรายการ Data Quality ที่ไม่ผ่าน")

            if fail_on_dq:
                return 1

        return 0

    finally:
        spark.stop()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Part 3: ประมวลผลข้อมูลเฝ้าระวังโรคด้วย PySpark"
    )

    parser.add_argument(
        "--project-root",
        default=".",
        help="ตำแหน่ง Root Directory ของโปรเจกต์",
    )

    parser.add_argument(
        "--fail-on-dq",
        action="store_true",
        help="คืนค่า exit code 1 เมื่อ Data Quality ไม่ผ่าน",
    )

    parser.add_argument(
        "--allow-missing-population",
        action="store_true",
        help=(
            "สร้าง Dashboard จำนวนผู้ป่วยแม้ยังไม่มี Population; "
            "Incidence Rate จะเป็นค่าว่าง"
        ),
    )

    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_arguments()

    exit_code = run_pipeline(
        project_root=Path(arguments.project_root).resolve(),
        fail_on_dq=arguments.fail_on_dq,
        allow_missing_population=arguments.allow_missing_population,
    )

    sys.exit(exit_code)