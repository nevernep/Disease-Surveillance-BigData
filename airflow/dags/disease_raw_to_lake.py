"""Weekly: download the disease CSVs as new raw versions, validate, and land them in the S3 raw zone."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pendulum
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from airflow import DAG  # pyright: ignore[reportAttributeAccessIssue, reportMissingImports]
from airflow.exceptions import AirflowSkipException  # pyright: ignore[reportMissingImports]
from airflow.operators.trigger_dagrun import (  # pyright: ignore[reportMissingImports]
    TriggerDagRunOperator,
)
from airflow.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]


YEARS = ("2568", "2569")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
JSON_CONTENT_TYPE = "application/json"
POPULATION_REFERENCE_DIR = Path("disease") / "reference"
POPULATION_FIELDS = ("ปี", "เขต", "ประชากรรวม")
EXPECTED_BANGKOK_DISTRICTS = {
    "พระนคร", "ดุสิต", "หนองจอก", "บางรัก", "บางเขน",
    "บางกะปิ", "ปทุมวัน", "ป้อมปราบศัตรูพ่าย", "พระโขนง",
    "มีนบุรี", "ลาดกระบัง", "ยานนาวา", "สัมพันธวงศ์",
    "พญาไท", "ธนบุรี", "บางกอกใหญ่", "ห้วยขวาง",
    "คลองสาน", "ตลิ่งชัน", "บางกอกน้อย", "บางขุนเทียน",
    "ภาษีเจริญ", "หนองแขม", "ราษฎร์บูรณะ", "บางพลัด",
    "ดินแดง", "บึงกุ่ม", "สาทร", "บางซื่อ", "จตุจักร",
    "บางคอแหลม", "ประเวศ", "คลองเตย", "สวนหลวง",
    "จอมทอง", "ดอนเมือง", "ราชเทวี", "ลาดพร้าว", "วัฒนา",
    "บางแค", "หลักสี่", "สายไหม", "คันนายาว", "สะพานสูง",
    "วังทองหลาง", "คลองสามวา", "บางนา", "ทวีวัฒนา",
    "ทุ่งครุ", "บางบอน",
}
EXPECTED_FIELDS = {
    "_id", "ชื่อกลุ่มโรค", "อายุ (เต็ม) ปี", "อายุ (เต็ม) เดือน", "อาย (เต็ม) วัน",
    "เพศ", "สถานภาพสมรส", "สัญชาติ", "อาชีพ", "จังหวัด", "อำเภอ/เขต",
    "ตำบล/แขวง", "วันที่เริ่มป่วย", "สภาพผู้ป่วย", "ประเภทผู้ป่วย", "สถานที่รักษา",
}
# Low-memory multipart upload: the default (10 threads x 8 MB buffers per file,
# two years in parallel) exhausted the container's RAM on the ~90 MB CSVs.
UPLOAD_CONFIG = TransferConfig(
    multipart_threshold=16 * 1024 * 1024,
    multipart_chunksize=8 * 1024 * 1024,
    max_concurrency=2,
)
# The full CSV download is the original file: same fields without the API's _id.
CSV_EXPECTED_FIELDS = EXPECTED_FIELDS - {"_id"}


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name=AWS_REGION,
    )


def ensure_bucket():
    client = _s3_client()
    bucket = os.environ["DATA_LAKE_BUCKET"]
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        client.create_bucket(Bucket=bucket)


def _validate_disease(payload: bytes, source_name: str) -> list:
    """Check the raw JSON against the 16-field data contract; return the records."""
    try:
        records = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{source_name}: invalid UTF-8 JSON: {error}") from error
    if not isinstance(records, list) or not records:
        raise ValueError(f"{source_name}: expected a non-empty JSON array of records")
    if set(records[0]) != EXPECTED_FIELDS:
        raise ValueError(f"{source_name}: source fields do not match the 16-field data contract")
    if any(set(record) != EXPECTED_FIELDS for record in records):
        raise ValueError(f"{source_name}: inconsistent fields between records")
    return records


def _validate_disease_csv(source: Path) -> int:
    """Stream-check the full CSV download against the 15-field file contract.

    The original CSV has no _id (the datastore API adds it), so it carries
    EXPECTED_FIELDS minus _id. Returns the number of data rows.
    """
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = [column.strip() for column in next(reader, [])]
        if len(header) != len(CSV_EXPECTED_FIELDS) or set(header) != CSV_EXPECTED_FIELDS:
            raise ValueError(
                f"{source.name}: header does not match the 15-field CSV contract; got {header}"
            )
        rows = 0
        for line_number, row in enumerate(reader, start=2):
            if len(row) != len(header):
                raise ValueError(
                    f"{source.name}:{line_number}: expected {len(header)} columns, got {len(row)}"
                )
            rows += 1
    if rows == 0:
        raise ValueError(f"{source.name}: no data rows")
    return rows


def _disease_source(year: str) -> Path:
    """Newest full download (disease_cases_{year}_full_YYYYMMDD.csv), else the
    legacy unversioned full file, else the 100-record API sample."""
    from spark.raw_versions import latest_per_year  # project package on PYTHONPATH

    base = Path(os.environ["RAW_DATA_DIR"]) / "disease"
    chosen = latest_per_year(str(path) for path in base.glob(f"disease_cases_{year}_*"))
    if year not in chosen:
        raise FileNotFoundError(
            f"Required raw input is missing for {year}: disease_cases_{year}_full_*.csv "
            f"or disease_cases_{year}_sample.json"
        )
    return Path(chosen[year])


def _file_sha256(source: Path) -> str:
    digest = hashlib.sha256()
    with source.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def land_disease(year: str):
    source = _disease_source(year)
    if source.suffix == ".csv":
        record_count = _validate_disease_csv(source)
        file_format, content_type = "csv", "text/csv"
        origin = "data.bangkok.go.th resource CSV (full download)"
    else:
        record_count = len(_validate_disease(source.read_bytes(), source.name))
        file_format, content_type = "json", JSON_CONTENT_TYPE
        origin = "Data.go.th Data API (sample)"

    digest = _file_sha256(source)
    bucket = os.environ["DATA_LAKE_BUCKET"]
    key = f"raw/disease/year={year}/{source.name}"
    client = _s3_client()

    try:
        existing = client.head_object(Bucket=bucket, Key=key)
        existing_digest = existing.get("Metadata", {}).get("sha256")
        if existing_digest != digest:
            raise ValueError(
                f"Raw object already exists with different content: s3://{bucket}/{key}. "
                "Use a new versioned filename/key to preserve raw history."
            )
        print(f"Already landed; checksum unchanged: s3://{bucket}/{key}")
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
        # upload_file streams from disk (multipart for large files).
        client.upload_file(
            str(source),
            bucket,
            key,
            ExtraArgs={
                "ContentType": content_type,
                "Metadata": {
                    "sha256": digest,
                    "record-count": str(record_count),
                    "source-year-be": year,
                },
            },
            Config=UPLOAD_CONFIG,
        )

    manifest = {
        "dataset": "disease_cases",
        "source": origin,
        "year_be": year,
        "filename": source.name,
        "object_key": key,
        "format": file_format,
        "record_count": record_count,
        "size_bytes": source.stat().st_size,
        "sha256": digest,
        "landed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    client.put_object(
        Bucket=bucket,
        Key=f"raw/disease/year={year}/_metadata/{source.stem}.manifest.json",
        Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType=JSON_CONTENT_TYPE,
    )
    print(f"Landed {record_count} records: s3://{bucket}/{key}; sha256={digest}")


def _env_flag(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes"}


def download_disease(year: str):
    """Download the year's full CSV as a new raw version (same code as the CLI).

    Skipped when downloads are disabled or no Data.go.th token is configured;
    the land task then uses the newest file already on disk.
    """
    if not _env_flag("DOWNLOAD_DISEASE", "true"):
        raise AirflowSkipException("DOWNLOAD_DISEASE is off; landing files already on disk")
    if not os.environ.get("DATA_GO_TH_TOKEN"):
        raise AirflowSkipException("DATA_GO_TH_TOKEN not set; landing files already on disk")

    from src.extract.extract_disease import RESOURCES, download_disease_full

    status, path = download_disease_full(
        year, RESOURCES[year], raw_dir=Path(os.environ["RAW_DATA_DIR"]) / "disease"
    )
    print(f"{year}: {status} -> {path.name}")


def population_required() -> bool:
    """Population is optional unless REQUIRE_POPULATION is set (cases-only mode)."""
    return os.environ.get("REQUIRE_POPULATION", "false").strip().lower() in {"1", "true", "yes"}


def _population_source(year: str) -> Path:
    return (
        Path(os.environ["RAW_DATA_DIR"])
        / POPULATION_REFERENCE_DIR
        / f"population_summary_{year}.csv"
    )


def _validate_population(year: str, payload: bytes, source: Path) -> int:
    try:
        rows = list(csv.DictReader(payload.decode("utf-8-sig").splitlines()))
    except (UnicodeDecodeError, csv.Error) as error:
        raise ValueError(f"{source.name}: invalid UTF-8 CSV: {error}") from error

    actual_fields = tuple(rows[0].keys()) if rows else ()
    if actual_fields != POPULATION_FIELDS:
        raise ValueError(
            f"{source.name}: expected columns {POPULATION_FIELDS}, "
            f"got {actual_fields}"
        )

    districts = []
    for row_number, row in enumerate(rows, start=2):
        if row["ปี"].strip() != year:
            raise ValueError(
                f"{source.name}:{row_number}: year must be {year}"
            )
        district = row["เขต"].strip()
        if not district:
            raise ValueError(f"{source.name}:{row_number}: district is empty")
        try:
            population = int(row["ประชากรรวม"].strip())
        except ValueError as error:
            raise ValueError(
                f"{source.name}:{row_number}: population must be an integer"
            ) from error
        if population <= 0:
            raise ValueError(
                f"{source.name}:{row_number}: population must be positive"
            )
        districts.append(district)

    duplicate_districts = sorted(
        district for district in set(districts) if districts.count(district) > 1
    )
    missing = sorted(EXPECTED_BANGKOK_DISTRICTS - set(districts))
    unexpected = sorted(set(districts) - EXPECTED_BANGKOK_DISTRICTS)
    if duplicate_districts or missing or unexpected:
        raise ValueError(
            f"{source.name}: population coverage is not exactly Bangkok's "
            f"50 districts; duplicates={duplicate_districts}, "
            f"missing={missing}, unexpected={unexpected}"
        )

    return len(rows)


def land_population(year: str):
    source = _population_source(year)
    if not source.is_file():
        if not population_required():
            raise AirflowSkipException(
                f"Population reference not found ({source.name}); "
                "running in cases-only mode. Set REQUIRE_POPULATION=true to fail instead."
            )
        raise FileNotFoundError(
            f"Required population reference is missing: {source}"
        )

    payload = source.read_bytes()
    record_count = _validate_population(year, payload, source)
    digest = hashlib.sha256(payload).hexdigest()
    bucket = os.environ["DATA_LAKE_BUCKET"]
    key = f"raw/population/year={year}/{source.name}"
    client = _s3_client()

    try:
        existing = client.head_object(Bucket=bucket, Key=key)
        existing_digest = existing.get("Metadata", {}).get("sha256")
        if existing_digest != digest:
            raise ValueError(
                f"Raw object already exists with different content: s3://{bucket}/{key}. "
                "Use a new versioned filename/key to preserve raw history."
            )
        print(f"Already landed; checksum unchanged: s3://{bucket}/{key}")
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ContentType="text/csv",
            Metadata={
                "sha256": digest,
                "record-count": str(record_count),
                "source-year-be": year,
                "coverage": "Bangkok-50-districts",
            },
        )

    manifest = {
        "dataset": "population_reference",
        "source": "Bangkok population reference CSV",
        "year_be": year,
        "filename": source.name,
        "object_key": key,
        "format": "csv",
        "record_count": record_count,
        "district_count": len(EXPECTED_BANGKOK_DISTRICTS),
        "coverage": "Bangkok-50-districts",
        "size_bytes": len(payload),
        "sha256": digest,
        "landed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    client.put_object(
        Bucket=bucket,
        Key=f"raw/population/year={year}/_metadata/{source.stem}.manifest.json",
        Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType=JSON_CONTENT_TYPE,
    )
    print(
        f"Landed {record_count} population rows: "
        f"s3://{bucket}/{key}; sha256={digest}"
    )


with DAG(
    dag_id="disease_raw_to_lake",
    description="Validate disease data (full CSV or API sample) and population, land immutable raw objects in S3",
    # Weekly refresh: the source CSVs are updated in place by the publisher.
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    schedule="0 6 * * 1",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2},
    tags=["data-lake", "raw", "disease"],
) as dag:
    create_bucket = PythonOperator(task_id="ensure_raw_bucket", python_callable=ensure_bucket)
    for data_year in YEARS:
        download = PythonOperator(
            task_id=f"download_disease_{data_year}",
            python_callable=download_disease,
            op_kwargs={"year": data_year},
        )
        upload = PythonOperator(
            task_id=f"land_disease_{data_year}",
            python_callable=land_disease,
            op_kwargs={"year": data_year},
            # Land the newest file on disk even if today's download was
            # skipped or failed (the failed download stays red in the UI).
            trigger_rule="all_done",
        )
        create_bucket >> download >> upload  # pyright: ignore[reportUnusedExpression]
        population_upload = PythonOperator(
            task_id=f"land_population_{data_year}",
            python_callable=land_population,
            op_kwargs={"year": data_year},
        )
        create_bucket >> population_upload  # pyright: ignore[reportUnusedExpression]

    trigger_spark = TriggerDagRunOperator(
        task_id="trigger_spark_processing",
        trigger_dag_id="spark_processing",
        wait_for_completion=False,
        # Skipped population tasks (cases-only mode) must not block Spark.
        trigger_rule="none_failed",
    )
    create_bucket >> trigger_spark  # pyright: ignore[reportUnusedExpression]
    for data_year in YEARS:
        trigger_spark.set_upstream(
            dag.task_dict[f"land_disease_{data_year}"]
        )
        trigger_spark.set_upstream(
            dag.task_dict[f"land_population_{data_year}"]
        )
