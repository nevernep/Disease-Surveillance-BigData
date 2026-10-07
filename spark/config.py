import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pyspark.sql import SparkSession


def is_remote_uri(location: object) -> bool:
    """True เมื่อเป็น URI ของ Data Lake เช่น s3a://bucket/..."""

    return isinstance(location, str) and "://" in location


@dataclass
class ProjectPaths:
    """รวม Path ทั้งหมดที่ Part 3 ใช้งาน

    เมื่อกำหนด lake_uri (เช่น s3a://disease-surveillance) ชั้น Raw จะอ่านจาก
    Data Lake และชั้น Parquet (clean/standardized/quarantine/gold/warehouse)
    จะเขียนกลับขึ้น Data Lake ส่วนไฟล์ CSV สำหรับ BI ยังเขียนลง data/processed
    """

    root: Path
    lake_uri: Optional[str] = None

    def _layer(self, *parts: str) -> str:
        """ตำแหน่งชั้นข้อมูล Parquet: บน Data Lake หรือในเครื่อง"""

        if self.lake_uri:
            return "/".join([self.lake_uri.rstrip("/"), "processed", *parts])
        return str(self.processed_dir.joinpath(*parts))

    @property
    def lake_raw_disease_glob(self) -> str:
        return f"{self._require_lake()}/raw/disease/year=*/disease_cases_*"

    @property
    def lake_raw_population_glob(self) -> str:
        return f"{self._require_lake()}/raw/population/year=*/population_summary_*.csv"

    def _require_lake(self) -> str:
        if not self.lake_uri:
            raise ValueError("lake_uri is not configured")
        return self.lake_uri.rstrip("/")

    @property
    def raw_disease_dir(self) -> Path:
        return self.root / "data" / "raw" / "disease"

    @property
    def raw_reference_dir(self) -> Path:
        return self.raw_disease_dir / "reference"

    @property
    def processed_dir(self) -> Path:
        return self.root / "data" / "processed"

    @property
    def clean_dir(self) -> str:
        return self._layer("clean", "disease_cleaned")

    @property
    def standardized_dir(self) -> str:
        return self._layer("standardized", "disease_standardized")

    @property
    def gold_dir(self) -> Path:
        return (
            self.processed_dir
            / "gold"
            / "disease_with_population"
        )

    @property
    def gold_csv(self) -> Path:
        return self.gold_dir / "curated_disease_data.csv"

    @property
    def gold_parquet(self) -> str:
        return self._layer(
            "gold", "disease_with_population", "curated_disease_data"
        )

    @property
    def warehouse_dir(self) -> Path:
        """โฟลเดอร์ CSV ของ star schema สำหรับ Power BI (ในเครื่องเสมอ)"""

        return self.processed_dir / "warehouse"

    def warehouse_table(self, table_name: str) -> str:
        return self._layer("warehouse", table_name)

    @property
    def quarantine_dir(self) -> str:
        return self._layer("quarantine", "disease_rejected")

    @property
    def quality_parquet(self) -> str:
        return self._layer("quality", "data_quality_report")

    @property
    def quality_csv(self) -> Path:
        return (
            self.processed_dir
            / "quality"
            / "data_quality_report.csv"
        )

    @property
    def error_log_csv(self) -> Path:
        return self.processed_dir / "quality" / "data_quality_errors.csv"


@dataclass
class Settings:
    """ค่าตั้งต้นที่อ่านจากไฟล์ .env"""

    spark_master: str = "local[*]"
    spark_log_level: str = "WARN"
    timezone: str = "Asia/Bangkok"
    shuffle_partitions: int = 8
    max_missing_population_rate: float = 0.05
    minimum_year_be: int = 2400
    maximum_year_be: int = 2700
    maximum_age: int = 120
    valid_sex_values: tuple = field(
        default_factory=lambda: ("M", "F", "U")
    )
    data_lake_uri: Optional[str] = None
    s3_endpoint: Optional[str] = None
    s3_access_key: Optional[str] = None
    s3_secret_key: Optional[str] = None
    extra_jars: Optional[str] = None
    driver_memory: Optional[str] = None

    @classmethod
    def from_env(cls, project_root: Path) -> "Settings":
        load_dotenv(project_root / ".env")

        return cls(
            spark_master=os.getenv(
                "SPARK_MASTER", "local[*]"
            ),
            spark_log_level=os.getenv(
                "SPARK_LOG_LEVEL", "WARN"
            ),
            timezone=os.getenv(
                "SPARK_TIMEZONE", "Asia/Bangkok"
            ),
            shuffle_partitions=int(
                os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")
            ),
            max_missing_population_rate=float(
                os.getenv(
                    "DQ_MAX_MISSING_POPULATION_RATE",
                    "0.05",
                )
            ),
            data_lake_uri=os.getenv("DATA_LAKE_URI") or None,
            s3_endpoint=os.getenv("S3_ENDPOINT") or None,
            s3_access_key=os.getenv("S3_ACCESS_KEY") or None,
            s3_secret_key=os.getenv("S3_SECRET_KEY") or None,
            extra_jars=os.getenv("SPARK_JARS") or None,
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY") or None,
        )


def create_spark_session(
    settings: Settings,
    app_name: str = "DiseaseSurveillancePart3",
) -> SparkSession:
    """สร้าง SparkSession ตามค่าใน Settings"""

    builder = SparkSession.builder

    if settings.extra_jars:
        builder = builder.config("spark.jars", settings.extra_jars)

    # local[N]: make the JVM see N CPUs too, so GC/compiler threads don't
    # saturate every core and starve Airflow running in the same container.
    local_cores = re.fullmatch(r"local\[(\d+)\]", settings.spark_master)
    if local_cores:
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-XX:ActiveProcessorCount={local_cores.group(1)}",
        )

    if settings.driver_memory:
        # Applies because the JVM is launched by this session (not reused).
        builder = builder.config("spark.driver.memory", settings.driver_memory)

    if settings.data_lake_uri:
        if not settings.s3_endpoint:
            raise ValueError("DATA_LAKE_URI ต้องกำหนด S3_ENDPOINT ด้วย")
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key or "")
            .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key or "")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            # SeaweedFS/MinIO ใช้ path-style และในเครื่องไม่มี TLS
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(settings.s3_endpoint.startswith("https")).lower(),
            )
        )

    session = (
        builder
        .appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", settings.timezone)
        .config(
            "spark.sql.shuffle.partitions",
            settings.shuffle_partitions,
        )
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy",
        )
        .config(
            "spark.sql.legacy.timeParserPolicy",
            "CORRECTED",
        )
        .getOrCreate()
    )

    session.sparkContext.setLogLevel(settings.spark_log_level)

    return session