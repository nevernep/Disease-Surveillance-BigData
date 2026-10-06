from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.config import Settings
from spark.deduplication import count_duplicate_keys
from spark.warehouse import FACT_CASES_KEY
from spark.schemas import BANGKOK_DISTRICTS, GOLD_PRIMARY_KEY


@dataclass
class QualityCheck:
    layer: str
    rule: str
    observed: str
    threshold: str
    status: str


def _status(condition: bool) -> str:
    return "PASS" if condition else "FAIL"


SKIP = "SKIP"


def run_quality_checks(
    spark: SparkSession,
    raw: DataFrame,
    cleaned: DataFrame,
    standardized: DataFrame,
    curated: DataFrame,
    removed_duplicates: int,
    settings: Settings,
    canonical: Optional[DataFrame] = None,
    population_available: bool = True,
    warehouse: Optional[Dict[str, DataFrame]] = None,
) -> Tuple[DataFrame, bool]:
    """ตรวจกฎคุณภาพข้อมูลทั้งหมดและคืนผลเป็น DataFrame"""

    raw_rows = raw.count()
    clean_rows = cleaned.count()
    standardized_rows = standardized.count()
    curated_rows = curated.count()

    invalid_districts = standardized.filter(
        ~F.col("district_name").isin(BANGKOK_DISTRICTS)
    ).count()

    missing_population = curated.filter(
        F.col("population").isNull() | (F.col("population") <= 0)
    ).count()

    population_covered_rows = curated_rows - missing_population
    population_uncovered_rate = (
        missing_population / curated_rows if curated_rows else 0.0
    )
    # Real rate (was hard-coded to 0). Only enforced when population was
    # loaded; in cases-only mode every row is uncovered by design.
    missing_population_rate = population_uncovered_rate
    population_coverage_rate = (
        population_covered_rows / curated_rows
        if curated_rows
        else 0.0
    )
    coverage_accounting_error = (
        population_covered_rows + missing_population - curated_rows
    )

    non_positive_cases = curated.filter(
        F.col("total_cases") <= 0
    ).count()

    duplicate_keys = count_duplicate_keys(
        curated, GOLD_PRIMARY_KEY
    )

    unavailable_rate_rows = curated.filter(
        F.col("incidence_rate_per_100k").isNull()
    ).count()

    formula_mismatch_rows = curated.filter(
        (F.col("population") > 0)
        & (
            F.col("incidence_rate_per_100k").isNull()
            | (
                F.abs(
                    F.col("incidence_rate_per_100k")
                    - (
                        F.col("total_cases")
                        / F.col("population")
                        * F.lit(100000)
                    )
                ) > F.lit(0.01)
            )
        )
    ).count()

    validation_source = canonical if canonical is not None else cleaned
    missing_report_dates = validation_source.filter(
        F.col("report_date").isNull()
    ).count()
    # Invalid ages must not survive cleaning; rows caught upstream are
    # quarantined and reported separately (informational).
    invalid_ages = cleaned.filter(
        F.col("age").isNotNull()
        & ~F.col("age").between(0, settings.maximum_age)
    ).count()
    quarantined_rows = (
        canonical.count() - clean_rows if canonical is not None else 0
    )
    # year_be comes from the resource/file year; flag cases whose onset date
    # falls in another year (e.g. if a resource were a fiscal year).
    year_date_mismatch_rows = cleaned.filter(
        F.col("report_date").isNotNull()
        & (F.col("year_be") != F.year("report_date") + F.lit(543))
    ).count()
    required_columns = {
        "year_be", "disease_name_raw", "district_name_raw",
        "case_count", "report_date", "age",
    }
    missing_schema_columns = sorted(
        required_columns - set(validation_source.columns)
    )

    checks: List[QualityCheck] = [
        QualityCheck(
            "raw", "raw_rows_not_empty",
            str(raw_rows), "> 0", _status(raw_rows > 0),
        ),
        QualityCheck(
            "clean", "clean_rows_not_empty",
            str(clean_rows), "> 0", _status(clean_rows > 0),
        ),
        QualityCheck(
            "clean", "clean_retention_rate",
            f"{(clean_rows / raw_rows if raw_rows else 0):.4f}",
            ">= 0.5",
            _status(raw_rows > 0 and clean_rows / raw_rows >= 0.5),
        ),
        QualityCheck(
            "clean", "duplicate_rows_removed",
            str(removed_duplicates), "informational", "PASS",
        ),
        QualityCheck(
            "standardized", "standardized_rows_not_empty",
            str(standardized_rows), "> 0",
            _status(standardized_rows > 0),
        ),
        QualityCheck(
            "standardized", "district_in_bangkok_master",
            str(invalid_districts), "= 0",
            _status(invalid_districts == 0),
        ),
        QualityCheck(
            "gold", "curated_rows_not_empty",
            str(curated_rows), "> 0", _status(curated_rows > 0),
        ),
        QualityCheck(
            "gold", "missing_population_rate",
            f"{missing_population_rate:.4f}",
            (
                f"<= {settings.max_missing_population_rate:.4f}"
                if population_available
                else "cases-only mode (no population)"
            ),
            (
                _status(
                    missing_population_rate
                    <= settings.max_missing_population_rate
                )
                if population_available
                else SKIP
            ),
        ),
        QualityCheck(
            "gold", "population_coverage_rate",
            f"{population_coverage_rate:.4f}",
            "informational", "PASS",
        ),
        QualityCheck(
            "gold", "population_uncovered_rows",
            str(missing_population), "informational", "PASS",
        ),
        QualityCheck(
            "gold", "population_uncovered_rate",
            f"{population_uncovered_rate:.4f}",
            "informational", "PASS",
        ),
        QualityCheck(
            "gold", "population_coverage_accounting",
            str(coverage_accounting_error), "= 0",
            _status(coverage_accounting_error == 0),
        ),
        QualityCheck(
            "gold", "non_positive_total_case_rows",
            str(non_positive_cases), "= 0",
            _status(non_positive_cases == 0),
        ),
        QualityCheck(
            "gold", "primary_key_unique",
            str(duplicate_keys), "= 0",
            _status(duplicate_keys == 0),
        ),
        QualityCheck(
            "gold", "incidence_rate_unavailable_rows",
            str(unavailable_rate_rows), "informational", "PASS",
        ),
        QualityCheck(
            "gold", "incidence_unavailable_matches_uncovered",
            str(unavailable_rate_rows - missing_population), "= 0",
            _status(unavailable_rate_rows == missing_population),
        ),
        QualityCheck(
            "gold", "incidence_formula_mismatch_rows",
            str(formula_mismatch_rows), "= 0",
            _status(formula_mismatch_rows == 0),
        ),
        QualityCheck(
            "dq", "required_schema_columns",
            ", ".join(missing_schema_columns) or "complete",
            "complete", _status(not missing_schema_columns),
        ),
        QualityCheck(
            "dq", "missing_report_date_rows",
            str(missing_report_dates), "informational", "PASS",
        ),
        QualityCheck(
            "dq", "invalid_age_rows_after_clean",
            str(invalid_ages), "= 0",
            _status(invalid_ages == 0),
        ),
        QualityCheck(
            "clean", "quarantined_rows",
            str(quarantined_rows), "informational", "PASS",
        ),
        QualityCheck(
            "clean", "year_be_vs_report_date_mismatch_rows",
            str(year_date_mismatch_rows), "informational", "PASS",
        ),
    ]

    if warehouse is not None:
        checks.extend(_warehouse_checks(standardized, warehouse))

    report = spark.createDataFrame(
        [check.__dict__ for check in checks]
    ).select("layer", "rule", "observed", "threshold", "status")

    has_failure = any(check.status == "FAIL" for check in checks)

    return report, has_failure


def _warehouse_checks(
    standardized: DataFrame,
    warehouse: Dict[str, DataFrame],
) -> List[QualityCheck]:
    """Reconcile the star schema against the case-level source."""

    fact = warehouse["fact_disease_cases"]
    source_cases = standardized.agg(
        F.coalesce(F.round(F.sum("case_count")), F.lit(0)).cast("long")
    ).first()[0]
    fact_cases = fact.agg(
        F.coalesce(F.sum("total_cases"), F.lit(0)).cast("long")
    ).first()[0]
    duplicate_fact_keys = count_duplicate_keys(fact, FACT_CASES_KEY)
    unmatched_date_keys = fact.join(
        warehouse["dim_date"], "date_key", "left_anti"
    ).count()

    return [
        QualityCheck(
            "warehouse", "fact_cases_reconcile_with_standardized",
            str(fact_cases - source_cases), "= 0",
            _status(fact_cases == source_cases),
        ),
        QualityCheck(
            "warehouse", "fact_primary_key_unique",
            str(duplicate_fact_keys), "= 0",
            _status(duplicate_fact_keys == 0),
        ),
        QualityCheck(
            "warehouse", "fact_date_key_in_dim_date",
            str(unmatched_date_keys), "= 0",
            _status(unmatched_date_keys == 0),
        ),
    ]
