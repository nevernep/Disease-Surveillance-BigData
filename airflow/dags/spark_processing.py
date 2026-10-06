"""Run the Spark processing pipeline, then load the star schema into PostgreSQL."""

from datetime import datetime, timedelta

# Airflow is installed in the Docker image, not the local interpreter.
from airflow import DAG  # pyright: ignore[reportAttributeAccessIssue, reportMissingImports]
from airflow.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]


PROJECT_ROOT = "/opt/airflow/project"


def run_spark_pipeline():
    import os
    import subprocess

    command = [
        "python",
        "-m",
        "spark.main",
        "--project-root",
        PROJECT_ROOT,
        "--fail-on-dq",
    ]
    require_population = os.environ.get(
        "REQUIRE_POPULATION", "false"
    ).strip().lower() in {"1", "true", "yes"}
    if not require_population:
        # Cases-only mode: incidence rate stays blank when population is absent.
        command.append("--allow-missing-population")

    # Stream Spark output into the Airflow task log (a bare subprocess writes
    # to the worker's stdout, which never reaches the UI).
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"},
    )
    assert process.stdout is not None
    for line in process.stdout:
        line = line.rstrip()
        # Skip Spark's carriage-return progress bars.
        if line and "[Stage " not in line:
            print(line, flush=True)
    returncode = process.wait()
    if returncode != 0:
        raise RuntimeError(
            f"Spark processing pipeline failed with exit code {returncode}"
        )


def load_warehouse():
    # Imported at run time: the project package is on PYTHONPATH only in the container.
    from pathlib import Path

    from spark.load_warehouse import load_warehouse as load

    for table, rows in load(Path(PROJECT_ROOT)).items():
        print(f"Loaded {rows} rows into mart.{table}")


with DAG(
    dag_id="spark_processing",
    description="Process landed data with Spark, then load the PostgreSQL warehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["spark", "curated", "warehouse"],
) as dag:
    run_spark = PythonOperator(
        task_id="run_spark_pipeline",
        python_callable=run_spark_pipeline,
    )

    load_dw = PythonOperator(
        task_id="load_warehouse",
        python_callable=load_warehouse,
    )

    run_spark >> load_dw  # pyright: ignore[reportUnusedExpression]
