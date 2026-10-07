"""DAG structure and raw-landing validation tests (run where Airflow is installed)."""

import importlib.util
import json
import os
from pathlib import Path

import pytest

pytest.importorskip("airflow")

# Repo checkout: airflow/dags; inside the Airflow container: $AIRFLOW_HOME/dags.
DAG_FOLDER = Path(__file__).resolve().parents[1] / "airflow" / "dags"
if not DAG_FOLDER.is_dir():
    DAG_FOLDER = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "dags"
FIELDS = [
    "_id", "ชื่อกลุ่มโรค", "อายุ (เต็ม) ปี", "อายุ (เต็ม) เดือน", "อาย (เต็ม) วัน",
    "เพศ", "สถานภาพสมรส", "สัญชาติ", "อาชีพ", "จังหวัด", "อำเภอ/เขต",
    "ตำบล/แขวง", "วันที่เริ่มป่วย", "สภาพผู้ป่วย", "ประเภทผู้ป่วย", "สถานที่รักษา",
]


@pytest.fixture(scope="module")
def raw_dag_module():
    spec = importlib.util.spec_from_file_location(
        "disease_raw_to_lake", DAG_FOLDER / "disease_raw_to_lake.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert {"disease_raw_to_lake", "spark_processing"} <= set(dagbag.dag_ids)


def test_raw_dag_triggers_spark_even_when_population_is_skipped(dagbag):
    dag = dagbag.get_dag("disease_raw_to_lake")
    trigger = dag.get_task("trigger_spark_processing")

    assert trigger.trigger_rule == "none_failed"
    assert {
        "land_disease_2568", "land_disease_2569",
        "land_population_2568", "land_population_2569",
    } <= trigger.upstream_task_ids


def test_spark_dag_loads_warehouse_after_spark(dagbag):
    dag = dagbag.get_dag("spark_processing")

    assert dag.get_task("load_warehouse").upstream_task_ids == {
        "run_spark_pipeline"
    }


def _record(**overrides):
    record = {field: "x" for field in FIELDS}
    record.update(overrides)
    return record


def test_validate_disease_accepts_any_non_empty_extract(raw_dag_module):
    payload = json.dumps([_record() for _ in range(3)]).encode("utf-8")

    assert len(raw_dag_module._validate_disease(payload, "f.json")) == 3


@pytest.mark.parametrize(
    "payload",
    [
        b"[]",
        b"not json",
        json.dumps([{"_id": 1}]).encode("utf-8"),
        json.dumps([_record(), {"_id": 2}]).encode("utf-8"),
    ],
    ids=["empty", "invalid-json", "wrong-fields", "inconsistent-fields"],
)
def test_validate_disease_rejects_contract_violations(raw_dag_module, payload):
    with pytest.raises(ValueError):
        raw_dag_module._validate_disease(payload, "f.json")


def _population_csv(districts, year="2568"):
    lines = ["ปี,เขต,ประชากรรวม"] + [f"{year},{name},1000" for name in districts]
    return "\n".join(lines).encode("utf-8-sig")


def test_validate_population_requires_all_50_districts(raw_dag_module):
    districts = sorted(raw_dag_module.EXPECTED_BANGKOK_DISTRICTS)
    source = Path("population_summary_2568.csv")

    assert raw_dag_module._validate_population(
        "2568", _population_csv(districts), source
    ) == 50

    with pytest.raises(ValueError, match="missing"):
        raw_dag_module._validate_population(
            "2568", _population_csv(districts[:-1]), source
        )
    with pytest.raises(ValueError, match="year must be"):
        raw_dag_module._validate_population(
            "2568", _population_csv(districts, year="2569"), source
        )


@pytest.mark.parametrize(
    ("value", "expected"),
    [("true", True), ("1", True), ("YES", True), ("false", False), ("", False)],
)
def test_population_required_flag(raw_dag_module, monkeypatch, value, expected):
    monkeypatch.setenv("REQUIRE_POPULATION", value)

    assert raw_dag_module.population_required() is expected


def _write_csv(path, header, rows):
    path.write_text(
        "\r\n".join([",".join(header)] + [",".join(row) for row in rows]) + "\r\n",
        encoding="utf-8-sig",
    )


def test_validate_disease_csv_counts_rows(raw_dag_module, tmp_path):
    header = [field for field in FIELDS if field != "_id"]
    path = tmp_path / "disease_cases_2568_full.csv"
    _write_csv(path, header, [["x"] * 15, ["y"] * 15])

    assert raw_dag_module._validate_disease_csv(path) == 2


@pytest.mark.parametrize(
    ("header", "rows", "message"),
    [
        (FIELDS, [["x"] * 16], "15-field"),             # API header with _id
        (None, [["x"] * 15, ["x"] * 14], "columns"),    # ragged row
        (None, [], "no data rows"),
    ],
    ids=["has-_id", "ragged-row", "empty"],
)
def test_validate_disease_csv_rejects_violations(
    raw_dag_module, tmp_path, header, rows, message
):
    header = header or [field for field in FIELDS if field != "_id"]
    path = tmp_path / "disease_cases_2568_full.csv"
    _write_csv(path, header, rows)

    with pytest.raises(ValueError, match=message):
        raw_dag_module._validate_disease_csv(path)


def test_disease_source_prefers_full_csv(raw_dag_module, tmp_path, monkeypatch):
    disease_dir = tmp_path / "disease"
    disease_dir.mkdir()
    (disease_dir / "disease_cases_2568_sample.json").write_text("[]")
    monkeypatch.setenv("RAW_DATA_DIR", str(tmp_path))

    assert raw_dag_module._disease_source("2568").name == "disease_cases_2568_sample.json"

    (disease_dir / "disease_cases_2568_full.csv").write_text("h\n")
    assert raw_dag_module._disease_source("2568").name == "disease_cases_2568_full.csv"

    with pytest.raises(FileNotFoundError):
        raw_dag_module._disease_source("2569")
