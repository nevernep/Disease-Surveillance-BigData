import pytest

from spark.raw_versions import (
    latest_full,
    latest_per_year,
    select_disease_files,
    versioned_full_name,
)

LAKE = "s3a://b/raw/disease"
FILES = [
    f"{LAKE}/year=2568/disease_cases_2568_sample.json",
    f"{LAKE}/year=2568/disease_cases_2568_full.csv",            # legacy, unversioned
    f"{LAKE}/year=2569/disease_cases_2569_sample.json",
    f"{LAKE}/year=2569/disease_cases_2569_full.csv",
    f"{LAKE}/year=2569/disease_cases_2569_full_20261008.csv",
    f"{LAKE}/year=2569/disease_cases_2569_full_20261102.csv",   # newest
    f"{LAKE}/year=2569/disease_cases_2569_full_20261201.csv.part",
    f"{LAKE}/year=2569/_metadata/disease_cases_2569_full_20261102.manifest.json",
    f"{LAKE}/year=2570/disease_cases_2570_sample.json",         # sample only
]


def test_newest_full_download_wins_per_year():
    assert latest_per_year(FILES) == {
        "2568": f"{LAKE}/year=2568/disease_cases_2568_full.csv",
        "2569": f"{LAKE}/year=2569/disease_cases_2569_full_20261102.csv",
        "2570": f"{LAKE}/year=2570/disease_cases_2570_sample.json",
    }


def test_select_is_ordered_by_year_and_ignores_partial_and_manifest_files():
    selected = select_disease_files(FILES)

    assert [path.rsplit("/", 1)[-1] for path in selected] == [
        "disease_cases_2568_full.csv",
        "disease_cases_2569_full_20261102.csv",
        "disease_cases_2570_sample.json",
    ]


def test_windows_paths_are_recognised():
    path = r"D:\data\raw\disease\disease_cases_2569_full_20261008.csv"

    assert latest_per_year([path]) == {"2569": path}


def test_latest_full_ignores_the_api_sample():
    assert latest_full(FILES, "2570") is None
    assert latest_full(FILES, "2569").endswith("disease_cases_2569_full_20261102.csv")


def test_versioned_full_name():
    assert versioned_full_name("2569", "20261008") == "disease_cases_2569_full_20261008.csv"
    with pytest.raises(ValueError):
        versioned_full_name("2569", "2026-10-08")
