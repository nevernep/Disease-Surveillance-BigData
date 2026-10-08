"""Pick the raw disease file to use per year (shared by extract, DAG and Spark).

Raw files are immutable, so a new download of a resource gets a new name:

    disease_cases_{year}_full_{YYYYMMDD}.csv   full download (versioned)
    disease_cases_{year}_full.csv              full download (legacy, unversioned)
    disease_cases_{year}_sample.json           100-record API sample

Per year the newest full download wins; an unversioned full file counts as
the oldest version; the API sample is used only when no full file exists.
Does not import pyspark.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional, Tuple

DISEASE_FILE_PATTERN = re.compile(
    r"disease_cases_(?P<year>\d{4})_(?P<kind>full|sample)(?:_(?P<version>\d{8}))?"
    r"\.(?P<ext>csv|json)$"
)


def _rank(match: "re.Match[str]") -> Tuple[int, str]:
    """Higher sorts later: any full file beats the sample; newer beats older."""
    if match.group("kind") == "sample":
        return (0, "")
    return (1, match.group("version") or "00000000")


def latest_per_year(paths: Iterable[str]) -> Dict[str, str]:
    """Map year -> chosen path. Paths that don't match the pattern are ignored."""
    chosen: Dict[str, Tuple[Tuple[int, str], str]] = {}
    for path in paths:
        match = DISEASE_FILE_PATTERN.search(str(path).replace("\\", "/"))
        if match is None:
            continue
        year, rank = match.group("year"), _rank(match)
        if year not in chosen or rank > chosen[year][0]:
            chosen[year] = (rank, str(path))
    return {year: chosen[year][1] for year in sorted(chosen)}


def select_disease_files(paths: Iterable[str]) -> List[str]:
    """Chosen file per year, ordered by year."""
    return list(latest_per_year(paths).values())


def latest_full(paths: Iterable[str], year: str) -> Optional[str]:
    """Newest full download for one year, ignoring the API sample."""
    full = [
        path for path in paths
        if (match := DISEASE_FILE_PATTERN.search(str(path).replace("\\", "/")))
        and match.group("year") == year and match.group("kind") == "full"
    ]
    picked = latest_per_year(full)
    return picked.get(year)


def versioned_full_name(year: str, version: str) -> str:
    """File name for a new full download, version = YYYYMMDD."""
    if not re.fullmatch(r"\d{8}", version):
        raise ValueError(f"version must be YYYYMMDD, got {version!r}")
    return f"disease_cases_{year}_full_{version}.csv"
