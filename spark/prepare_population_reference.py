"""Prepare Bangkok population reference CSVs for the Part 3 pipeline.

The script never invents population values.  It accepts an official CSV/XLSX
file or URL for each year, validates it, aggregates subdistrict rows when
needed, and writes the result only after all checks pass.
"""

from __future__ import annotations

import argparse
import io
import re
import zipfile
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

from spark.schemas import BANGKOK_DISTRICTS

EXPECTED_COLUMNS = ["ปี", "เขต", "ประชากรรวม"]
YEAR_COLUMN_ALIASES = {"ปี", "ปีข้อมูล", "ปีพ.ศ.", "พ.ศ.", "year", "year_be"}
DISTRICT_COLUMN_ALIASES = {
    "เขต", "ชื่อเขต", "อำเภอ/เขต", "district", "district_name",
    # DOPA stat_a files: one registration office per Bangkok district,
    # named "ท้องถิ่นเขต<district>".
    "ชื่อสำนักทะเบียน",
}
POPULATION_COLUMN_ALIASES = {
    "ประชากรรวม", "ประชากรทั้งหมด", "จำนวนประชากร", "ประชากร",
    "total_population", "population", "total", "จำนวนประชากรทั้งหมด",
}
# Nationwide files (DOPA) are narrowed to Bangkok when this column exists.
PROVINCE_COLUMN_ALIASES = {"ชื่อจังหวัด", "จังหวัด", "province"}
BANGKOK_PROVINCE = "กรุงเทพ"
# DOPA yearly zip: stat_c (province), stat_a (district), stat_t, stat_m.
DOPA_DISTRICT_MEMBER = re.compile(r"(^|/)stat_a\d+\.xls$")
CSV_ENCODINGS = ("utf-8-sig", "cp874")
# A Thai above/below vowel or tone mark typed twice in a row.
THAI_DOUBLED_MARK = r"([ัิ-ฺ็-๎])\1+"
SUBDISTRICT_COLUMN_ALIASES = {"แขวง", "ชื่อตำบล", "subdistrict"}


def _clean_name(value: object) -> str:
    return re.sub(r"\s+", " ", str(value).replace("\ufeff", "")).strip()


def _find_column(columns: Iterable[object], aliases: set[str]) -> str | None:
    """Case-insensitive match, e.g. "District" / "TOTAL" in official CSVs."""
    normalized = {_clean_name(column).lower(): str(column) for column in columns}
    for alias in aliases:
        if alias.lower() in normalized:
            return normalized[alias.lower()]
    return None


def _read_csv_bytes(content: bytes) -> pd.DataFrame:
    """Official CSVs come as UTF-8 (BOM) or Thai Windows-874."""
    for encoding in CSV_ENCODINGS:
        try:
            return pd.read_csv(io.BytesIO(content), encoding=encoding, dtype=str)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Unsupported CSV encoding; tried {CSV_ENCODINGS}")


def _normalize_district(value: object) -> str:
    district = _clean_name(value)
    district = re.sub(r"^ท้องถิ่น", "", district)
    district = re.sub(r"^(เขต|ข\.?)\s*", "", district)
    district = re.sub(r"\s*(กรุงเทพมหานคร|กรุงเทพฯ|กทม\.?)$", "", district)
    # Common Thai typing errors seen in official files: "เเ" typed for "แ",
    # a doubled vowel/tone mark ("วัังทองหลาง"), a space inside the name.
    district = district.replace("เเ", "แ")
    district = re.sub(THAI_DOUBLED_MARK, r"\1", district)
    district = re.sub(r"\s+", "", district)
    aliases = {
        "ป้อมปราบฯ": "ป้อมปราบศัตรูพ่าย",
        "ป้อมปราบ": "ป้อมปราบศัตรูพ่าย",
        "สัมพันธวงษ์": "สัมพันธวงศ์",
    }
    return aliases.get(district, district)


class _HtmlTable(HTMLParser):
    """Rows of the first HTML table (DOPA ".xls" files are HTML tables)."""

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: str | None = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = ""

    def handle_endtag(self, tag):
        if tag in {"td", "th"} and self._cell is not None and self._row is not None:
            self._row.append(self._cell.strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell += data


def _read_html_table(content: bytes) -> pd.DataFrame:
    parser = _HtmlTable()
    parser.feed(content.decode("utf-8-sig", errors="strict"))
    if not parser.rows:
        raise ValueError("HTML source contains no table rows")
    header, *rows = parser.rows
    return pd.DataFrame([row for row in rows if len(row) == len(header)], columns=header)


def _read_bytes(content: bytes, name: str) -> pd.DataFrame:
    """Dispatch on content: DOPA zip, HTML table, Excel workbook or CSV."""
    if content[:4] == b"PK\x03\x04" and name.lower().endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            members = [m for m in archive.namelist() if DOPA_DISTRICT_MEMBER.search(m)]
            if len(members) != 1:
                raise ValueError(
                    f"{name}: expected one district file (stat_a*.xls), got {members}"
                )
            return _read_bytes(archive.read(members[0]), members[0])
    if content.lstrip(b"\xef\xbb\xbf \r\n\t")[:1] == b"<":
        return _read_html_table(content)
    if content[:4] == b"PK\x03\x04" or content[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return pd.read_excel(io.BytesIO(content))
    return _read_csv_bytes(content)


def _read_source(source: str) -> pd.DataFrame:
    path = Path(source)
    if path.is_file():
        return _read_bytes(path.read_bytes(), path.name)

    response = requests.get(source, timeout=60)
    response.raise_for_status()
    return _read_bytes(response.content, source.split("?")[0])


def prepare_year(source: str, year: int) -> pd.DataFrame:
    frame = _read_source(source)
    frame.columns = [_clean_name(column) for column in frame.columns]

    year_column = _find_column(frame.columns, YEAR_COLUMN_ALIASES)
    district_column = _find_column(frame.columns, DISTRICT_COLUMN_ALIASES)
    population_column = _find_column(frame.columns, POPULATION_COLUMN_ALIASES)
    subdistrict_column = _find_column(
        frame.columns, SUBDISTRICT_COLUMN_ALIASES
    )
    province_column = _find_column(frame.columns, PROVINCE_COLUMN_ALIASES)
    if province_column is not None:
        frame = frame[
            frame[province_column].astype(str).str.contains(BANGKOK_PROVINCE, regex=False)
        ]

    if district_column is None or population_column is None:
        missing = []
        if district_column is None:
            missing.append("เขต")
        if population_column is None:
            missing.append("ประชากรรวม")
        raise ValueError(
            f"{source}: cannot identify required columns: {', '.join(missing)}"
        )

    selected = frame[[column for column in (
        year_column, district_column, subdistrict_column, population_column
    ) if column is not None]].copy()
    selected = selected.rename(columns={
        district_column: "เขต",
        population_column: "ประชากรรวม",
    })

    if year_column is not None:
        selected["ปี"] = pd.to_numeric(selected[year_column], errors="coerce")
        if selected["ปี"].notna().any() and (selected["ปี"] != year).any():
            raise ValueError(f"{source}: contains a year other than {year}")
    else:
        selected["ปี"] = year

    selected["เขต"] = selected["เขต"].map(_normalize_district)
    selected["ประชากรรวม"] = pd.to_numeric(
        selected["ประชากรรวม"].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    )
    selected = selected[["ปี", "เขต", "ประชากรรวม"]]
    selected = selected.dropna(subset=["ปี", "เขต", "ประชากรรวม"])
    # "-" is the province-total row in DOPA files.
    selected = selected[~selected["เขต"].isin({"", "-"})]
    # Total rows: "รวม", "ยอดรวม", and the source typo "ยอรวม". No Bangkok
    # district name contains "รวม".
    selected = selected[
        ~selected["เขต"].str.contains("รวม", regex=False)
        & ~selected["เขต"].str.lower().isin({"ทั้งหมด", "total"})
    ]
    selected = selected[selected["ประชากรรวม"] > 0]

    result = (
        selected.groupby(["ปี", "เขต"], as_index=False)["ประชากรรวม"]
        .sum()
    )
    result["ปี"] = result["ปี"].astype(int)
    result["ประชากรรวม"] = result["ประชากรรวม"].astype("int64")
    validate_population(result, year, source)
    return result[EXPECTED_COLUMNS].sort_values("เขต").reset_index(drop=True)


def validate_population(
    frame: pd.DataFrame, year: int, source: str = "population"
) -> None:
    if list(frame.columns) != EXPECTED_COLUMNS:
        raise ValueError(
            f"{source}: columns must be exactly {EXPECTED_COLUMNS}"
        )
    if frame.isna().any().any():
        raise ValueError(f"{source}: null values are not allowed")
    if len(frame) != 50:
        raise ValueError(f"{source}: expected 50 districts, got {len(frame)}")
    if frame["ปี"].astype(int).ne(year).any():
        raise ValueError(f"{source}: every row must have year {year}")
    if frame["เขต"].astype(str).str.strip().eq("").any():
        raise ValueError(f"{source}: district names cannot be empty")
    if frame["เขต"].duplicated().any():
        raise ValueError(f"{source}: duplicate district names found")
    if set(frame["เขต"]) != set(BANGKOK_DISTRICTS):
        missing = sorted(set(BANGKOK_DISTRICTS) - set(frame["เขต"]))
        unexpected = sorted(set(frame["เขต"]) - set(BANGKOK_DISTRICTS))
        raise ValueError(
            f"{source}: district coverage mismatch; missing={missing}, "
            f"unexpected={unexpected}"
        )
    if not pd.api.types.is_integer_dtype(frame["ประชากรรวม"]):
        raise ValueError(f"{source}: population must be integer")
    if frame["ประชากรรวม"].le(0).any():
        raise ValueError(f"{source}: population must be positive")


def write_reference(frame: pd.DataFrame, output: Path, year: int) -> None:
    if output.exists():
        raise FileExistsError(
            f"{output} already exists; refusing to overwrite an existing file"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    frame.to_csv(temporary, index=False, encoding="utf-8-sig", lineterminator="\n")
    temporary.replace(output)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Aggregate and validate Bangkok population reference data"
    )
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--source-2568", help="Official CSV/XLSX path or URL")
    parser.add_argument("--source-2569", help="Official CSV/XLSX path or URL")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace output files explicitly; default is refuse-to-overwrite",
    )
    args = parser.parse_args()
    sources = {
        year: source
        for year, source in ((2568, args.source_2568), (2569, args.source_2569))
        if source
    }
    if not sources:
        parser.error("give at least one of --source-2568 / --source-2569")

    reference = (
        args.project_root / "data" / "raw" / "disease" / "reference"
    ).resolve()
    outputs = {
        2568: reference / "population_summary_2568.csv",
        2569: reference / "population_summary_2569.csv",
    }
    prepared = {year: prepare_year(source, year) for year, source in sources.items()}
    if not args.overwrite and any(outputs[year].exists() for year in prepared):
        existing = [str(outputs[year]) for year in prepared if outputs[year].exists()]
        raise FileExistsError(
            "Refusing to overwrite existing output(s): " + ", ".join(existing)
        )
    for year, frame in prepared.items():
        if args.overwrite and outputs[year].exists():
            outputs[year].unlink()
        write_reference(frame, outputs[year], year)
        print(f"Wrote {len(frame)} districts for {year}: {outputs[year]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
