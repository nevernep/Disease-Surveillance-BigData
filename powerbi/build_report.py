"""Generate the 5 report pages of DiseaseSurveillance.Report (PBIR format).

Rewrites DiseaseSurveillance.Report/definition/pages/ from the layout below,
using the measures and columns of DiseaseSurveillance.SemanticModel.

Close Power BI Desktop before running: Desktop does not reload files that
change on disk and overwrites them when the report is saved.

    python powerbi/build_report.py
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Dict, List, Optional

REPORT = Path(__file__).resolve().parent / "DiseaseSurveillance.Report" / "definition"
SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition"
PAGE_WIDTH, PAGE_HEIGHT = 1280, 720

NOTE_INCIDENCE = (
    "อัตราป่วยต่อแสนประชากรเป็นอัตราสะสมตามช่วงที่มีข้อมูล "
    "(2568: มิ.ย.–ธ.ค. 7 เดือน, 2569: ม.ค.–ก.ย. 9 เดือน) อย่าเทียบยอดรวมข้ามปีโดยตรง "
    "ตัวหารคือประชากรตามทะเบียนราษฎร มิ.ย. 2569 (ปี 2568 ใช้ค่าเดียวกัน) "
    "เขตชั้นในที่มีประชากรแฝงมากอาจมีอัตราสูงเกินจริง"
)


# ---------------------------------------------------------------------------
# Field references
# ---------------------------------------------------------------------------

def column(entity: str, name: str) -> Dict:
    return {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": name}}


def measure(name: str, entity: str = "fact_disease_cases") -> Dict:
    return {"Measure": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": name}}


def projection(field: Dict, active: bool = False) -> Dict:
    kind = "Column" if "Column" in field else "Measure"
    entity = field[kind]["Expression"]["SourceRef"]["Entity"]
    name = field[kind]["Property"]
    item = {"field": field, "queryRef": f"{entity}.{name}", "nativeQueryRef": name}
    if active:
        item["active"] = True
    return item


MONTH = column("dim_date", "month_label")
YEAR = column("dim_date", "year_be")
DISEASE = column("dim_disease", "disease_name")
DISTRICT = column("dim_district", "district_name")
AGE = column("dim_age_group", "age_group")
SEX = column("dim_sex", "sex_label")
TOTAL = measure("Total Cases")


# ---------------------------------------------------------------------------
# Visual builders
# ---------------------------------------------------------------------------

def _name(*parts: object) -> str:
    """Stable 20-hex id so re-running the script yields identical files."""
    return hashlib.sha1("|".join(map(str, parts)).encode("utf-8")).hexdigest()[:20]


def visual(
    page: str,
    key: str,
    visual_type: str,
    box: List[int],
    roles: Optional[Dict[str, List[Dict]]] = None,
    sort: Optional[Dict] = None,
    objects: Optional[Dict] = None,
    title: Optional[str] = None,
) -> Dict:
    x, y, width, height = box
    body: Dict = {"visualType": visual_type}
    if roles:
        body["query"] = {
            "queryState": {role: {"projections": items} for role, items in roles.items()}
        }
        if sort:
            body["query"]["sortDefinition"] = sort
    if objects:
        body["objects"] = objects
    if title:
        body["visualContainerObjects"] = {
            "title": [{
                "properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "text": {"expr": {"Literal": {"Value": f"'{title}'"}}},
                }
            }]
        }
    body["drillFilterOtherVisuals"] = True
    return {
        "$schema": f"{SCHEMA}/visualContainer/2.13.0/schema.json",
        "name": _name(page, key),
        "position": {"x": x, "y": y, "z": 0, "height": height, "width": width},
        "visual": body,
    }


def sort_by(field: Dict, direction: str) -> Dict:
    return {"sort": [{"field": field, "direction": direction}], "isDefaultSort": False}


def card(page: str, key: str, box: List[int], field: Dict, title: str) -> Dict:
    return visual(page, key, "cardVisual", box, {"Data": [projection(field)]}, title=title)


def slicer(page: str, key: str, box: List[int], field: Dict, title: str) -> Dict:
    return visual(page, key, "slicer", box, {"Values": [projection(field, active=True)]},
                  title=title)


def text_box(page: str, key: str, box: List[int], text: str, size: str, bold: bool) -> Dict:
    style = {"fontSize": size}
    if bold:
        style["fontWeight"] = "bold"
    paragraphs = [{"textRuns": [{"value": text, "textStyle": style}]}]
    return visual(page, key, "textbox", box,
                  objects={"general": [{"properties": {"paragraphs": paragraphs}}]})


def heading(page: str, text: str) -> Dict:
    return text_box(page, "heading", [20, 10, 940, 50], text, "20pt", True)


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

def page_overview(p: str) -> List[Dict]:
    return [
        heading(p, "เฝ้าระวังโรคติดต่อ กรุงเทพมหานคร — ภาพรวม"),
        card(p, "cases", [20, 70, 300, 110], TOTAL, "ผู้ป่วยทั้งหมด"),
        card(p, "rate", [340, 70, 300, 110], measure("Incidence Rate per 100k"),
             "อัตราป่วยต่อแสนประชากร"),
        card(p, "coverage", [660, 70, 300, 110], measure("Population Coverage Status"),
             "ข้อมูลประชากร"),
        visual(p, "trend", "lineChart", [20, 195, 940, 330],
               {"Category": [projection(MONTH, True)], "Y": [projection(TOTAL)],
                "Series": [projection(DISEASE)]},
               sort_by(MONTH, "Ascending"), title="ผู้ป่วยรายเดือนแยกตามโรค"),
        visual(p, "by_disease", "clusteredBarChart", [20, 540, 600, 170],
               {"Category": [projection(DISEASE, True)], "Y": [projection(TOTAL)]},
               sort_by(TOTAL, "Descending"), title="ผู้ป่วยแยกตามโรค"),
        text_box(p, "note", [640, 540, 620, 170], NOTE_INCIDENCE, "10pt", False),
        slicer(p, "year", [980, 70, 280, 130], YEAR, "ปี พ.ศ."),
        slicer(p, "disease", [980, 215, 280, 310], DISEASE, "โรค"),
    ]


def page_trend(p: str) -> List[Dict]:
    return [
        heading(p, "แนวโน้มรายเดือน"),
        visual(p, "trend", "lineChart", [20, 70, 940, 640],
               {"Category": [projection(MONTH, True)], "Y": [projection(TOTAL)]},
               sort_by(MONTH, "Ascending"), title="ผู้ป่วยรายเดือน"),
        card(p, "mom", [980, 70, 280, 110], measure("Month over Month Cases %"),
             "เทียบเดือนก่อน (เดือนล่าสุดที่เลือก)"),
        card(p, "yoy", [980, 195, 280, 110], measure("Year over Year Cases %"),
             "เทียบเดือนเดียวกันปีก่อน"),
        slicer(p, "disease", [980, 320, 280, 190], DISEASE, "โรค"),
        slicer(p, "district", [980, 525, 280, 185], DISTRICT, "เขต"),
    ]


def page_districts(p: str) -> List[Dict]:
    return [
        heading(p, "สถานการณ์รายเขต"),
        visual(p, "by_district", "clusteredBarChart", [20, 70, 560, 640],
               {"Category": [projection(DISTRICT, True)], "Y": [projection(TOTAL)]},
               sort_by(TOTAL, "Descending"), title="ผู้ป่วยรายเขต"),
        visual(p, "table", "tableEx", [600, 70, 660, 470],
               {"Values": [projection(DISTRICT), projection(TOTAL),
                           projection(measure("Population")),
                           projection(measure("Incidence Rate per 100k")),
                           projection(measure("District Rank"))]},
               sort_by(measure("Incidence Rate per 100k"), "Descending"),
               title="อัตราป่วยต่อแสนประชากรรายเขต"),
        slicer(p, "year", [600, 555, 200, 155], YEAR, "ปี พ.ศ."),
        slicer(p, "disease", [815, 555, 445, 155], DISEASE, "โรค"),
    ]


def page_demographics(p: str) -> List[Dict]:
    return [
        heading(p, "อายุและเพศ"),
        visual(p, "age_sex", "barChart", [20, 70, 760, 520],
               {"Category": [projection(AGE, True)], "Y": [projection(TOTAL)],
                "Series": [projection(SEX)]},
               sort_by(AGE, "Ascending"), title="ผู้ป่วยตามกลุ่มอายุและเพศ"),
        visual(p, "share", "pivotTable", [800, 70, 460, 400],
               {"Rows": [projection(AGE, True)], "Columns": [projection(SEX)],
                "Values": [projection(measure("Case Share %"))]},
               title="สัดส่วนผู้ป่วย (%)"),
        text_box(p, "note", [20, 605, 760, 105],
                 "อัตราป่วยต่อแสนประชากรไม่แสดงในหน้านี้ เพราะไม่มีข้อมูลประชากรแยกอายุ/เพศ "
                 "จึงแสดงเป็นจำนวนและสัดส่วนผู้ป่วยแทน", "10pt", False),
        slicer(p, "year", [800, 490, 220, 220], YEAR, "ปี พ.ศ."),
        slicer(p, "disease", [1040, 490, 220, 220], DISEASE, "โรค"),
    ]


def page_quality(p: str) -> List[Dict]:
    status = column("data_quality", "status")
    return [
        heading(p, "คุณภาพข้อมูล (Data Quality)"),
        card(p, "failed", [20, 70, 300, 110], measure("Failed DQ Rules", "data_quality"),
             "จำนวน rule ที่ไม่ผ่าน"),
        text_box(p, "legend", [340, 70, 920, 110],
                 "PASS = ผ่าน, FAIL = ไม่ผ่าน (pipeline หยุด), SKIP = rule ไม่ใช้ในโหมดที่รัน "
                 "ข้อมูลจาก Spark รอบล่าสุดที่โหลดเข้า warehouse", "11pt", False),
        visual(p, "rules", "tableEx", [20, 195, 1240, 515],
               {"Values": [projection(column("data_quality", name))
                           for name in ("layer", "rule", "observed", "threshold", "status")]},
               sort_by(status, "Ascending"), title="ผลตรวจทุก rule"),
    ]


PAGES = [
    ("ภาพรวม", page_overview),
    ("แนวโน้ม", page_trend),
    ("รายเขต", page_districts),
    ("อายุและเพศ", page_demographics),
    ("คุณภาพข้อมูล", page_quality),
]


def write_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    if not REPORT.is_dir():
        raise FileNotFoundError(f"Save the report as .pbip first: {REPORT} not found")

    pages_dir = REPORT / "pages"
    if pages_dir.exists():
        shutil.rmtree(pages_dir)

    order = []
    for display_name, build in PAGES:
        page_name = _name("page", display_name)
        order.append(page_name)
        write_json(pages_dir / page_name / "page.json", {
            "$schema": f"{SCHEMA}/page/2.1.0/schema.json",
            "name": page_name,
            "displayName": display_name,
            "displayOption": "FitToPage",
            "height": PAGE_HEIGHT,
            "width": PAGE_WIDTH,
        })
        for z, item in enumerate(build(page_name)):
            item["position"]["z"] = z * 1000
            item["position"]["tabOrder"] = z * 1000
            write_json(pages_dir / page_name / "visuals" / item["name"] / "visual.json", item)
        print(f"{display_name}: {len(build(page_name))} visuals")

    write_json(pages_dir / "pages.json", {
        "$schema": f"{SCHEMA}/pagesMetadata/1.1.0/schema.json",
        "pageOrder": order,
        "activePageName": order[0],
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
