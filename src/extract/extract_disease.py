import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv


# ============================================================
# Configuration
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Version naming is shared with the raw-landing DAG and the Spark reader.
sys.path.insert(0, str(PROJECT_ROOT))
from spark.raw_versions import latest_full, versioned_full_name  # noqa: E402

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "disease"
BANGKOK_TZ = timezone(timedelta(hours=7))

load_dotenv(PROJECT_ROOT / ".env")


BASE_URL = "https://opend.data.go.th/get-ckan/datastore_search"
RESOURCE_SHOW_URL = "https://opend.data.go.th/get-ckan/resource_show"
DOWNLOAD_CHUNK_BYTES = 1024 * 1024

RESOURCES = {
    "2568": "ae882ad6-e057-4419-b3f7-1ffdbcfb6593",
    "2569": "b59f2279-ced4-4c41-8452-e1c2cf60b2f1",
}

# จำกัดจำนวนข้อมูลเพื่อการพัฒนาและทดสอบ
SAMPLE_LIMIT = 100
API_PAGE_SIZE = 1000

def api_headers():
    """Token is read when a request is made, so importing this module is safe."""
    token = os.getenv("DATA_GO_TH_TOKEN")
    if not token:
        raise ValueError("ไม่พบ DATA_GO_TH_TOKEN กรุณาตรวจสอบไฟล์ .env")
    return {"api-key": token}


# ============================================================
# Extract Sample Data
# ============================================================

def extract_disease_sample(year, resource_id, limit=SAMPLE_LIMIT):

    print("\n" + "=" * 60)
    print(f"Extracting Disease Sample ปี {year}")
    print("=" * 60)

    try:
        records = []
        total = 0
        offset = 0

        while len(records) < limit:
            page_limit = min(API_PAGE_SIZE, limit - len(records))
            params = {
                "resource_id": resource_id,
                "limit": page_limit,
                "offset": offset,
            }
            response = requests.get(
                BASE_URL,
                params=params,
                headers=api_headers(),
                timeout=60,
            )

            print(f"HTTP Status: {response.status_code} (offset={offset})")
            response.raise_for_status()

            data = response.json()
            if not data.get("success"):
                print("API returned success=False")
                return False

            result = data.get("result", {})
            total = result.get("total", 0)
            page_records = result.get("records", [])
            records.extend(page_records)

            if not page_records or len(records) >= total:
                break

            offset += len(page_records)

        print(f"Total available : {total:,}")
        print(f"Requested       : {limit:,}")
        print(f"Received        : {len(records):,}")

        # ----------------------------------------------------
        # Save Raw JSON
        # ----------------------------------------------------

        RAW_DIR.mkdir(parents=True, exist_ok=True)
        output_file = (
            RAW_DIR /
            f"disease_cases_{year}_sample.json"
        )

        with open(
            output_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                records,
                file,
                ensure_ascii=False,
                indent=2
            )

        print(f"Saved           : {output_file}")
        print("Status          : SUCCESS")

        return True

    except requests.exceptions.RequestException as error:

        print(f"Request Error: {error}")
        return False

    except ValueError as error:

        print(f"JSON Error: {error}")
        return False


# ============================================================
# Download Full Dataset (one request per year)
# ============================================================

def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(DOWNLOAD_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_disease_full(year, resource_id, raw_dir=None, version=None):
    """ดาวน์โหลดไฟล์ CSV ทั้งชุดของ resource (1 คำขอ/ปี) เป็นไฟล์เวอร์ชันใหม่

    Raw ห้ามเขียนทับ: บันทึกเป็น disease_cases_{ปี}_full_{YYYYMMDD}.csv
    ถ้าเนื้อหาเหมือนเวอร์ชันล่าสุดที่มีอยู่ (SHA-256 เท่ากัน) จะไม่เก็บซ้ำ
    เขียนลง .part ก่อน และใช้ไฟล์เมื่อขนาดตรงกับ Content-Length เท่านั้น

    คืนค่า (status, path): status = "new" | "unchanged"
    """

    raw_dir = Path(raw_dir or RAW_DIR)
    raw_dir.mkdir(parents=True, exist_ok=True)
    version = version or datetime.now(BANGKOK_TZ).strftime("%Y%m%d")
    output_file = raw_dir / versioned_full_name(year, version)

    print("\n" + "=" * 60)
    print(f"Downloading Full Disease Dataset ปี {year} (version {version})")
    print("=" * 60)

    metadata = requests.get(
        RESOURCE_SHOW_URL,
        params={"id": resource_id},
        headers=api_headers(),
        timeout=60,
    )
    metadata.raise_for_status()
    resource = metadata.json().get("result", {})
    url = resource.get("url")
    if not url:
        raise RuntimeError(f"resource {resource_id}: no download URL in metadata")

    print(f"Source URL      : {url}")
    print(f"Last modified   : {resource.get('last_modified')}")

    partial_file = raw_dir / (output_file.name + ".part")
    with requests.get(url, stream=True, timeout=300) as response:
        response.raise_for_status()
        expected = int(response.headers.get("Content-Length", 0))
        written = 0
        with open(partial_file, "wb") as file:
            for chunk in response.iter_content(DOWNLOAD_CHUNK_BYTES):
                file.write(chunk)
                written += len(chunk)

    if expected and written != expected:
        partial_file.unlink(missing_ok=True)
        raise RuntimeError(f"size mismatch: got {written:,} of {expected:,} bytes")

    new_digest = _sha256(partial_file)
    previous = latest_full([str(p) for p in raw_dir.glob(f"disease_cases_{year}_full*.csv")], year)
    if previous and _sha256(previous) == new_digest:
        partial_file.unlink()
        print(f"Unchanged       : same content as {Path(previous).name}")
        return "unchanged", Path(previous)

    if output_file.exists():
        partial_file.unlink()
        raise FileExistsError(
            f"{output_file.name} already exists with different content; "
            "raw files are immutable — pass another --version"
        )

    partial_file.replace(output_file)
    print(f"Saved           : {output_file} ({written:,} bytes, sha256 {new_digest[:12]}…)")
    return "new", output_file


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="ดึงข้อมูลผู้ป่วยโรคจาก Data.go.th"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=SAMPLE_LIMIT,
        help=f"จำนวน records ต่อปี (ค่าเริ่มต้น: {SAMPLE_LIMIT})",
    )
    parser.add_argument(
        "--year",
        choices=sorted(RESOURCES),
        help="เลือกปีที่ต้องการดึง เช่น 2569; หากไม่ระบุจะดึงทุกปี",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="ดาวน์โหลดไฟล์ CSV ทั้งชุด (1 คำขอ/ปี) เป็น disease_cases_{ปี}_full.csv",
    )
    parser.add_argument(
        "--version",
        help="ใช้กับ --full: กำหนดเวอร์ชัน YYYYMMDD เอง (ค่าเริ่มต้น = วันนี้ตามเวลาไทย)",
    )
    arguments = parser.parse_args()

    if arguments.limit <= 0:
        parser.error("--limit ต้องมากกว่า 0")

    print("=" * 60)
    print(
        "Disease Surveillance - Full Dataset Download"
        if arguments.full
        else "Disease Surveillance - Ethical Sample Ingestion"
    )
    print("=" * 60)

    resources = RESOURCES
    if arguments.year:
        resources = {arguments.year: RESOURCES[arguments.year]}

    results = {}

    for year, resource_id in resources.items():

        if arguments.full:
            try:
                status, path = download_disease_full(
                    year, resource_id, version=arguments.version
                )
                results[year] = True
            except (requests.exceptions.RequestException, RuntimeError,
                    FileExistsError, ValueError) as error:
                print(f"Error: {error}")
                results[year] = False
        else:
            results[year] = extract_disease_sample(
                year,
                resource_id,
                arguments.limit,
            )

    print("\n" + "=" * 60)
    print("Extraction Summary")
    print("=" * 60)

    for year, success in results.items():

        status = "SUCCESS" if success else "FAILED"

        print(f"{year}: {status}")