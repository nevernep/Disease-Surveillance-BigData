import argparse
import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


# ============================================================
# Configuration
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "disease"
RAW_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv(PROJECT_ROOT / ".env")

TOKEN = os.getenv("DATA_GO_TH_TOKEN")

if not TOKEN:
    raise ValueError(
        "ไม่พบ DATA_GO_TH_TOKEN กรุณาตรวจสอบไฟล์ .env"
    )


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

HEADERS = {
    "api-key": TOKEN
}


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
                headers=HEADERS,
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

def download_disease_full(year, resource_id, overwrite=False):
    """ดาวน์โหลดไฟล์ CSV ทั้งชุดของ resource (1 คำขอ/ปี แทนการเรียก API หลายร้อยครั้ง)

    ไฟล์ต้นฉบับมี 15 fields (ไม่มี _id ซึ่ง datastore API เพิ่มเอง)
    เขียนลง .part ก่อน แล้วเปลี่ยนชื่อเมื่อขนาดตรงกับ Content-Length เท่านั้น
    """

    print("\n" + "=" * 60)
    print(f"Downloading Full Disease Dataset ปี {year}")
    print("=" * 60)

    output_file = RAW_DIR / f"disease_cases_{year}_full.csv"
    if output_file.exists() and not overwrite:
        print(f"มีไฟล์อยู่แล้ว ไม่ดาวน์โหลดซ้ำ: {output_file}")
        print("ใช้ --overwrite หากต้องการแทนที่ (Raw ควรคงเดิม)")
        return True

    try:
        metadata = requests.get(
            RESOURCE_SHOW_URL,
            params={"id": resource_id},
            headers=HEADERS,
            timeout=60,
        )
        metadata.raise_for_status()
        resource = metadata.json().get("result", {})
        url = resource.get("url")
        if not url:
            print("ไม่พบ URL สำหรับดาวน์โหลดใน resource metadata")
            return False

        print(f"Source URL      : {url}")
        print(f"Last modified   : {resource.get('last_modified')}")

        partial_file = output_file.with_suffix(".csv.part")
        with requests.get(url, stream=True, timeout=120) as response:
            response.raise_for_status()
            expected = int(response.headers.get("Content-Length", 0))
            written = 0
            with open(partial_file, "wb") as file:
                for chunk in response.iter_content(DOWNLOAD_CHUNK_BYTES):
                    file.write(chunk)
                    written += len(chunk)

        if expected and written != expected:
            partial_file.unlink(missing_ok=True)
            print(f"ขนาดไม่ตรง: ได้ {written:,} bytes จาก {expected:,}")
            return False

        partial_file.replace(output_file)
        print(f"Saved           : {output_file} ({written:,} bytes)")
        print("Status          : SUCCESS")
        return True

    except requests.exceptions.RequestException as error:
        print(f"Request Error: {error}")
        return False


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
        "--overwrite",
        action="store_true",
        help="ใช้กับ --full: แทนที่ไฟล์ที่มีอยู่แล้ว",
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
            results[year] = download_disease_full(
                year, resource_id, arguments.overwrite
            )
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