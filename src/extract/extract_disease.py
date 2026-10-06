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
    arguments = parser.parse_args()

    if arguments.limit <= 0:
        parser.error("--limit ต้องมากกว่า 0")

    print("=" * 60)
    print("Disease Surveillance - Ethical Sample Ingestion")
    print("=" * 60)

    resources = RESOURCES
    if arguments.year:
        resources = {arguments.year: RESOURCES[arguments.year]}

    results = {}

    for year, resource_id in resources.items():

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