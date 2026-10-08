# Data Contract
## Big Data Analytics for Communicable Disease Surveillance in Bangkok

Version: 1.0

---

# 1. Purpose

เอกสาร Data Contract นี้กำหนดรูปแบบข้อมูลและข้อตกลงในการส่งต่อข้อมูลระหว่าง

- PART 1: Data Sources & Data Ingestion
- PART 2: Data Lake & Apache Airflow
- PART 3: Apache Spark Processing & Data Quality
- PART 4: Data Warehouse & Dashboard

โดยมีวัตถุประสงค์เพื่อให้แต่ละส่วนของ Data Pipeline
สามารถทำงานร่วมกันโดยใช้โครงสร้างข้อมูลที่ชัดเจนและสอดคล้องกัน

---

# 2. Overall Data Flow

Data Sources
    |
    v
PART 1 - Data Ingestion
    |
    | Raw JSON / Raw Excel
    v
PART 2 - Data Lake & Airflow
    |
    | Raw Zone
    v
PART 3 - Apache Spark
    |
    | Clean / Standardized Data
    v
PART 4 - Data Warehouse
    |
    v
Dashboard / Analytics

---

# 3. General Rules

## Raw Data

ข้อมูลที่ได้รับจากแหล่งข้อมูลต้นทางต้องถูกจัดเก็บใน Raw Layer
โดยรักษาค่าจากต้นทางไว้

PART 1 และ PART 2 ไม่ควร:

- เปลี่ยนชื่อคอลัมน์
- Standardize ชื่อโรค
- แก้ไข Missing Value
- ลบ Duplicate
- เปลี่ยนประเภทข้อมูลเพื่อการวิเคราะห์
- สร้าง Age Group
- Aggregate จำนวนผู้ป่วย

การดำเนินการดังกล่าวเป็นหน้าที่ของ PART 3

---

# 4. Dataset: Disease Surveillance

## Dataset ID

disease_cases

## Source

Data.go.th Data API

## Source Type

REST API

## Raw Format

JSON

## Encoding

UTF-8

## Available Years

- 2568 / 2025
- 2569 / 2026

## Development Mode

ในขั้นตอน Development ใช้ข้อมูลตัวอย่าง:

- 100 records สำหรับปี 2568
- 100 records สำหรับปี 2569

ข้อมูลตัวอย่างใช้สำหรับทดสอบ Pipeline เท่านั้น
และไม่ใช้เป็นตัวแทนสำหรับการสรุปสถิติของข้อมูลทั้งหมด

---

# 5. Disease Raw File Naming Convention

มีไฟล์ได้ 2 แบบต่อปี ใน `data/raw/disease/`:

| ไฟล์ | ที่มา | รูปแบบ | Fields |
|---|---|---|---|
| `disease_cases_{year}_full_{YYYYMMDD}.csv` | ไฟล์ต้นฉบับทั้งชุด ดาวน์โหลดโดย DAG ทุกสัปดาห์ หรือ `extract_disease.py --full` | CSV UTF-8 BOM, CRLF | 15 (ไม่มี `_id`) |
| `disease_cases_{year}_full.csv` | ไฟล์เต็มรุ่นแรก (ก่อนมีการตั้งเวอร์ชัน) นับเป็นเวอร์ชันเก่าที่สุด | CSV | 15 |
| `disease_cases_{year}_sample.json` | Data API 100 records แรก | JSON array | 16 (มี `_id`) |

**ระบบใช้ไฟล์เดียวต่อปี:** ไฟล์เต็มเวอร์ชันใหม่ที่สุด ถ้าไม่มีไฟล์เต็มจึงใช้ sample
กฎนี้อยู่ใน `spark/raw_versions.py` ใช้ร่วมกันทั้ง extract, DAG (land) และ Spark (read) เพื่อไม่ให้นับผู้ป่วยซ้ำ

**ข้อมูลดิบห้ามเขียนทับ:** ผู้เผยแพร่อัปเดตไฟล์ต้นทางในที่เดิม การดาวน์โหลดแต่ละรอบจึงได้ชื่อไฟล์ใหม่ตามวันที่ (เวลาไทย)
ถ้าเนื้อหาเหมือนเวอร์ชันล่าสุด (SHA-256 เท่ากัน) จะไม่เก็บซ้ำ เวอร์ชันเก่าทั้งหมดยังอยู่ทั้งในเครื่องและใน Data Lake

ข้อตกลงของไฟล์ CSV:

- header ต้องเป็น 15 fields ตาม section 6 (ไม่รวม `_id`) และทุกแถวต้องมี 15 คอลัมน์
- `วันที่เริ่มป่วย` เป็น `D/M/YYYY` ค.ศ. เช่น `1/6/2025` = 1 มิถุนายน 2025
- ไม่มีรหัสผู้ป่วย Spark สร้าง `_record_id` = `{ชื่อไฟล์}:{ลำดับแถว}` เป็น case id

> วันที่ใน JSON ของ Data API ถูก datastore แปลงผิด (อ่าน `1/6/2025` เป็น 6 ม.ค.)
> จึงใช้ JSON เพื่อทดสอบ pipeline เท่านั้น

---

# 6. Disease Schema

| Field | Source Type | Required | Description |
|---|---|---|---|
| _id | integer | Yes | รหัส record จากแหล่งข้อมูล |
| ชื่อกลุ่มโรค | string | Yes | ชื่อกลุ่มโรค |
| อายุ (เต็ม) ปี | numeric | No | อายุเต็มหน่วยปี |
| อายุ (เต็ม) เดือน | numeric | No | อายุส่วนเดือน |
| อาย (เต็ม) วัน | numeric | No | อายุส่วนวัน |
| เพศ | string | No | เพศ |
| สถานภาพสมรส | string | No | สถานภาพสมรส |
| สัญชาติ | string | No | สัญชาติ |
| อาชีพ | string | No | อาชีพ |
| จังหวัด | string | Yes | จังหวัด |
| อำเภอ/เขต | string | Yes | อำเภอหรือเขต |
| ตำบล/แขวง | string | No | ตำบลหรือแขวง |
| วันที่เริ่มป่วย | timestamp/string | Yes | วันที่เริ่มป่วย |
| สภาพผู้ป่วย | string | No | สถานะ/สภาพผู้ป่วย |
| ประเภทผู้ป่วย | string | No | เช่น ผู้ป่วยนอก |
| สถานที่รักษา | string | No | สถานพยาบาลที่รักษา |

หมายเหตุ:
ชื่อ Field ต้องคงตาม Source ใน Raw Layer

---

# 7. Disease Data Quality Expectations

PART 1 ตรวจสอบเบื้องต้น:

- Schema ของปี 2568 และ 2569 มี 16 fields
- Sample ไม่มี Full-row Duplicate
- Sample ที่ตรวจไม่พบ Missing Value
- วันที่เริ่มป่วยสามารถแปลงเป็น Date/Timestamp ได้
- ข้อมูลมีระดับพื้นที่เขตและแขวง

อย่างไรก็ตาม การตรวจสอบจาก Sample
ไม่ถือเป็นผล Data Quality ของ Full Dataset

---

# 8. Known Disease Data Quality Issues

พบความแตกต่างของชื่อโรคระหว่างปี เช่น:

2568:

อุจจาระร่วง

2569:

โรคอุจจาระร่วงเฉียบพลัน

PART 3 ทำ Standardization ใน `spark/schemas.py` (`DISEASE_ALIASES`) ค่า raw เก็บไว้ใน
`disease_name_raw` ห้ามแก้ไขใน Raw Layer

| ชื่อมาตรฐาน | ค่าในต้นทาง |
|---|---|
| ไข้หวัดใหญ่ | ไข้หวัดใหญ่ |
| อุจจาระร่วงเฉียบพลัน | อุจจาระร่วง (2568), โรคอุจจาระร่วงเฉียบพลัน (2569) |
| โควิด-19 | โควิด-19 (COVID-19) (2568), ติดเชื้อไวรัสโคโรนา 2019 (covid-19) (2569) |
| ปอดบวม | โรคปอดบวม (2568), โรคปอดอักเสบหรือโรคปอดบวม (2569) |
| ไข้เลือดออก (รวม) | ไข้เลือดออก (2568); ไข้เด็งกี่ (Dengue fever), ไข้เลือดออก (DHF), ไข้เลือดออกช็อค (DSS) (2569) |

ข้อมูลที่ถูกแยกไป quarantine (`processed/quarantine/`, คอลัมน์ `_quarantine_reason`):

| เหตุผล | ความหมาย |
|---|---|
| `invalid_year` | ไม่มีปี หรือปีอยู่นอกช่วงที่กำหนด |
| `missing_disease_name` / `missing_district_name` | ไม่มีชื่อโรค หรือไม่มีชื่อเขต |
| `district_not_in_bangkok` | เขตไม่อยู่ใน 50 เขตของกรุงเทพฯ เช่น `เมืองระยอง`, `1020` |
| `invalid_case_count` | จำนวนผู้ป่วยน้อยกว่าหรือเท่ากับ 0 |
| `invalid_age` | อายุนอกช่วง 0–120 ปี |
| `invalid_or_missing_report_date` | วันที่เริ่มป่วยอ่านไม่ได้ |

แถวที่ค่าเหมือนกันทุกช่องจะ**ถูกเก็บไว้** (ไม่มีรหัสผู้ป่วยให้แยก) และรายงานจำนวนใน DQ

---

# 9. Dataset: Population Reference

## Dataset ID

population_buengkum

## Source

ข้อมูลประชากรและครัวเรือน
สำนักงานเขตบึงกุ่ม กรุงเทพมหานคร

## Source Type

Excel File

## Raw Format

XLSX

## Available Years

- 2568
- 2569

## Geographic Coverage

เฉพาะเขตบึงกุ่ม กรุงเทพมหานคร

ประกอบด้วย:

- คลองกุ่ม
- นวมินทร์
- นวลจันทร์

---

# 10. Population Raw File Naming Convention

ประชากรและครัวเรือน_{year}.xlsx

ตัวอย่าง:

ประชากรและครัวเรือน_2568.xlsx
ประชากรและครัวเรือน_2569.xlsx

Development Source Path:

data/raw/reference/

---

# 11. Population Excel Structure

Worksheet:

จำนวนประชากรและครัวเรือน

โครงสร้างไฟล์ต้นฉบับ:

Row 1
ชื่อรายงาน

Row 2
Column Header

Rows 3-5
ข้อมูลรายแขวง

Row 6
ยอดรวม

Row 7
Blank Row

Row 8
วันที่ปรับปรุงข้อมูล

ดังนั้น Raw Excel ไม่ควรถูกอ่านโดยสมมติว่า Row 1
เป็น Column Header

---

# 12. Population Schema

| Field | Expected Type | Required | Description |
|---|---|---|---|
| แขวง | string | Yes | ชื่อแขวง |
| จำนวนครัวเรือน | numeric | Yes | จำนวนครัวเรือน |
| ประชากรชาย | numeric | Yes | จำนวนประชากรชาย |
| ประชากรหญิง | numeric | Yes | จำนวนประชากรหญิง |
| ประชากรรวม | numeric | Yes | จำนวนประชากรรวม |

---

# 13. Population Validation Values

## 2568

คลองกุ่ม:
Population = 66,476

นวมินทร์:
Population = 25,979

นวลจันทร์:
Population = 43,435

Total:
Population = 135,890


## 2569

คลองกุ่ม:
Population = 65,467

นวมินทร์:
Population = 25,677

นวลจันทร์:
Population = 42,938

Total:
Population = 134,082

ค่าดังกล่าวสามารถใช้ตรวจสอบว่า
Data Lake / Spark อ่านข้อมูลจาก Excel ถูกต้องหรือไม่

---

# 14. Known Population Data Quality Issues

Raw Excel มี:

- Title Row
- Summary Row
- Blank Row
- Last Updated Row

ดังนั้น Missing Values ที่เกิดจากแถวเหล่านี้
ไม่ควรถูกตีความทันทีว่าเป็นข้อมูลประชากรสูญหาย

PART 3 ต้องแยก Data Records
ออกจาก Metadata Rows ก่อนการวิเคราะห์

---

# 15. Population Limitation

Population Dataset ปัจจุบันครอบคลุมเฉพาะเขตบึงกุ่ม

ดังนั้น:

ห้ามใช้ Population Dataset นี้
เพื่อคำนวณ Incidence Rate ของทั้งกรุงเทพมหานคร

สามารถใช้สำหรับ:

- Reference Data
- Pipeline Testing
- Data Validation
- การทดลอง Join เฉพาะเขตบึงกุ่ม

หากต้องการวิเคราะห์ Incidence Rate ทั้งกรุงเทพมหานคร
ต้องเพิ่ม Population Dataset ที่ครอบคลุมพื้นที่เดียวกับ Disease Dataset

---

# 16. PART 1 Responsibilities

PART 1 รับผิดชอบ:

1. เชื่อมต่อ Data Source
2. Authentication
3. ดึงข้อมูลตามขอบเขตที่กำหนด
4. จัดเก็บ Raw Data
5. ตรวจสอบ Schema เบื้องต้น
6. ตรวจสอบจำนวน Records
7. บันทึก Source Metadata
8. ปฏิบัติตามข้อกำหนดและ Rate Limit ของ API

PART 1 ไม่รับผิดชอบ Data Transformation

---

# 17. PART 2 Responsibilities

PART 2 รับข้อมูลจาก PART 1 และรับผิดชอบ:

1. นำ Raw Data เข้า Data Lake
2. แยก Storage Zone ตาม Dataset
3. ใช้ Apache Airflow จัดการ Workflow
4. ตรวจสอบว่า File Ingestion สำเร็จ
5. Logging
6. Retry เมื่อ Task ล้มเหลวตามนโยบายที่กำหนด
7. ส่ง Raw Data ให้ PART 3

PART 2 ต้องรักษา Raw Data ต้นฉบับไว้

---

# 18. Data Lake Structure

Bucket `disease-surveillance` (SeaweedFS, S3-compatible):

```text
raw/                                   PART 2 lands original bytes (immutable)
+-- disease/year={ปี}/
|   +-- disease_cases_{ปี}_sample.json
|   +-- _metadata/disease_cases_{ปี}_sample.manifest.json   (sha256, record count)
+-- population/year={ปี}/              optional (cases-only mode when absent)
    +-- population_summary_{ปี}.csv    columns ปี,เขต,ประชากรรวม; exactly 50 districts
    +-- _metadata/population_summary_{ปี}.manifest.json

processed/                             PART 3 Spark output (Parquet, rebuilt each run)
+-- clean/disease_cleaned/
+-- quarantine/disease_rejected/       rejected rows with _quarantine_reason
+-- standardized/disease_standardized/
+-- gold/disease_with_population/curated_disease_data/
+-- warehouse/{dim_date, dim_district, dim_disease, dim_age_group, dim_sex,
|              fact_disease_cases, fact_population}/
+-- quality/data_quality_report/
```

The Excel files for เขตบึงกุ่ม (section 9–15) are a local reference only and
are not landed in the lake.

---

# 19. Data Warehouse Contract

Spark builds the star schema from the case-level standardized data and writes
each table as UTF-8 CSV (`data/processed/warehouse/`) and Parquet (Data Lake
`processed/warehouse/`). The `load_warehouse` task then loads it into the
PostgreSQL schema `mart` (`sql/ddl.sql`) in a single transaction.

| Table | Grain / key | Columns |
|---|---|---|
| `dim_date` | month / `date_key` = yyyymm | `date_key`, `month_start`, `year_be`, `year_ce`, `quarter`, `month`, `month_label` |
| `dim_district` | district / `district_key` | `district_key`, `district_name`, `is_bangkok_district` |
| `dim_disease` | standardized disease / `disease_key` | `disease_key`, `disease_name` |
| `dim_age_group` | age band / `age_group_key` | `age_group_key`, `age_group`, `min_age`, `max_age` |
| `dim_sex` | `sex` (M/F/U) | `sex`, `sex_label` |
| `fact_disease_cases` | month + district + disease + age group + sex | keys above, `total_cases`, `source_records` |
| `fact_population` | year + district | `year_be`, `district_key`, `population` |

Rules:

- `dim_date` covers every month from the first to the last onset date, including
  months with zero cases.
- `dim_district` always contains all 50 Bangkok districts.
- Population is stored only in `fact_population`, so incidence denominators cover
  every district in scope, not only districts that reported a case.
- Incidence is not computed by age group or sex (no population by age/sex).
- Data Quality reconciles `SUM(fact_disease_cases.total_cases)` with the
  standardized case total (must be equal).

The annual Gold dataset (`curated_disease_data`, year × district × disease with
`population` and `incidence_rate_per_100k`) is kept for validation and export.

Power BI relationships and measures: `docs/powerbi_dashboard.md`.
Semantic views: `sql/analytics_views.sql`.
