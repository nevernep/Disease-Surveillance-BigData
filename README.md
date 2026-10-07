# Disease Surveillance Big Data

## การวิเคราะห์ข้อมูลขนาดใหญ่เพื่อเฝ้าระวังแนวโน้มโรคติดต่อในกรุงเทพมหานคร

**Big Data Analytics for Communicable Disease Surveillance in Bangkok**

โปรเจกต์นี้ออกแบบและพัฒนา Big Data Pipeline สำหรับรวบรวม จัดเก็บ ประมวลผล และวิเคราะห์ข้อมูลผู้ป่วยโรคติดต่อในกรุงเทพมหานคร จากข้อมูลเปิดภาครัฐ (Data.go.th) ประกอบกับข้อมูลประชากร

```text
Data.go.th API → Raw JSON → Data Lake (S3) → Airflow → Spark (Clean/DQ) → PostgreSQL Warehouse → Power BI
```

---

## วัตถุประสงค์

1. รวบรวมข้อมูลผู้ป่วยโรคติดต่อจากแหล่งข้อมูลภาครัฐ
2. พัฒนา Data Ingestion จาก REST API และไฟล์ Excel/CSV
3. จัดเก็บข้อมูลต้นฉบับแบบไม่แก้ไขใน Raw Zone ของ Data Lake
4. ใช้ Apache Airflow ควบคุมและติดตาม Pipeline
5. ใช้ Apache Spark ทำความสะอาด Standardize และตรวจคุณภาพข้อมูล
6. จัดเก็บข้อมูลที่ประมวลผลแล้วใน Data Warehouse (PostgreSQL, Star Schema)
7. วิเคราะห์แนวโน้มโรคตามเวลา พื้นที่ อายุ และเพศ
8. นำเสนอผลผ่าน Dashboard (Power BI)

---

## สถาปัตยกรรม

```text
┌──────────────────────┐
│ Data.go.th REST API  │  src/extract/extract_disease.py (token ใน .env)
└──────────┬───────────┘
           │ Raw JSON  data/raw/disease/
           ▼
┌──────────────────────────────────────────────────────────────┐
│ Apache Airflow (Docker, LocalExecutor)                        │
│                                                               │
│  DAG disease_raw_to_lake                                      │
│   ensure bucket → land_disease_{ปี} (ตรวจ 16 fields, SHA-256)  │
│                 → land_population_{ปี} (ข้ามได้: cases-only)   │
│                 → trigger spark_processing                    │
│                                                               │
│  DAG spark_processing                                         │
│   run_spark_pipeline → load_warehouse                         │
└──────────┬──────────────────────────────┬────────────────────┘
           ▼                              ▼
┌──────────────────────┐       ┌───────────────────────────────┐
│ Data Lake (SeaweedFS,│       │ Apache Spark 3.5 (PySpark)    │
│ S3-compatible)       │◄─────►│ อ่าน raw/ ผ่าน S3A             │
│  raw/       ต้นฉบับ   │       │ canonical → quarantine/clean  │
│  processed/ Parquet  │       │ → dedup → standardize         │
└──────────────────────┘       │ → gold + star schema + DQ     │
                               └──────────────┬────────────────┘
                                              ▼
                               ┌───────────────────────────────┐
                               │ PostgreSQL Warehouse (mart)   │
                               │ dim_* + fact_disease_cases    │
                               │ + fact_population + views     │
                               └──────────────┬────────────────┘
                                              ▼
                                         Power BI
```

| Service | หน้าที่ | Port (เครื่อง local) |
|---|---|---|
| `airflow` | Web UI, scheduler และรัน Spark | `8080` |
| `object-store` | SeaweedFS: S3 API และ filer | `8333`, `8888` |
| `postgres` | metadata ของ Airflow เท่านั้น | — |
| `warehouse-db` | Data Warehouse (schema `mart`) | `127.0.0.1:5433` |

---

## เริ่มใช้งาน (Docker)

ต้องมี Docker Desktop และควรให้ RAM แก่ Docker อย่างน้อย 4 GB (ถ้าจะใช้ข้อมูลเต็ม แนะนำ 6–8 GB)

```powershell
# 1) ตั้งค่า: คัดลอกค่าตั้งต้นแล้วใส่ DATA_GO_TH_TOKEN (ห้าม commit .env)
Copy-Item .env.example .env

# 2) ดึงข้อมูล: ไฟล์เต็ม (ปีละ 1 คำขอ ~80–90 MB) หรือ sample 100 records/ปี
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python src\extract\extract_disease.py --full   # ข้อมูลเต็ม (แนะนำ)
python src\extract\extract_disease.py          # หรือ sample จาก API

# 3) เปิดระบบ
docker compose up -d --build

# 4) รัน pipeline ทั้งเส้น (raw → lake → spark → warehouse)
docker compose exec airflow airflow dags unpause disease_raw_to_lake
docker compose exec airflow airflow dags unpause spark_processing
docker compose exec airflow airflow dags trigger disease_raw_to_lake
```

- Airflow UI: http://localhost:8080 (user `airflow` / `airflow-local-only`)
- SeaweedFS filer: http://localhost:8888 (bucket `disease-surveillance`)
- Warehouse: `localhost:5433` database `surveillance_dw` (user `dw_user` / `warehouse-local-only`)

รันเฉพาะ Spark + โหลด warehouse ใหม่ โดยไม่ land raw ซ้ำ:

```powershell
docker compose exec airflow airflow dags trigger spark_processing
```

> ค่า credentials ใน Compose ใช้สำหรับเครื่อง local เท่านั้น รายละเอียดการใช้งานและการแก้ปัญหาอยู่ที่ [docs/data_lake_airflow.md](docs/data_lake_airflow.md)

### ตัวแปรสำคัญใน `.env`

| ตัวแปร | ค่าตั้งต้น | ความหมาย |
|---|---|---|
| `DATA_GO_TH_TOKEN` | — | token ของ Data.go.th ใช้เฉพาะสคริปต์ extract |
| `REQUIRE_POPULATION` | `false` | `false` = **cases-only mode** (ไม่มีข้อมูลประชากรก็รันได้ incidence จะว่าง), `true` = บังคับต้องมีข้อมูลประชากร 50 เขต |
| `DATA_LAKE_URI` | `s3a://disease-surveillance` | Spark อ่าน/เขียนผ่าน Data Lake; ตั้งเป็นค่าว่างเพื่อใช้ไฟล์ในเครื่อง |
| `SPARK_MASTER` | `local[2]` | จำนวน core ของ Spark (จำกัดไว้เพื่อให้ Airflow UI ไม่ล่ม) |
| `WAREHOUSE_DB_*` | ดู `.env.example` | การเชื่อมต่อ PostgreSQL Warehouse |

---

## โครงสร้างโปรเจกต์

```text
Disease-Surveillance-BigData/
├── airflow/
│   ├── Dockerfile                 Airflow 2.10 + Java 17 + PySpark + S3A jars
│   └── dags/
│       ├── disease_raw_to_lake.py ตรวจ raw ตาม data contract แล้ว land ลง S3
│       └── spark_processing.py    รัน Spark แล้วโหลด warehouse
├── spark/
│   ├── main.py                    entry point ของ pipeline (10 ขั้น)
│   ├── config.py                  path (local / s3a), SparkSession, S3A config
│   ├── readers.py                 อ่าน raw จากเครื่องหรือ Data Lake
│   ├── cleaners.py                canonical schema, กฎ clean/quarantine ชุดเดียว
│   ├── deduplication.py           case_id + ลบข้อมูลซ้ำ
│   ├── standardizers.py           ชื่อเขต ชื่อโรค เพศ กลุ่มอายุ
│   ├── population.py              เตรียมข้อมูลประชากร
│   ├── curated.py                 gold รายปี + incidence rate
│   ├── warehouse.py               star schema รายเดือน
│   ├── data_quality.py            กฎ Data Quality + กระทบยอด warehouse
│   ├── writers.py                 เขียน Parquet/CSV
│   ├── load_warehouse.py          โหลด PostgreSQL ใน transaction เดียว
│   ├── prepare_population_reference.py   เตรียม CSV ประชากร 50 เขตจากแหล่งทางการ
│   └── validate_population_reference.py  ตรวจ CSV ประชากรที่เตรียมแล้ว
├── sql/
│   ├── ddl.sql                    schema mart (PK/FK/CHECK)
│   └── analytics_views.sql        semantic views สำหรับ BI
├── src/extract/                   ดึงข้อมูลจาก API และ profiling
├── tests/                         unit tests (pytest)
├── powerbi/                       DAX measures, layout, theme
├── docs/                          data sources, data contract, lake/airflow, Power BI
├── data/                          (ไม่ commit ยกเว้น data/raw/disease/reference/)
│   ├── raw/disease/               disease_cases_{ปี}_sample.json
│   │   └── reference/             population_summary_{ปี}.csv
│   ├── raw/reference/             Excel ประชากรเขตบึงกุ่ม (ต้นฉบับ)
│   └── processed/                 CSV สำหรับ BI และรายงาน DQ
├── docker-compose.yml
├── requirements.txt
└── pytest.ini
```

### Data Lake layout

```text
s3://disease-surveillance/
├── raw/
│   ├── disease/year={ปี}/disease_cases_{ปี}_sample.json  (+ _metadata/*.manifest.json)
│   └── population/year={ปี}/population_summary_{ปี}.csv
└── processed/
    ├── clean/  quarantine/  standardized/
    ├── gold/disease_with_population/curated_disease_data/
    ├── warehouse/{dim_*, fact_disease_cases, fact_population}/
    └── quality/data_quality_report/
```

---

## แหล่งข้อมูล

### 1. Disease Surveillance Data — Data.go.th Data API (JSON)

| ปี พ.ศ. | ค.ศ. | Records ทั้งหมด |
|---|---:|---:|
| 2568 | 2025 | 234,081 |
| 2569 | 2026 | 193,866 |
| **รวม** | | **427,947** |

ข้อมูลเต็มดาวน์โหลดเป็นไฟล์ CSV ต้นฉบับ (15 fields, วันที่ `D/M/YYYY`) ปีละ 1 คำขอ ปัจจุบันมี
234,081 (2568, มิ.ย.–ธ.ค. 2025) + 239,448 (2569, ม.ค.–ก.ย. 2026) = **473,529 records**
ถ้ามีทั้งไฟล์เต็มและ sample ของปีเดียวกัน ระบบใช้ไฟล์เต็ม ส่วน sample จาก Data API (100 records แรก)
มี 16 fields เช่น `ชื่อกลุ่มโรค`, `อายุ (เต็ม) ปี`, `เพศ`, `จังหวัด`, `อำเภอ/เขต`, `ตำบล/แขวง`, `วันที่เริ่มป่วย` ดูทั้งหมดที่ [docs/data_sources.md](docs/data_sources.md)

โรคในข้อมูล (หลัง Standardize): ไข้หวัดใหญ่, ไข้เลือดออก, อุจจาระร่วงเฉียบพลัน, ปอดบวม, โควิด-19

### 2. Population Reference

- **Excel เขตบึงกุ่ม** (3 แขวง ปี 2568–2569) ใช้เป็น reference และตรวจการอ่านไฟล์เท่านั้น เพราะไม่ครอบคลุมทั้งกรุงเทพฯ
- **Population 50 เขต** (`data/raw/disease/reference/population_summary_{ปี}.csv`: `ปี,เขต,ประชากรรวม`) ใช้เป็นตัวหารของ incidence rate

| ปี | สถานะ | แหล่งข้อมูล |
|---|---|---|
| 2569 | ✅ `population_summary_2569.csv` 50 เขต รวม 5,408,167 คน | สำนักงานปกครองและทะเบียน กทม. ข้อมูล ณ มิ.ย. 2569 (data.bangkok.go.th) |
| 2568 | ⏳ ยังไม่มีไฟล์ทางการ Spark ใช้ปี 2569 แทน และ DQ ติดป้าย `population_years_filled_from_latest` | ระบบสถิติของกรมการปกครองไม่อนุญาตให้ดึงข้อมูลอัตโนมัติ |

แหล่งที่มา วันที่อ้างอิง และชื่อเขตที่แก้ไข บันทึกไว้ใน [`data/raw/disease/reference/SOURCES.md`](data/raw/disease/reference/SOURCES.md)
ถ้าจะเพิ่มหรือแทนที่ไฟล์ (เช่น ปี 2568 จาก stat.bora.dopa.go.th) ให้ใช้:

```powershell
python -m spark.prepare_population_reference --project-root . --source-2568 "<ไฟล์หรือ URL ทางการ>"
python -m spark.validate_population_reference --project-root .
```

สคริปต์รับไฟล์ CSV (UTF-8 หรือ Windows-874) และ XLSX แก้ชื่อเขตที่พิมพ์ผิดแบบที่พบบ่อย
(เช่น `เเ` แทน `แ`, วรรณยุกต์ซ้ำ, ช่องว่างกลางชื่อ) ตัดแถวยอดรวม แล้วตรวจว่าครบ 50 เขตพอดี
สคริปต์ไม่สร้างตัวเลขขึ้นเอง และจะไม่เขียนทับไฟล์เดิมถ้าไม่ระบุ `--overwrite`

---

## Data Warehouse (Star Schema)

| ตาราง | Grain |
|---|---|
| `fact_disease_cases` | เดือน × เขต × โรค × กลุ่มอายุ × เพศ (`total_cases`, `source_records`) |
| `fact_population` | ปี × เขต (ตัวหารของ incidence) |
| `dim_date` | เดือน (`date_key` = yyyymm, มีเดือนที่ไม่มีผู้ป่วยด้วย) |
| `dim_district` | ครบ 50 เขตเสมอ |
| `dim_disease`, `dim_age_group`, `dim_sex` | |

Views: `vw_monthly_trend`, `vw_year_disease`, `vw_district_disease_year`, `vw_district_ranking`, `vw_demographics`, `vw_disease_overview`, `vw_year_period`

ใน view รายปี `cumulative_incidence_per_100k` เป็น**อัตราสะสมตามช่วงที่มีข้อมูล** และมีคอลัมน์ `months_covered`
กับ `period_label` (เช่น "มิ.ย. 2568 – ธ.ค. 2568", 7 เดือน) กำกับไว้ ไม่ใช่อัตราทั้งปี

`load_warehouse` ทำงานใน transaction เดียว (DDL → COPY → ตรวจจำนวนแถว → views → `load_audit`) ถ้าล้มจะ rollback ข้อมูลเดิมไม่เสียหาย

### Power BI

- **Power BI บนเว็บ (ไม่ต้องใช้ Desktop)**: export warehouse เป็นไฟล์ Excel แล้วทำตาม [docs/powerbi_web_guide.md](docs/powerbi_web_guide.md)
  ```powershell
  docker compose exec airflow bash -c "cd /opt/airflow/project && python -m spark.export_powerbi"
  # → data/processed/powerbi/disease_surveillance_powerbi.xlsx
  ```
- **Power BI Desktop**: ต่อ PostgreSQL `localhost:5433` ตรง ดู [docs/powerbi_dashboard.md](docs/powerbi_dashboard.md)

---

## Data Quality

รายงานอยู่ที่ `data/processed/quality/data_quality_report.csv` (และ Parquet บน lake) โดย DAG จะล้มเมื่อมี rule เป็น `FAIL` ตัวอย่าง rule:

- จำนวนแถวไม่ว่างทุกชั้น และสัดส่วนข้อมูลที่ผ่าน clean ≥ 50%
- ชื่อเขตอยู่ในรายชื่อ 50 เขตของกรุงเทพฯ
- primary key ไม่ซ้ำ ทั้ง gold และ warehouse
- สูตร incidence ถูกต้อง และแถวที่ไม่มี incidence ตรงกับแถวที่ไม่มีประชากร
- สัดส่วนข้อมูลประชากรที่ขาด ≤ 5% (`SKIP` ในโหมด cases-only)
- ไม่มีอายุผิดช่วงหลัง clean และปีของไฟล์ตรงกับปีของวันที่เริ่มป่วย
- ยอดผู้ป่วยใน `fact_disease_cases` เท่ากับยอดใน standardized

แถวที่ไม่ผ่าน validation ถูกเก็บใน `processed/quarantine/` พร้อมสาเหตุ (`_quarantine_reason`)

---

## Tests

```powershell
pip install -r requirements.txt   # ต้องมี Java 17 สำหรับ PySpark
pytest
```

หรือรันใน container ที่มี Spark และ Airflow ครบอยู่แล้ว (รวมเทสต์ของ DAG):

```powershell
docker compose exec airflow bash -c "cd /opt/airflow/project && python -m pytest"
```

เทสต์ของ DAG (`tests/test_dags.py`) จะถูกข้ามอัตโนมัติถ้าไม่มี Airflow ในเครื่อง

---

## ข้อควรระวังในการตีความผล

- **Sample bias**: ไฟล์ sample จาก API คือ 100 records แรกของแต่ละปี ใช้ทดสอบ pipeline เท่านั้น และ**วันที่ใน JSON ของ API ผิด** (datastore อ่าน `1/6/2025` เป็น 6 ม.ค.) ใช้ไฟล์เต็มสำหรับการวิเคราะห์
- **ช่วงเวลาไม่ครบปี**: ปี 2568 เริ่ม มิ.ย. 2025 จึงห้ามเทียบยอดรวมทั้งปี 2568 กับ 2569 ตรง ๆ ให้เทียบรายเดือนที่ตรงกัน
- **ไข้เลือดออก** ปี 2569 รวม DF + DHF + DSS (ไข้เลือดออกรวม) เพื่อให้เทียบกับปี 2568 ได้
- **Incidence rate** แสดงเฉพาะเมื่อมีข้อมูลประชากรครบ 50 เขต และไม่คำนวณแยกตามอายุ/เพศ (ไม่มีประชากรแยกอายุ/เพศ)
- **ตัวหารคือประชากรตามทะเบียนราษฎร** เขตชั้นในที่มีประชากรแฝงมาก (เช่น วัฒนา ห้วยขวาง ราชเทวี) อาจมีอัตราสูงเกินจริง และปี 2568 ใช้ประชากร มิ.ย. 2569 แทน
- **ชื่อโรคต่างกันระหว่างปี** เช่น `อุจจาระร่วง` (2568) กับ `โรคอุจจาระร่วงเฉียบพลัน` (2569) ถูกรวมเป็น `อุจจาระร่วงเฉียบพลัน` ใน Spark โดยไม่แก้ Raw

---

## Raw Data Policy และ Ethical Data Ingestion

Raw Data ต้องคงค่าตามต้นทาง ห้ามเปลี่ยนชื่อคอลัมน์ ลบ missing/duplicate, standardize, แปลงวันที่ หรือ aggregate ใน Raw Layer การแปลงทั้งหมดทำใน Spark DAG ตรวจ checksum และจะไม่เขียนทับ object เดิมที่เนื้อหาเปลี่ยน (ต้องใช้ชื่อไฟล์ใหม่)

การใช้ API:

1. ดึงเฉพาะเท่าที่จำเป็น ใช้ `limit` และไม่เรียกซ้ำเมื่อมี Raw อยู่แล้ว
2. เคารพ Authentication และ Rate Limit ไม่พยายาม bypass
3. เก็บ token ใน `.env` เท่านั้น ห้าม hard-code หรือ commit
4. การดึงข้อมูลเต็มควรแบ่งหน้า หน่วงเวลาระหว่างคำขอ และ retry แบบ backoff หรือใช้ไฟล์ดาวน์โหลดทั้งชุดจากหน้า dataset

---

## การแบ่งงาน

| Part | ขอบเขต | ผลลัพธ์หลัก |
|---|---|---|
| 1 — Data Sources & Ingestion | API, authentication, extraction, profiling, data contract | `src/extract/`, `docs/data_sources.md`, `docs/data_contract.md` |
| 2 — Data Lake & Airflow | SeaweedFS, DAG, retry, logging, raw zone | `docker-compose.yml`, `airflow/` |
| 3 — Spark & Data Quality | cleaning, standardization, dedup, DQ, gold | `spark/` |
| 4 — Warehouse & Dashboard | PostgreSQL star schema, SQL views, Power BI | `sql/`, `spark/load_warehouse.py`, `powerbi/` |

## สถานะปัจจุบัน

| ส่วน | สถานะ |
|---|---|
| Ingestion จาก API (sample) | ✅ ใช้งานได้ |
| Raw zone บน Data Lake + Airflow | ✅ ใช้งานได้ |
| Spark อ่าน/เขียนผ่าน Data Lake + DQ | ✅ ใช้งานได้ |
| PostgreSQL Warehouse + views | ✅ ใช้งานได้ |
| Unit tests | ✅ 57 tests |
| ข้อมูลประชากร 50 เขต (incidence rate) | ✅ ปี 2569 (มิ.ย. 2569) / ⏳ ปี 2568 ใช้ปี 2569 แทนจนกว่าจะได้ไฟล์ทางการ |
| ข้อมูลเต็ม (473,529 records, มิ.ย. 2025 – ก.ย. 2026) | ✅ รันผ่านทั้ง pipeline |
| ไฟล์ Excel สำหรับ Power BI | ✅ `spark/export_powerbi.py` |
| รายงาน Power BI | ⏳ กำลังทำบน Power BI Service ตาม [docs/powerbi_web_guide.md](docs/powerbi_web_guide.md) |

---

## เอกสารเพิ่มเติม

- [docs/data_sources.md](docs/data_sources.md) — แหล่งข้อมูลและผล profiling
- [docs/data_contract.md](docs/data_contract.md) — schema และข้อตกลงระหว่างแต่ละ Part
- [docs/data_lake_airflow.md](docs/data_lake_airflow.md) — การใช้งาน Data Lake/Airflow/Spark และการแก้ปัญหา
- [docs/powerbi_web_guide.md](docs/powerbi_web_guide.md) — ทำรายงาน Power BI บนเว็บทีละขั้น (ไม่ต้องใช้ Desktop)
- [docs/powerbi_dashboard.md](docs/powerbi_dashboard.md) — การเชื่อมต่อ Power BI Desktop กับ PostgreSQL, model และ measures

---

โปรเจกต์นี้มีวัตถุประสงค์เพื่อการศึกษาและการวิเคราะห์ข้อมูลเฝ้าระวังโรคจากข้อมูลที่หน่วยงานภาครัฐเผยแพร่ ผลจาก Development Sample ไม่ควรตีความว่าเป็นสถิติทางระบาดวิทยาของประชากรทั้งหมด
