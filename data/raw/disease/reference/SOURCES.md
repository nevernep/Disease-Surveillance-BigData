# Population reference sources

Files in this folder are inputs to the incidence-rate denominator
(`fact_population`). They are produced by `spark/prepare_population_reference.py`
from official files and validated by `spark/validate_population_reference.py`
(exactly the 50 Bangkok districts, positive integers). No value is typed by hand.

## population_summary_2569.csv

| | |
|---|---|
| Publisher | สำนักงานปกครองและทะเบียน กรุงเทพมหานคร (via data.bangkok.go.th) |
| Dataset | ชุดข้อมูลจำนวนประชากรทั้งหมดของกรุงเทพมหานคร (`data-on-the-total-population-of-bangkok-in-2026`) |
| Resource URL | https://data.bangkok.go.th/dataset/c73e286d-65be-4562-badd-e8d7b4fdeda5/resource/7c9e4655-e61f-4488-a00c-e2f8f8fac941/download/data-on-the-total-population-of-bangkok-in-2026.csv |
| Reference date | June 2569 (มิถุนายน 2569), registered population by district |
| Resource last modified | 2026-08-31 |
| Downloaded | 2026-10-07 |
| Total | 5,408,167 (matches the source's own total row) |

Corrections applied by the normalizer (source typos, not data changes):
`ดินเเดง`, `บางเเค`, `หนองเเขม` ("เเ" → "แ"), `วัังทองหลาง` (doubled mark),
`ป้อมปราบ ศัตรูพ่าย` (space). The source total row `ยอรวม` is dropped.

## population_summary_2568.csv

Not available yet. The Department of Provincial Administration statistics
site (stat.bora.dopa.go.th) rejects automated requests, and the other
data.bangkok.go.th district file (`bkkpopulationdistric`) does not state its
reference year, so it is not used.

Until an official 2568 file is added, Spark fills 2568 with the latest available
year (2569) and the Data Quality report flags it
(`population_years_filled_from_latest`).
