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

| | |
|---|---|
| Publisher | กรมการปกครอง (DOPA), via data.go.th dataset `dopa-star` "สถิติจำนวนประชากร" |
| Resource | ข้อมูลสถิติจำนวนประชากร พ.ศ.2567 (`stat_67.zip`, district file `stat_a67.xls`) |
| Resource URL | https://catalog.dopa.go.th/dataset/221a73a3-2223-4fdc-a79e-6738ff40bb57/resource/3100169d-d5f8-4719-8bc4-b319fe27814a/download/stat_67.zip |
| SHA-256 of the zip | `c524ebf1b0c714aba291aa54e27a439dc7605f11f5c7b2a85f805b57d74ac184` (2,354,820 bytes) |
| Reference date | 31 Dec 2567 (`ปีเดือน` = 6712), registered Thai population |
| Downloaded | 2026-10-08 |
| Total | 5,455,020 (matches the file's own Bangkok total row) |

Why this file for 2568: the registered population at the end of the previous
year is the usual denominator for a year's disease rates.

Bangkok districts appear as registration offices named `ท้องถิ่นเขต<district>`;
the normalizer drops the `ท้องถิ่น` prefix, and the province total row (`-`)
and all other provinces are removed before validation.

Download note: catalog.dopa.go.th serves only its leaf certificate (the
Sectigo "Public Server Authentication CA DV R36" intermediate is missing), so
some clients reject the connection. The file was downloaded with full chain
verification by adding that intermediate, fetched from the certificate's own
AIA URL (http://crt.sectigo.com/SectigoPublicServerAuthenticationCADVR36.crt),
to the standard root bundle. Verification was not disabled.

To rebuild:

```powershell
python -m spark.prepare_population_reference --project-root . --source-2568 stat_67.zip --overwrite
```

Not used: the data.bangkok.go.th district file `bkkpopulationdistric`
(total 5,471,588) does not state its reference year.
