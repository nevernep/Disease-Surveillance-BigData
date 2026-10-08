-- =====================================================================
-- คำสั่งสาธิตคลังข้อมูลเฝ้าระวังโรค (PostgreSQL: localhost:5433 / surveillance_dw)
-- เปิดใน DBeaver แล้ววางเคอร์เซอร์ในคำสั่งที่ต้องการ กด Ctrl+Enter เพื่อรันทีละข้อ
-- =====================================================================


-- 1) ภาพรวมคลังข้อมูล: แต่ละตารางใน Star Schema มีกี่แถว
SELECT 'fact_disease_cases' AS ตาราง, COUNT(*) AS จำนวนแถว, 'เดือน × เขต × โรค × กลุ่มอายุ × เพศ' AS ระดับข้อมูล FROM mart.fact_disease_cases
UNION ALL SELECT 'fact_population', COUNT(*), 'ปี × เขต (ตัวหารอัตราป่วย)' FROM mart.fact_population
UNION ALL SELECT 'dim_date',        COUNT(*), 'เดือน' FROM mart.dim_date
UNION ALL SELECT 'dim_district',    COUNT(*), 'เขต (ครบ 50 เขต)' FROM mart.dim_district
UNION ALL SELECT 'dim_disease',     COUNT(*), 'โรค' FROM mart.dim_disease
UNION ALL SELECT 'dim_age_group',   COUNT(*), 'กลุ่มอายุ' FROM mart.dim_age_group
UNION ALL SELECT 'dim_sex',         COUNT(*), 'เพศ' FROM mart.dim_sex
UNION ALL SELECT 'data_quality',    COUNT(*), 'กฎตรวจคุณภาพข้อมูลจาก Spark' FROM mart.data_quality;


-- 2) กระทบยอด: ข้อมูลดิบ → ตัดแถวที่ไม่ผ่านการตรวจ → ยอดในคลังข้อมูลต้องตรงกันพอดี
SELECT
    dq_raw.observed::int                         AS รายงานดิบ,
    dq_q.observed::int                           AS แยกไป_quarantine,
    dq_raw.observed::int - dq_q.observed::int    AS ควรเหลือ,
    (SELECT SUM(total_cases) FROM mart.fact_disease_cases) AS ยอดในคลังข้อมูล,
    CASE WHEN dq_raw.observed::int - dq_q.observed::int
              = (SELECT SUM(total_cases) FROM mart.fact_disease_cases)
         THEN 'ตรงกัน' ELSE 'ไม่ตรง' END AS ผลกระทบยอด
FROM mart.data_quality AS dq_raw
JOIN mart.data_quality AS dq_q ON dq_q.rule = 'quarantined_rows'
WHERE dq_raw.rule = 'raw_rows_not_empty';


-- 3) ประวัติการโหลดเข้าคลังข้อมูล (Airflow โหลดอัตโนมัติหลัง Spark ทำงานเสร็จ)
SELECT load_id,
       loaded_at AT TIME ZONE 'Asia/Bangkok' AS เวลาโหลด,
       row_counts ->> 'fact_disease_cases'   AS แถว_fact,
       row_counts ->> 'fact_population'      AS แถว_ประชากร
FROM mart.load_audit
ORDER BY load_id DESC
LIMIT 5;


-- 4) Star Schema join: ผู้ป่วยแยกตามปีและโรค (เชื่อม fact กับ dimension 2 ตาราง)
SELECT d.year_be                AS ปี_พศ,
       dse.disease_name         AS โรค,
       SUM(f.total_cases)       AS ผู้ป่วย
FROM mart.fact_disease_cases AS f
JOIN mart.dim_date    AS d   ON d.date_key = f.date_key
JOIN mart.dim_disease AS dse ON dse.disease_key = f.disease_key
GROUP BY d.year_be, dse.disease_name
ORDER BY d.year_be, ผู้ป่วย DESC;


-- 5) อัตราป่วยสะสมต่อแสนประชากร ระดับกรุงเทพฯ (view สำเร็จรูป)
--    เป็นอัตราสะสมตามช่วงที่มีข้อมูล: 2568 = 7 เดือน, 2569 = 9 เดือน
SELECT year_be                         AS ปี_พศ,
       period_label                    AS ช่วงข้อมูล,
       disease_name                    AS โรค,
       total_cases                     AS ผู้ป่วย,
       population                      AS ประชากร,
       cumulative_incidence_per_100k   AS อัตราต่อแสน
FROM mart.vw_year_disease
ORDER BY year_be, cumulative_incidence_per_100k DESC;


-- 6) แนวโน้มรายเดือน: ผู้ป่วยแต่ละโรคในแต่ละเดือน (ตารางไขว้)
SELECT month_label AS เดือน,
       SUM(total_cases) FILTER (WHERE disease_name = 'ไข้หวัดใหญ่')          AS ไข้หวัดใหญ่,
       SUM(total_cases) FILTER (WHERE disease_name = 'อุจจาระร่วงเฉียบพลัน') AS อุจจาระร่วง,
       SUM(total_cases) FILTER (WHERE disease_name = 'โควิด-19')             AS โควิด19,
       SUM(total_cases) FILTER (WHERE disease_name = 'ปอดบวม')              AS ปอดบวม,
       SUM(total_cases) FILTER (WHERE disease_name = 'ไข้เลือดออก')          AS ไข้เลือดออก,
       SUM(total_cases)                                                      AS รวม
FROM mart.vw_monthly_trend
GROUP BY month_start, month_label
ORDER BY month_start;


-- 7) เดือนที่ระบาดสูงสุดของแต่ละโรค (Window function)
SELECT โรค, เดือน, ผู้ป่วย
FROM (
    SELECT disease_name AS โรค, month_label AS เดือน, total_cases AS ผู้ป่วย,
           ROW_NUMBER() OVER (PARTITION BY disease_name ORDER BY total_cases DESC) AS อันดับ
    FROM mart.vw_monthly_trend
) AS ranked
WHERE อันดับ = 1
ORDER BY ผู้ป่วย DESC;


-- 8) 10 เขตที่อัตราป่วยสูงสุด ปี 2569 (ทุกโรครวมกัน, ตัวหาร = ประชากรเขตนั้น)
SELECT dst.district_name                                     AS เขต,
       SUM(f.total_cases)                                    AS ผู้ป่วย,
       p.population                                          AS ประชากร,
       ROUND(SUM(f.total_cases) * 100000.0 / p.population, 1) AS อัตราต่อแสน
FROM mart.fact_disease_cases AS f
JOIN mart.dim_date        AS d   ON d.date_key = f.date_key
JOIN mart.dim_district    AS dst ON dst.district_key = f.district_key
JOIN mart.fact_population AS p   ON p.district_key = f.district_key AND p.year_be = d.year_be
WHERE d.year_be = 2569
GROUP BY dst.district_name, p.population
ORDER BY อัตราต่อแสน DESC
LIMIT 10;


-- 9) สัดส่วนผู้ป่วยตามกลุ่มอายุและเพศ ปี 2569 (ทุกโรค)
SELECT age_group AS กลุ่มอายุ,
       SUM(total_cases) FILTER (WHERE sex_label = 'ชาย')  AS ชาย,
       SUM(total_cases) FILTER (WHERE sex_label = 'หญิง') AS หญิง,
       SUM(total_cases)                                   AS รวม,
       ROUND(100.0 * SUM(total_cases) / SUM(SUM(total_cases)) OVER (), 1) AS ร้อยละ
FROM mart.vw_demographics
WHERE year_be = 2569
GROUP BY age_group_key, age_group
ORDER BY age_group_key;


-- 10) ข้อมูลประชากรที่ใช้เป็นตัวหาร: ครบ 50 เขตทั้งสองปี
SELECT year_be AS ปี_พศ,
       COUNT(*) AS จำนวนเขต,
       SUM(population) AS ประชากรรวม,
       CASE year_be WHEN 2568 THEN 'กรมการปกครอง ณ 31 ธ.ค. 2567'
                    WHEN 2569 THEN 'สำนักงานปกครองและทะเบียน กทม. ณ มิ.ย. 2569' END AS แหล่งข้อมูล
FROM mart.fact_population
GROUP BY year_be
ORDER BY year_be;


-- 11) คุณภาพข้อมูล: สรุปผลตรวจ แล้วดูกฎที่สำคัญ
SELECT status AS ผล, COUNT(*) AS จำนวนกฎ
FROM mart.data_quality
GROUP BY status;

SELECT layer AS ชั้นข้อมูล, rule AS กฎ, observed AS ค่าที่พบ, threshold AS เกณฑ์, status AS ผล
FROM mart.data_quality
WHERE threshold <> 'informational'
ORDER BY layer, rule;
