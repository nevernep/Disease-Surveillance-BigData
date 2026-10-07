from spark.population import canonicalize_population_data, fill_missing_years


def test_canonicalize_population_drops_totals_and_normalizes(spark, settings):
    raw = spark.createDataFrame(
        [
            ("2568", "เขตบางกะปิ", "140,000"),
            ("2568", "รวม", "5,000,000"),
            ("2025", "ป้อมปราบฯ", "50000"),
            ("2568", "ดุสิต", "0"),
        ],
        ["ปี", "เขต", "ประชากรรวม"],
    )

    rows = {
        (row["year_be"], row["district_name"]): row["population"]
        for row in canonicalize_population_data(raw, settings).collect()
    }

    assert rows == {
        (2568, "บางกะปิ"): 140000.0,
        (2568, "ป้อมปราบศัตรูพ่าย"): 50000.0,
    }


def test_fill_missing_years_uses_latest_population(spark):
    population = spark.createDataFrame(
        [(2568, "บางกะปิ", 140000.0)],
        "year_be int, district_name string, population double",
    )
    disease = spark.createDataFrame([(2568,), (2569,)], "year_be int")

    filled = fill_missing_years(population, disease)

    assert {(row["year_be"], row["population"]) for row in filled.collect()} == {
        (2568, 140000.0),
        (2569, 140000.0),
    }


def _official_style_csv(tmp_path, encoding="utf-8-sig"):
    """Mimics data.bangkok.go.th's district file: English headers, "เขต" prefix,
    real typos from the 2569 file, and a misspelled total row."""
    from spark.schemas import BANGKOK_DISTRICTS

    typos = {
        "ดินแดง": "ดินเเดง",
        "บางแค": "บางเเค",
        "วังทองหลาง": "วัังทองหลาง",
        "ป้อมปราบศัตรูพ่าย": "ป้อมปราบ ศัตรูพ่าย",
    }
    lines = ["District,male,female,total"]
    for name in BANGKOK_DISTRICTS:
        lines.append(f"เขต{typos.get(name, name)},400,600,1000")
    lines.append(f"ยอรวม,20000,30000,{1000 * len(BANGKOK_DISTRICTS)}")
    path = tmp_path / "official.csv"
    path.write_bytes(("\n".join(lines) + "\n").encode(encoding))
    return path


def test_prepare_year_handles_official_district_file(tmp_path):
    from spark.prepare_population_reference import prepare_year
    from spark.schemas import BANGKOK_DISTRICTS

    frame = prepare_year(str(_official_style_csv(tmp_path)), 2569)

    assert list(frame.columns) == ["ปี", "เขต", "ประชากรรวม"]
    assert set(frame["เขต"]) == set(BANGKOK_DISTRICTS)
    assert frame["ประชากรรวม"].sum() == 1000 * len(BANGKOK_DISTRICTS)


def test_prepare_year_reads_thai_windows_encoding(tmp_path):
    from spark.prepare_population_reference import prepare_year

    frame = prepare_year(str(_official_style_csv(tmp_path, "cp874")), 2568)

    assert len(frame) == 50
