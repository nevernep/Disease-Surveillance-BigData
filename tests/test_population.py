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
