# Unit tests for the silver-layer transformations.

from src.rail.transforms import (
    typed_stop_events, deduplicate_stop_events, enrich_stations, station_match_key,
)
from conftest import RAW_SCHEMA
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.rail.transforms import (MONTHLY_DATE_COLUMNS, count_unparsed_dates, normalize_monthly_dates,)

def raw_row(train_no="1234", delay_arr="120", ingested="2026-08-09 06:00:00"):
    return (
        "2026-08-08", train_no, "BRUSSEL-MECHELEN", "A", "NMBS",
        "5678", "Mechelen", "25", "25",
        "2026-08-08", "08:10:00", "2026-08-08", "08:12:00",
        "2026-08-08", "08:12:00", "2026-08-08", "08:14:00",
        delay_arr, "100", ingested,
    )

def test_punctuality_uses_the_infrabel_threshold(spark):
    df = spark.createDataFrame(
        [raw_row(delay_arr="120"), raw_row(train_no="9999", delay_arr="600")], RAW_SCHEMA
    )
    result = {r.train_no: r.is_punctual_arr for r in typed_stop_events(df).collect()}
    assert result[1234]        # 2 min -> punctual
    assert not result[9999]    # 10 min -> not punctual

def test_early_arrivals_are_kept(spark):
    """Negative delays mean the train ran early. They are valid, not errors."""
    df = spark.createDataFrame([raw_row(delay_arr="-60")], RAW_SCHEMA)
    row = typed_stop_events(df).collect()[0]
    assert row.delay_arr_s == -60
    assert row.is_punctual_arr

def test_dedup_keeps_the_latest_ingestion(spark):
    """The D-1 export is overwritten daily, so the same event can arrive twice."""
    df = spark.createDataFrame(
        [
            raw_row(delay_arr="120", ingested="2026-08-09 06:00:00"),
            raw_row(delay_arr="300", ingested="2026-08-09 07:00:00"),
        ],
        RAW_SCHEMA,
    )
    rows = deduplicate_stop_events(typed_stop_events(df)).collect()
    assert len(rows) == 1
    assert rows[0].delay_arr_s == 300

def test_rows_without_a_natural_key_are_dropped(spark):
    df = spark.createDataFrame([raw_row(train_no=None)], RAW_SCHEMA)
    assert typed_stop_events(df).count() == 0



MONTHLY_DATE_SCHEMA = StructType(
    [StructField(c, StringType(), True) for c in MONTHLY_DATE_COLUMNS]
)


def test_monthly_dates_normalize_to_iso(spark):
    df = spark.createDataFrame(
        [("01JUL2026", "01JUL2026", "01JUL2026", "01JUL2026", "01JUL2026")],
        MONTHLY_DATE_SCHEMA,
    )
    row = normalize_monthly_dates(df).collect()[0]
    assert all(row[c] == "2026-07-01" for c in MONTHLY_DATE_COLUMNS)


def test_null_planned_dates_survive_normalization(spark):
    """A stop with no planned arrival is valid data, not a parse failure."""
    df = spark.createDataFrame(
        [("01JUL2026", None, "01JUL2026", "01JUL2026", "01JUL2026")],
        MONTHLY_DATE_SCHEMA,
    )
    row = normalize_monthly_dates(df).collect()[0]
    assert row.PLANNED_DATE_ARR is None
    assert row.DATDEP == "2026-07-01"
    assert count_unparsed_dates(df)["PLANNED_DATE_ARR"] == 0


def test_unparseable_month_abbreviation_is_counted(spark):
    """Guards against a historical file using non-English month names."""
    df = spark.createDataFrame(
        [("01JUIL2026", None, None, None, None)], MONTHLY_DATE_SCHEMA
    )
    assert count_unparsed_dates(df)["DATDEP"] == 1

def test_typed_events_label_source_feed(spark):
    """The origin label defaults to daily and is emitted on every row."""
    df = spark.createDataFrame([raw_row()], RAW_SCHEMA)
    default_row = typed_stop_events(df).collect()[0]
    monthly_row = typed_stop_events(df, source_feed="monthly").collect()[0]
    assert default_row.source_feed == "daily"
    assert monthly_row.source_feed == "monthly"


def test_native_ptcar_is_read_only_when_requested(spark):
    """The daily feed carries no PTCAR_NO; only the monthly path reads it natively."""
    df = spark.createDataFrame([raw_row()], RAW_SCHEMA)
    without = typed_stop_events(df).collect()[0]
    withp = typed_stop_events(df, with_native_ptcar=True).collect()[0]
    assert without.ptcar_no is None
    assert withp.ptcar_no == 5678  # PTCAR_NO position in raw_row's conftest schema


STATIONS_SCHEMA = StructType([
    StructField("station_key", StringType(), True),
    StructField("station_name", StringType(), True),
    StructField("ptcar_no", IntegerType(), True),
])

OPERATIONAL_POINTS_SCHEMA = StructType([
    StructField("ptcarid", StringType(), True),
    StructField("longnamedutch", StringType(), True),
    StructField("class_en", StringType(), True),
    StructField("geo_point_2d", StringType(), True),
    StructField("taftapcode", StringType(), True),
])

IRAIL_STATIONS_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("alternative-nl", StringType(), True),
    StructField("alternative-fr", StringType(), True),
    StructField("country-code", StringType(), True),
    StructField("taf-tap-code", StringType(), True),
])


def no_irail_rows(spark):
    return spark.createDataFrame([], IRAIL_STATIONS_SCHEMA)


def test_enrich_stations_matches_by_ptcar_id(spark):
    """ID match wins even when the reference name differs from dim_station's."""
    stations = spark.createDataFrame(
        [("k1", "ANY NAME", 123)], STATIONS_SCHEMA
    )
    ops = spark.createDataFrame(
        [("123", "Different Name", "Station", "51.0182, 4.4801", None)], OPERATIONAL_POINTS_SCHEMA
    )
    row = enrich_stations(stations, ops, no_irail_rows(spark)).collect()[0]
    assert row.station_type == "Station"
    assert row.latitude == 51.0182
    assert row.longitude == 4.4801


def test_enrich_stations_falls_back_to_name(spark):
    """An unmatched PTCAR ID still resolves through the normalized name."""
    stations = spark.createDataFrame(
        [("k2", "Mortsel-Deurnesteenweg", None)], STATIONS_SCHEMA
    )
    ops = spark.createDataFrame(
        [("9", "MORTSEL-DEURNESTEENWEG", "Stop in open track", "51.19, 4.48", None)],
        OPERATIONAL_POINTS_SCHEMA,
    )
    row = enrich_stations(stations, ops, no_irail_rows(spark)).collect()[0]
    assert row.station_type == "Stop in open track"


def test_enrich_stations_leaves_ambiguous_name_unmatched(spark):
    """A reference name shared by two distinct PTCAR IDs is not usable for the fallback."""
    stations = spark.createDataFrame([("k3", "BAULERS", None)], STATIONS_SCHEMA)
    ops = spark.createDataFrame(
        [
            ("1", "BAULERS", "Station", "50.6, 4.5", None),
            ("2", "BAULERS", "Station", "50.6, 4.5", None),
        ],
        OPERATIONAL_POINTS_SCHEMA,
    )
    row = enrich_stations(stations, ops, no_irail_rows(spark)).collect()[0]
    assert row.station_type is None
    assert row.latitude is None


def test_enrich_stations_parses_geo_point(spark):
    """geo_point_2d is 'lat, lon' text, with or without a space after the comma."""
    stations = spark.createDataFrame([("k6", "MECHELEN", 42)], STATIONS_SCHEMA)
    ops = spark.createDataFrame(
        [("42", "Mechelen", "Station", "50.8503,4.3517", None)], OPERATIONAL_POINTS_SCHEMA
    )
    row = enrich_stations(stations, ops, no_irail_rows(spark)).collect()[0]
    assert row.latitude == 50.8503
    assert row.longitude == 4.3517


def test_station_match_key_handles_ligature_accent_and_punctuation(spark):
    """OE ligature, accents, parentheses and hyphens all collapse to the same key."""
    df = spark.createDataFrame(
        [("Sint-Gillis-Dendermonde",), ("SINT-GILLIS(DENDERMONDE)",), ("œuvres-Éclairées",)],
        ["station_name"],
    )
    keys = [r.k for r in df.select(station_match_key("station_name").alias("k")).collect()]
    assert keys[0] == keys[1] == "SINTGILLISDENDERMONDE"
    assert keys[2] == "OEUVRESECLAIREES"


def test_enrich_stations_name_match_splits_bilingual_slash(spark):
    """'Haren-Sud/Haren-Zuid' yields two valid keys, one per language."""
    stations = spark.createDataFrame([("k1", "Haren-Zuid", None)], STATIONS_SCHEMA)
    ops = spark.createDataFrame([], OPERATIONAL_POINTS_SCHEMA)
    irail = spark.createDataFrame(
        [("Haren-Sud/Haren-Zuid", None, None, "be", "100")], IRAIL_STATIONS_SCHEMA
    )
    row = enrich_stations(stations, ops, irail).collect()[0]
    assert row.is_passenger


def test_enrich_stations_name_absent_from_operational_point_still_matches(spark):
    """Pass 1 uses dim_station.station_name directly: no operational_point row needed."""
    stations = spark.createDataFrame([("k1", "Aalst", None)], STATIONS_SCHEMA)
    ops = spark.createDataFrame([], OPERATIONAL_POINTS_SCHEMA)
    irail = spark.createDataFrame([("Aalst", None, None, "be", "880")], IRAIL_STATIONS_SCHEMA)
    row = enrich_stations(stations, ops, irail).collect()[0]
    assert row.is_passenger


def test_enrich_stations_pass2_rejects_duplicated_code(spark):
    """A code shared by two candidate iRail rows cannot be trusted as a fallback."""
    stations = spark.createDataFrame([("k1", "Unrelated Name", 1)], STATIONS_SCHEMA)
    ops = spark.createDataFrame([("1", "Other", "Station", None, "100")], OPERATIONAL_POINTS_SCHEMA)
    irail = spark.createDataFrame(
        [
            ("Station A", None, None, "be", "100"),
            ("Station B", None, None, "be", "100"),
        ],
        IRAIL_STATIONS_SCHEMA,
    )
    row = enrich_stations(stations, ops, irail).collect()[0]
    assert not row.is_passenger


def test_enrich_stations_pass2_rejects_row_already_claimed_by_name(spark):
    """A station matched by name in pass 1 cannot also let pass 2 claim its iRail row for another station."""
    stations = spark.createDataFrame(
        [("k1", "Real Name", None), ("k2", "Impostor", 2)], STATIONS_SCHEMA
    )
    ops = spark.createDataFrame([("2", "Other", "Station", None, "200")], OPERATIONAL_POINTS_SCHEMA)
    irail = spark.createDataFrame([("Real Name", None, None, "be", "200")], IRAIL_STATIONS_SCHEMA)
    rows = {r.station_key: r.is_passenger for r in enrich_stations(stations, ops, irail).collect()}
    assert rows["k1"]
    assert not rows["k2"]


def test_enrich_stations_yard_with_colliding_code_stays_false(spark):
    """A yard whose class_en is 'Station' but shares operational_point's taftapcode with an
    unrelated NMBS station is not made passenger by station_type alone."""
    stations = spark.createDataFrame([("k1", "Some Yard", 1)], STATIONS_SCHEMA)
    ops = spark.createDataFrame([("1", "Some Yard", "Station", None, "300")], OPERATIONAL_POINTS_SCHEMA)
    irail = spark.createDataFrame(
        [
            ("NMBS Station One", None, None, "be", "300"),
            ("NMBS Station Two", None, None, "be", "300"),
        ],
        IRAIL_STATIONS_SCHEMA,
    )
    row = enrich_stations(stations, ops, irail).collect()[0]
    assert row.station_type == "Station"
    assert not row.is_passenger