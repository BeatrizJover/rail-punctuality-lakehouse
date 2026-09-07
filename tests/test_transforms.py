# Unit tests for the silver-layer transformations.
import pytest
from src.rail.transforms import typed_stop_events, deduplicate_stop_events
from conftest import RAW_SCHEMA

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

from pyspark.sql.types import StructType, StructField, StringType

from src.rail.transforms import (
    MONTHLY_DATE_COLUMNS,
    count_unparsed_dates,
    normalize_monthly_dates,
)

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