# Unit tests for station identity resolution.

import datetime as dt

from src.rail.identity import (
    STATION_ALIAS_SCHEMA, alias_changes, alias_upserts, resolve_station_aliases,
)

OBSERVED_SCHEMA = (
    "stop_point_key string, stop_point_name_key string, stop_point_name string, "
    "ptcar_no int, source_feed string, service_date date"
)
REFERENCE_SCHEMA = "ptcarid string, longnamedutch string"


def obs(name, ptcar_no=None, feed="daily", day="2026-08-01"):
    # Plain uppercase names, so the key equals normalize_station_name(name).
    return (f"key_{name}", name, name, ptcar_no, feed, dt.date.fromisoformat(day))


def resolve(spark, observed, reference, existing=None):
    if existing is None:
        existing = spark.createDataFrame([], STATION_ALIAS_SCHEMA)
    return resolve_station_aliases(
        spark.createDataFrame(observed, OBSERVED_SCHEMA),
        spark.createDataFrame(reference, REFERENCE_SCHEMA),
        existing,
    )


def by_key(df):
    return {r.stop_point_key: r for r in df.collect()}


def test_monthly_native_wins_over_reference(spark):
    row = resolve(spark, [obs("MECHELEN", 100, "monthly")], [("200", "Mechelen")]).first()
    assert (row.ptcar_no, row.status, row.resolution_source) == (100, "resolved", "monthly_native")


def test_two_monthly_ptcar_no_is_ambiguous(spark):
    row = resolve(
        spark,
        [obs("MECHELEN", 100, "monthly"), obs("MECHELEN", 101, "monthly", day="2026-08-02")],
        [("100", "Mechelen")],
    ).first()
    assert (row.ptcar_no, row.status, row.resolution_source) == (None, "ambiguous", "monthly_native")


def test_reference_cannot_override_monthly_alias(spark):
    first = resolve(spark, [obs("MECHELEN", 100, "monthly")], [])
    row = resolve(spark, [obs("MECHELEN", day="2026-08-02")], [("200", "Mechelen")], first).first()
    assert (row.ptcar_no, row.status, row.resolution_source) == (100, "resolved", "monthly_native")


def test_ambiguous_reference_name_is_not_used(spark):
    row = resolve(spark, [obs("MECHELEN")], [("200", "Mechelen"), ("201", "MECHELEN ")]).first()
    assert (row.ptcar_no, row.status, row.resolution_source) == (None, "unresolved", None)


def test_no_evidence_is_unresolved(spark):
    row = resolve(spark, [obs("MECHELEN")], [("200", "Leuven")]).first()
    assert (row.ptcar_no, row.status, row.resolution_source) == (None, "unresolved", None)


def test_second_run_yields_no_changes(spark):
    observed = [obs("AARSCHOT", 100, "monthly"), obs("BOOM"), obs("DIEST")]
    reference = [("300", "Boom")]
    first = resolve(spark, observed, reference)
    second = resolve(spark, observed, reference, first)
    assert alias_upserts(first, second).count() == 0
    assert alias_changes(first, second).count() == 0


def test_change_rows_only_for_real_changes(spark):
    reference = [("200", "Aarschot"), ("300", "Charleroi")]
    existing = resolve(spark, [obs("AARSCHOT"), obs("BOOM"), obs("CHARLEROI")], reference)

    resolved = resolve(
        spark,
        [
            obs("AARSCHOT", 100, "monthly", day="2026-08-02"),  # reference -> monthly
            obs("BOOM"),                                        # identical
            obs("CHARLEROI", day="2026-07-30"),                 # first_seen only
            obs("DIEST"),                                       # new key
        ],
        reference,
        existing,
    )

    upserts = by_key(alias_upserts(existing, resolved))
    assert set(upserts) == {"key_AARSCHOT", "key_CHARLEROI", "key_DIEST"}
    assert upserts["key_CHARLEROI"].first_seen == dt.date(2026, 7, 30)

    changes = alias_changes(existing, resolved).collect()
    assert len(changes) == 1
    change = changes[0]
    assert change.stop_point_key == "key_AARSCHOT"
    assert (change.old_ptcar_no, change.new_ptcar_no) == (200, 100)
    assert (change.old_status, change.new_status) == ("resolved", "resolved")
    assert (change.old_resolution_source, change.new_resolution_source) == (
        "operational_point_name", "monthly_native",
    )
