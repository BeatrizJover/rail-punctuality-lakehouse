# Unit tests for station identity resolution.

import datetime as dt

from src.rail.identity import (
    STATION_ALIAS_SCHEMA, UNRESOLVED_STATION_KEY,
    alias_changes, alias_upserts, build_station_dimension, resolve_station_aliases,
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


def test_two_ptcar_no_in_the_same_month_is_ambiguous(spark):
    row = resolve(
        spark,
        [obs("MECHELEN", 100, "monthly"), obs("MECHELEN", 101, "monthly", day="2026-08-02")],
        [("100", "Mechelen")],
    ).first()
    assert (row.ptcar_no, row.status, row.resolution_source) == (None, "ambiguous", "monthly_native")
    assert row.evidence_month == dt.date(2026, 8, 1)


def test_batch_with_two_months_resolves_to_the_latest(spark):
    """A renumbering inside the batch (Jan-Jun = A, Jul-Dec = B) is not ambiguity."""
    row = resolve(
        spark,
        [obs("MECHELEN", 100, "monthly", day="2026-03-10"), obs("MECHELEN", 101, "monthly", day="2026-08-10")],
        [],
    ).first()
    assert (row.ptcar_no, row.status) == (101, "resolved")
    assert row.evidence_month == dt.date(2026, 8, 1)


def test_older_month_does_not_replace_newer_evidence(spark):
    stored = resolve(spark, [obs("MECHELEN", 101, "monthly", day="2026-08-10")], [])
    row = resolve(spark, [obs("MECHELEN", 100, "monthly", day="2026-03-10")], [], stored).first()
    assert (row.ptcar_no, row.evidence_month) == (101, dt.date(2026, 8, 1))


def test_newer_month_replaces_stored_evidence_and_resolves_ambiguity(spark):
    stored = resolve(
        spark,
        [obs("MECHELEN", 100, "monthly"), obs("MECHELEN", 101, "monthly", day="2026-08-02")],
        [],
    )
    row = resolve(spark, [obs("MECHELEN", 102, "monthly", day="2026-09-05")], [], stored).first()
    assert (row.ptcar_no, row.status, row.evidence_month) == (102, "resolved", dt.date(2026, 9, 1))


def test_same_month_rerun_with_same_ptcar_no_changes_nothing(spark):
    observed = [obs("MECHELEN", 100, "monthly")]
    stored = resolve(spark, observed, [])
    again = resolve(spark, observed, [], stored)
    assert alias_upserts(stored, again).count() == 0
    assert alias_changes(stored, again).count() == 0


def test_same_month_rerun_with_another_ptcar_no_replaces_and_logs(spark):
    stored = resolve(spark, [obs("MECHELEN", 100, "monthly")], [])
    again = resolve(spark, [obs("MECHELEN", 101, "monthly", day="2026-08-15")], [], stored)
    assert alias_upserts(stored, again).first().ptcar_no == 101
    change = alias_changes(stored, again).first()
    assert (change.old_ptcar_no, change.new_ptcar_no) == (100, 101)


def test_reference_cannot_override_monthly_alias(spark):
    first = resolve(spark, [obs("MECHELEN", 100, "monthly")], [])
    row = resolve(spark, [obs("MECHELEN", day="2026-08-02")], [("200", "Mechelen")], first).first()
    assert (row.ptcar_no, row.status, row.resolution_source) == (100, "resolved", "monthly_native")


def test_reference_alias_is_kept_when_the_name_leaves_the_reference(spark):
    stored = resolve(spark, [obs("MECHELEN")], [("200", "Mechelen")])
    row = resolve(spark, [obs("MECHELEN", day="2026-08-02")], [("300", "Leuven")], stored).first()
    assert (row.ptcar_no, row.status, row.resolution_source) == (200, "resolved", "operational_point_name")


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


def alias(key, name, ptcar_no, status, first_seen, last_seen):
    return (
        key, name, name, ptcar_no, status, None, None,
        dt.date.fromisoformat(first_seen), dt.date.fromisoformat(last_seen), None,
    )


def test_station_dimension_unites_renamed_aliases_under_one_ptcar_no(spark):
    aliases = spark.createDataFrame(
        [
            alias("k1", "OLD NAME", 100, "resolved", "2024-01-01", "2025-06-30"),
            alias("k2", "NEW NAME", 100, "resolved", "2025-07-01", "2026-08-01"),
            alias("k3", "OTHER", 200, "resolved", "2024-01-01", "2026-08-01"),
            alias("k4", "LOST", None, "unresolved", "2023-01-01", "2026-08-01"),
            alias("k5", "BOTH", None, "ambiguous", "2023-01-01", "2026-08-01"),
        ],
        STATION_ALIAS_SCHEMA,
    )
    reference = spark.createDataFrame([("200", "Official Other ")], REFERENCE_SCHEMA)

    rows = {r.station_key: r for r in build_station_dimension(aliases, reference).collect()}

    assert set(rows) == {100, 200, UNRESOLVED_STATION_KEY}
    assert rows[100].station_name == "NEW NAME"          # latest alias, not in reference
    assert (rows[100].first_seen, rows[100].last_seen) == (dt.date(2024, 1, 1), dt.date(2026, 8, 1))
    assert rows[200].station_name == "Official Other"    # official name wins
    assert rows[UNRESOLVED_STATION_KEY].ptcar_no is None
