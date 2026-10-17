import datetime as dt
from src.rail.quality import blocking_failures, evaluate_checks

def test_evaluate_checks_flags_null_keys(spark):
    df = spark.createDataFrame(
        [("2026-08-08", "1234", "abc"), ("2026-08-08", None, "def")],
        "service_date string, train_no string, stop_point_key string",
    )
    checks = [("not_null_keys", "train_no IS NULL")]
    rows = evaluate_checks(df, "silver.stop_event", checks, dt.datetime.now())
    assert rows[0][3] == 2       # rows_checked
    assert rows[0][4] == 1       # rows_failed
    assert rows[0][6] is False   # passed

def test_evaluate_checks_metric_has_null_verdict(spark):
    df = spark.createDataFrame([(1,), (-1,)], "station_key int")
    checks = [("unresolved_station_key", "station_key = -1")]
    rows = evaluate_checks(df, "gold.fact", checks, dt.datetime.now(), metric=True)
    assert rows[0][4] == 1       # rows_failed
    assert rows[0][5] == 50.0    # pct_failed
    assert rows[0][6] is None    # passed


def test_evaluate_checks_assertion_passes_when_clean(spark):
    df = spark.createDataFrame([(1,), (2,)], "station_key int")
    checks = [("unresolved_station_key", "station_key = -1")]
    rows = evaluate_checks(df, "gold.fact", checks, dt.datetime.now())
    assert rows[0][6] is True    # passed


def test_evaluate_checks_assertion_fails_when_empty(spark):
    df = spark.createDataFrame([], "train_no string")
    checks = [("not_null_keys", "train_no IS NULL")]
    rows = evaluate_checks(df, "silver.stop_event", checks, dt.datetime.now())
    assert rows[0][3:6] == (0, 0, 0.0)
    assert rows[0][6] is False   # passed


def test_evaluate_checks_metric_stays_null_when_empty(spark):
    df = spark.createDataFrame([], "station_key int")
    checks = [("unresolved_station_key", "station_key = -1")]
    rows = evaluate_checks(df, "gold.fact", checks, dt.datetime.now(), metric=True)
    assert rows[0][3:6] == (0, 0, 0.0)
    assert rows[0][6] is None    # passed


def _row(check, rows_checked, rows_failed, passed, table="silver.stop_event"):
    pct = round(100 * rows_failed / rows_checked, 4) if rows_checked else 0.0
    return (dt.datetime.now(), table, check, rows_checked, rows_failed, pct, passed)


def test_blocking_failures_returns_failed_blocking_assertion():
    rows = [_row("not_null_keys", 1000, 1, False), _row("delay_in_range", 1000, 2, False)]
    assert [r[2] for r in blocking_failures(rows)] == ["not_null_keys", "delay_in_range"]


def test_blocking_failures_ignores_null_non_blocking_and_tolerated():
    rows = [
        _row("unresolved_station_key", 1000, 500, None),
        _row("unexpected_source_columns", 1000, 10, False, table="bronze.punctuality_raw"),
        _row("delay_in_range", 1000, 1, False),
    ]
    assert blocking_failures(rows) == []


def test_blocking_failures_blocks_tolerated_check_over_zero_rows():
    assert len(blocking_failures([_row("delay_in_range", 0, 0, False)])) == 1


def test_blocking_failures_clean_run_returns_nothing():
    rows = [_row(check, 1000, 0, True) for check in ("not_null_keys", "delay_in_range")]
    assert blocking_failures(rows) == []
