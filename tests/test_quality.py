import datetime as dt
from src.rail.quality import evaluate_checks

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
