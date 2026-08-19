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