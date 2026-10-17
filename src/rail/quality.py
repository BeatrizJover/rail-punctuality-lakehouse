"""Data quality rule definitions and evaluation logic for Bronze/Silver checks."""
import datetime as dt
from pyspark.sql import DataFrame

# Data quality rules evaluating failure conditions (rows_failed == 0 indicates PASS)
SILVER_CHECKS = [
    ("not_null_keys",        "service_date IS NULL OR train_no IS NULL OR stop_point_key IS NULL"),
    ("delay_in_range",       "delay_arr_s < -3600 OR delay_arr_s > 86400"),
    ("arrival_without_plan", "real_arr_ts IS NOT NULL AND planned_arr_ts IS NULL"),
    ("unknown_station",      "stop_point_name IS NULL OR stop_point_name = ''")    
]

# Share of the run's fact rows not yet resolved to a PTCAR_NO. A metric, not an alert.
GOLD_COVERAGE = [
    ("unresolved_station_key", "station_key = -1"),
]

BRONZE_CHECKS = [
    ("unexpected_source_columns", "_rescued_data IS NOT NULL"),
]

# Assertions that fail the job, with the pct_failed they tolerate.
BLOCKING_CHECKS = {
    "not_null_keys":        0.0,
    "delay_in_range":       0.1,
    "arrival_without_plan": 0.0,
    "unknown_station":      0.0,
}

DQ_RESULT_SCHEMA = (
    "run_ts timestamp, table_name string, check_name string, rows_checked long, "
    "rows_failed long, pct_failed double, passed boolean"
)


def evaluate_checks(
    df: DataFrame,
    table_name: str,
    checks: list[tuple[str, str]],
    run_ts: dt.datetime,
    metric: bool = False,
) -> list[tuple]:
    """Run (check_name, failing_condition) rules against a DataFrame.

    `failing_expr` describes rows that VIOLATE the rule — rows_failed == 0 means PASS.
    With `metric=True`, `passed` is NULL: the rule is measured, not asserted.
    An assertion over 0 rows fails: no data is not a healthy day.
    Returns one result row per check, matching DQ_RESULT_SCHEMA.
    """
    total = df.count()
    rows = []
    for name, failing_expr in checks:
        failed = df.filter(failing_expr).count()
        rows.append(
            (
                run_ts,
                table_name,
                name,
                total,
                failed,
                round(100 * failed / total, 4) if total else 0.0,
                None if metric else (total > 0 and failed == 0),
            )
        )
    return rows


def blocking_failures(rows: list[tuple]) -> list[tuple]:
    """Return the failed blocking assertions above their tolerance.

    Metrics (passed = NULL) and non-blocking rules never block. A run over 0 rows always does.
    """
    return [
        row for row in rows
        if row[6] is False
        and row[2] in BLOCKING_CHECKS
        and (row[3] == 0 or 100 * row[4] / row[3] > BLOCKING_CHECKS[row[2]])
    ]
