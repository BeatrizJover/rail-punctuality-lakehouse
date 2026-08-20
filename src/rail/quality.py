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

SILVER_COVERAGE = [
    ("ptcar_no_coverage", "ptcar_no IS NULL"),
]

BRONZE_CHECKS = [
    ("unexpected_source_columns", "_rescued_data IS NOT NULL"),
]

DQ_RESULT_SCHEMA = (
    "run_ts timestamp, table_name string, check_name string, rows_checked long, "
    "rows_failed long, pct_failed double, passed boolean"
)


def evaluate_checks(
    df: DataFrame, table_name: str, checks: list[tuple[str, str]], run_ts: dt.datetime
) -> list[tuple]:
    """Run (check_name, failing_condition) rules against a DataFrame.

    `failing_expr` describes rows that VIOLATE the rule — rows_failed == 0 means PASS.
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
                failed == 0,
            )
        )
    return rows