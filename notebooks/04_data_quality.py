# Executes Data Quality (DQ) assertions across Bronze and Silver layers and logs results

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import datetime as dt
from src.rail.config import BRONZE_RAW, SILVER_STOP, DQ_RESULTS

# Data quality rules evaluating failure conditions (rows_failed == 0 indicates PASS)
SILVER_CHECKS = [
    ("not_null_keys",       "service_date IS NULL OR train_no IS NULL OR stop_point_key IS NULL"),
    ("delay_in_range",      "delay_arr_s < -3600 OR delay_arr_s > 86400"),
    ("arrival_without_plan","real_arr_ts IS NOT NULL AND planned_arr_ts IS NULL"),
    ("unknown_station",     "stop_point_name IS NULL OR stop_point_name = ''"),    
    ("missing_ptcar_no",    "ptcar_no IS NULL"),
]

BRONZE_CHECKS = [    
    ("unexpected_source_columns", "_rescued_data IS NOT NULL"),
]

# Evaluate assertions against target tables
run_ts = dt.datetime.now()
rows = []

for table, checks in [(SILVER_STOP, SILVER_CHECKS), (BRONZE_RAW, BRONZE_CHECKS)]:
    df = spark.table(table)
    total = df.count()
    for name, failing_expr in checks:
        failed = df.filter(failing_expr).count()
        rows.append(
            (
                run_ts,
                table,
                name,
                total,
                failed,
                round(100 * failed / total, 4) if total else 0.0,
                failed == 0,
            )
        )

# Append execution metrics to operational tracking table       
(
    spark.createDataFrame(
        rows,
        "run_ts timestamp, table_name string, check_name string, rows_checked long, "
        "rows_failed long, pct_failed double, passed boolean",
    )
    .write.mode("append")
    .saveAsTable(DQ_RESULTS)
)
display(spark.table(DQ_RESULTS).orderBy("run_ts", ascending=False).limit(20))
