# Executes Data Quality (DQ) assertions across Bronze and Silver layers and logs results

import os
import sys
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import datetime as dt
from src.rail.config import BRONZE_LOOKBACK_DAYS, BRONZE_RAW, SILVER_STOP, DQ_RESULTS
from src.rail.quality import (
    SILVER_CHECKS, BRONZE_CHECKS, DQ_RESULT_SCHEMA, evaluate_checks, blocking_failures,
)
from src.rail.transforms import within_lookback

service_date = dbutils.jobs.taskValues.get(
    taskKey="silver_transform",
    key="service_date",
    default=None,
    debugValue=str(dt.date.today()),
)
if service_date is None:
    raise ValueError("No service_date received from silver_transform task value.")

# Evaluate assertions against target tables
run_ts = dt.datetime.now()
rows = []
for table, checks in [(SILVER_STOP, SILVER_CHECKS), (BRONZE_RAW, BRONZE_CHECKS)]:
    df = spark.table(table)
    if table == SILVER_STOP:
        df = df.filter(f"service_date = '{service_date}'")
    else:
        df = within_lookback(df, BRONZE_LOOKBACK_DAYS)
    rows.extend(evaluate_checks(df, table, checks, run_ts))

# Append execution metrics to operational tracking table
(
    spark.createDataFrame(rows, DQ_RESULT_SCHEMA)
    .write.mode("append")
    .saveAsTable(DQ_RESULTS)
)
display(spark.table(DQ_RESULTS).orderBy("run_ts", ascending=False).limit(20))

failures = blocking_failures(rows)
if failures:
    raise RuntimeError(
        "Blocking DQ checks failed: " + "; ".join(
            f"{table}.{check} rows_failed={failed} pct_failed={pct}"
            for _, table, check, _, failed, pct, _ in failures
        )
    )