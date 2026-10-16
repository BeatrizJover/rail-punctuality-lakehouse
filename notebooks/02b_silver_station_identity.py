# Databricks notebook source
# Resolves the run's stop points to PTCAR_NO in silver.station_alias.
# Shadow mode: nothing downstream reads the table yet.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import datetime as dt

from pyspark.sql import functions as F

from src.rail.config import BRONZE_LOOKBACK_DAYS, SILVER_STOP
from src.rail.silver_maintenance import refresh_station_aliases

# COMMAND ----------

service_date = dbutils.jobs.taskValues.get(
    taskKey="silver_transform",
    key="service_date",
    default=None,
    debugValue=str(dt.date.today()),
)
if service_date is None:
    raise ValueError("No service_date received from silver_transform task value.")

# The run's batch spans the lookback window, not just its latest service_date.
run_date = F.to_date(F.lit(service_date))
refresh_station_aliases(
    spark,
    spark.table(SILVER_STOP).filter(
        F.col("service_date").between(F.date_sub(run_date, BRONZE_LOOKBACK_DAYS), run_date)
    ),
)
