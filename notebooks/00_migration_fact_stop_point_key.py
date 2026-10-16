# Databricks notebook source
# ONE-SHOT MIGRATION -- run once, manually, with the daily job paused, in the
# same window as the deploy that moves the fact MERGE onto stop_point_key.
# Not part of the scheduled job.
#
# Adds stop_point_key to gold.fact_stop_event and backfills it from station_key,
# which still holds the same MD5 at this point. Safe to re-run: the column is
# added only if missing and only NULL values are filled.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.rail.config import GOLD

GOLD_FACT = f"{GOLD}.fact_stop_event"

# COMMAND ----------

if "stop_point_key" not in spark.table(GOLD_FACT).columns:
    spark.sql(f"ALTER TABLE {GOLD_FACT} ADD COLUMNS (stop_point_key STRING AFTER station_key)")
    print("added column: stop_point_key")
else:
    print("column already present, skipping ALTER TABLE")

spark.sql(f"UPDATE {GOLD_FACT} SET stop_point_key = station_key WHERE stop_point_key IS NULL")

# COMMAND ----------

# Expected: no NULLs, and every row's stop_point_key equal to its station_key.
display(spark.sql(f"""
SELECT
    count(*)                                     AS rows,
    count_if(stop_point_key IS NULL)             AS null_stop_point_key,
    count_if(stop_point_key <> station_key)      AS mismatched
FROM {GOLD_FACT}
"""))
