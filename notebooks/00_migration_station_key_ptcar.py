# Databricks notebook source
# ONE-SHOT MIGRATION -- run once, manually, right after deploying the change
# that keys Gold on PTCAR_NO and before the next scheduled run. Not part of the
# scheduled job.
#
# Prerequisites: silver.station_alias seeded, and fact_stop_event.stop_point_key
# backfilled (00_migration_fact_stop_point_key.py).
#
# - fact_stop_event.station_key: MD5 STRING -> PTCAR_NO INT, -1 when unresolved.
# - dim_station rebuilt keyed on PTCAR_NO, with the -1 member.
# - bronze.station_ref dropped.
#
# Safe to re-run: each table is rewritten only while its station_key is still
# a STRING. Rollback: RESTORE TABLE ... TO VERSION AS OF the versions printed below.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from pyspark.sql.types import StringType

from src.rail.config import BRONZE, GOLD_DIM_STATION, GOLD_FACT, SILVER_STATION_ALIAS
from src.rail.gold_maintenance import (
    DIM_STATION_COMMENT, DIM_STATION_SCHEMA,
    refresh_station_dimension, refresh_station_enrichment, repair_fact_station_keys,
)


def station_key_is_string(table: str) -> bool:
    return isinstance(spark.table(table).schema["station_key"].dataType, StringType)


def fact_totals():
    return spark.sql(f"""
        SELECT count(*) AS rows, sum(stop_events) AS stop_events, sum(punctual_arrivals) AS punctual
        FROM {GOLD_FACT}
    """).first()

# COMMAND ----------

if spark.table(SILVER_STATION_ALIAS).isEmpty():
    raise RuntimeError(f"{SILVER_STATION_ALIAS} is empty: run 00_migration_silver_station_alias_seed.py first.")

if "stop_point_key" not in spark.table(GOLD_FACT).columns or not spark.table(GOLD_FACT).filter(
    "stop_point_key IS NULL"
).isEmpty():
    raise RuntimeError("fact_stop_event.stop_point_key is missing or has NULLs: run 00_migration_fact_stop_point_key.py first.")

for table in [GOLD_FACT, GOLD_DIM_STATION]:
    version = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]
    print(f"rollback point: RESTORE TABLE {table} TO VERSION AS OF {version}")

before = fact_totals()
print(f"before: {before}")

# COMMAND ----------

if station_key_is_string(GOLD_FACT):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_FACT}
    USING DELTA
    CLUSTER BY (date_key, station_key)
    COMMENT 'Grain: one train passing one measuring point on one service date. Additive measures: stop_events, punctual_arrivals.'
    AS
    SELECT
        f.date_key,
        CAST(coalesce(a.ptcar_no, -1) AS INT) AS station_key,
        f.stop_point_key,
        f.relation_key,
        f.train_no,
        f.planned_hour,
        f.delay_arr_s,
        f.delay_dep_s,
        f.dwell_delta_s,
        f.punctual_arrivals,
        f.stop_events
    FROM {GOLD_FACT} f
    LEFT JOIN {SILVER_STATION_ALIAS} a
      ON a.stop_point_key = f.stop_point_key AND a.status = 'resolved'
    """)
    print("fact_stop_event rewritten with INT station_key")
else:
    print("fact_stop_event already keyed on INT, skipping rewrite")

if station_key_is_string(GOLD_DIM_STATION):
    spark.sql(
        f"CREATE OR REPLACE TABLE {GOLD_DIM_STATION} ({DIM_STATION_SCHEMA}) "
        f"USING DELTA COMMENT '{DIM_STATION_COMMENT}'"
    )
    print("dim_station recreated with INT station_key")
else:
    print("dim_station already keyed on INT, skipping recreate")

refresh_station_dimension(spark)
repair_fact_station_keys(spark)
refresh_station_enrichment(spark)

spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.station_ref")

# COMMAND ----------

# Expected: totals equal to `before`, no orphans, and the -1 share in line with
# the unresolved aliases.
after = fact_totals()
print(f"after:  {after}")
if (after["rows"], after["stop_events"], after["punctual"]) != (
    before["rows"], before["stop_events"], before["punctual"]
):
    raise RuntimeError(f"fact totals changed: before {before}, after {after}")

display(spark.sql(f"""
SELECT
    (SELECT count(*) FROM {GOLD_DIM_STATION})                      AS dim_rows,
    (SELECT count(*) FROM {GOLD_FACT} WHERE station_key = -1)      AS fact_unresolved_rows,
    (SELECT count(*) FROM {GOLD_FACT} f
       LEFT ANTI JOIN {GOLD_DIM_STATION} d USING (station_key))    AS fact_orphan_rows
"""))
