# Cleans and conforms bronze data into validated, structured silver tables

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.rail.config import (
    BRONZE_LOOKBACK_DAYS, BRONZE_RAW, BRONZE_STATION_REF, SILVER_STOP, PUNCTUAL_THRESHOLD_S,
)
from src.rail.transforms import (
    deduplicate_stop_events, has_processing_gap, typed_stop_events, within_lookback,
)

# Initialize target Silver Delta table with Liquid Clustering
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_STOP} (
    service_date        DATE,
    train_no            INT,
    stop_point_key      STRING,
    stop_point_name_key STRING,
    stop_point_name     STRING,
    ptcar_no            INT,
    relation            STRING,
    relation_direction  STRING,
    operator            STRING,
    line_no_dep         STRING,
    line_no_arr         STRING,
    planned_arr_ts      TIMESTAMP,
    real_arr_ts         TIMESTAMP,
    planned_dep_ts      TIMESTAMP,
    real_dep_ts         TIMESTAMP,
    delay_arr_s         INT,
    delay_dep_s         INT,
    delay_arr_min       DOUBLE,
    dwell_delta_s       INT,
    planned_hour        INT,
    is_punctual_arr     BOOLEAN,
    _ingested_at        TIMESTAMP,
    source_feed          STRING
)
USING DELTA
CLUSTER BY (service_date, stop_point_key)
""")

spark.sql(f"""
COMMENT ON TABLE {SILVER_STOP} IS
  'Grain: one train passing one measuring point per service date. Delays in seconds.'
""")

# Transform raw stop events and enrich ptcar_no from the station reference.
bronze_window = within_lookback(spark.table(BRONZE_RAW), BRONZE_LOOKBACK_DAYS)

# Fail before the MERGE if Bronze rows ingested since the last Silver run fall
# outside the window, or the window is empty.
window_start = spark.range(1).select(F.date_sub(F.current_date(), BRONZE_LOOKBACK_DAYS)).first()[0]
silver_daily = spark.table(SILVER_STOP).filter("source_feed = 'daily'")
# Pruned scan first; the full scan only runs when it finds nothing.
watermark = (
    silver_daily.filter(F.col("service_date") >= F.date_sub(F.lit(window_start), 1))
    .agg(F.max("_ingested_at"))
    .first()[0]
)
if watermark is None:
    watermark = silver_daily.agg(F.max("_ingested_at")).first()[0]

window_empty = bronze_window.isEmpty()
if window_empty or has_processing_gap(watermark, window_start):
    raise RuntimeError(
        f"Silver watermark {watermark} vs Bronze window start {window_start} "
        f"({BRONZE_LOOKBACK_DAYS}-day lookback): "
        + ("the window holds no Bronze rows. " if window_empty else "the watermark is before the window. ")
        + "Rows ingested in the gap are not processed by the daily path; "
        "widen the lookback for this run or use the monthly backfill."
    )

d1_typed = typed_stop_events(bronze_window, PUNCTUAL_THRESHOLD_S)

station_ref = spark.table(BRONZE_STATION_REF).select(
    "stop_point_name_key",
    F.col("ptcar_no").alias("ref_ptcar_no"),
)

enriched = (
    d1_typed.join(station_ref, on="stop_point_name_key", how="left")
    .withColumn("ptcar_no", F.coalesce("ptcar_no", "ref_ptcar_no"))
    .drop("ref_ptcar_no")
)

silver = deduplicate_stop_events(enriched)

min_service_date, max_service_date = silver.agg(
    F.min("service_date"), F.max("service_date")
).first()

if max_service_date is None:
    raise RuntimeError(
        "Every Bronze row in the window was dropped by the key or station-name filters; "
        "nothing to merge."
    )

# Upsert processed events into Silver table using natural key.
# The service_date bound lets Delta prune target files outside the window.
(
    DeltaTable.forName(spark, SILVER_STOP)
    .alias("t")
    .merge(
        silver.alias("s"),
        f"t.service_date >= DATE'{min_service_date}' AND "
        "t.service_date  = s.service_date AND "
        "t.train_no      = s.train_no     AND "
        "t.stop_point_key = s.stop_point_key",
    )
    .whenMatchedUpdateAll(
        condition="(t.source_feed = 'daily' OR s.source_feed = 'monthly') "
        "AND s._ingested_at > t._ingested_at"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

try:
    dbutils.jobs.taskValues.set(key="service_date", value=str(max_service_date))
except Exception:
    print("Not running as a job task — skipping task value publish.")

print(f"{spark.table(SILVER_STOP).count():,} rows in {SILVER_STOP}")
