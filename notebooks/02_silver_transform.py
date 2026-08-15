# Cleans and conforms bronze data into validated, structured silver tables

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.rail.config import BRONZE_RAW, BRONZE_STATION_REF, SILVER_STOP, PUNCTUAL_THRESHOLD_S
from src.rail.transforms import typed_stop_events, deduplicate_stop_events

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
    _ingested_at        TIMESTAMP
)
USING DELTA
CLUSTER BY (service_date, stop_point_key)
""")

spark.sql(f"""
COMMENT ON TABLE {SILVER_STOP} IS
  'Grain: one train passing one measuring point per service date. Delays in seconds.'
""")

# Transform raw stop events and left-join station reference metadata
d1_typed = typed_stop_events(spark.table(BRONZE_RAW), PUNCTUAL_THRESHOLD_S)

station_ref = spark.table(BRONZE_STATION_REF).select("stop_point_name_key", "ptcar_no")

silver = deduplicate_stop_events(
    d1_typed.join(station_ref, on="stop_point_name_key", how="left")
)

# Upsert processed events into Silver table using natural key
(
    DeltaTable.forName(spark, SILVER_STOP)
    .alias("t")
    .merge(
        silver.alias("s"),
        "t.service_date  = s.service_date AND "
        "t.train_no      = s.train_no     AND "
        "t.stop_point_key = s.stop_point_key",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"{spark.table(SILVER_STOP).count():,} rows in {SILVER_STOP}")