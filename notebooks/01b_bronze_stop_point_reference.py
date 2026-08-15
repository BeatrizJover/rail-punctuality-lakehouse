import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.rail.config import BRONZE_STATION_REF
from src.rail.transforms import normalize_station_name

# Target execution parameters
YEAR = "2026"
MONTH = "07"  # zero-padded

SOURCE_PATH = (
    f"/Volumes/rail_punctuality/bronze/landing/monthly/"
    f"Data_raw_punctuality_{YEAR}{MONTH}.csv"
)

# Initialize target Delta table schema
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_STATION_REF} (
    stop_point_name_key STRING,
    ptcar_no             INT,
    stop_point_name       STRING,
    source_year_month      STRING,
    _ingested_at             TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
COMMENT ON TABLE {BRONZE_STATION_REF} IS
  'Station name to PTCAR_NO reference crosswalk derived from monthly rail exports.'
""")

# Extract raw data and deduplicate to one record per station key
raw = spark.read.option("header", True).csv(SOURCE_PATH)

name_key = normalize_station_name("PTCAR_LG_NM_NL")

station_batch = (
    raw
    .select(
        name_key.alias("stop_point_name_key"),
        F.col("PTCAR_NO").cast("int").alias("ptcar_no"),
        F.trim(F.col("PTCAR_LG_NM_NL")).alias("stop_point_name"),
    )
    .filter(
        F.col("stop_point_name_key").isNotNull()
        & F.col("ptcar_no").isNotNull()
    )
    .groupBy("stop_point_name_key")
    .agg(
        F.first("ptcar_no", ignorenulls=True).alias("ptcar_no"),
        F.first("stop_point_name", ignorenulls=True).alias("stop_point_name"),
    )
    .withColumn("source_year_month", F.lit(f"{YEAR}{MONTH}"))
    .withColumn("_ingested_at", F.current_timestamp())
)

# Idempotent upsert into target Delta table
(
    DeltaTable.forName(spark, BRONZE_STATION_REF)
    .alias("t")
    .merge(
        station_batch.alias("s"),
        "t.stop_point_name_key = s.stop_point_name_key",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"{spark.table(BRONZE_STATION_REF).count():,} known stations in {BRONZE_STATION_REF}")