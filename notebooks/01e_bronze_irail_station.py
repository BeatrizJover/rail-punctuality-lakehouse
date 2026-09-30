# Databricks notebook source
# Downloads the iRail NMBS/SNCB station list and lands it in Bronze.
# Source: https://github.com/iRail/stations, stations.csv, master branch.
# License: CC0-1.0 (composer.json + README badge).
# Ad-hoc: run by hand when the crosswalk to operational_point needs a refresh.
#
# Overwrite, not MERGE: each publication is a full snapshot. Past snapshots
# stay on disk under their retrieval date, so overwriting the table loses
# nothing.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import datetime as dt

import requests
from pyspark.sql import functions as F

from src.rail.config import BRONZE_IRAIL_STATION, IRAIL_STATIONS_URL, OPERATIONAL_POINT_LANDING
from src.rail.ingest import validate_irail_station_export

# COMMAND ----------

# Fetch the station list and stage it to the landing volume
retrieved_at = dt.datetime.now(dt.timezone.utc).date()

dbutils.fs.mkdirs(OPERATIONAL_POINT_LANDING)

resp = requests.get(IRAIL_STATIONS_URL, timeout=600)
resp.raise_for_status()

# Fail loud on an empty or malformed payload instead of landing it
row_count = validate_irail_station_export(resp.content)

target = f"{OPERATIONAL_POINT_LANDING}/irail_station_{retrieved_at:%Y-%m-%d}.csv"
with open(target, "wb") as fh:
    fh.write(resp.content)

print(f"validated {row_count:,} rows -> landed {target}")

# COMMAND ----------

# Every published column is kept as text, with its original hyphenated name
raw = (
    spark.read
    .option("header", True)
    .option("sep", ",")
    .option("quote", '"')
    .option("multiLine", True)
    .csv(target)
)

batch = (
    raw
    .withColumn("_source_file", F.lit(target))
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    batch.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_IRAIL_STATION)
)

spark.sql(f"""
COMMENT ON TABLE {BRONZE_IRAIL_STATION} IS
  'Full snapshot of the iRail NMBS/SNCB station list
   (github.com/iRail/stations, stations.csv), CC0-1.0. Overwritten on each
   ingest, not merged, since each publication is a full republication.
   Past snapshots live under landing/reference/.'
""")

# COMMAND ----------

result = spark.table(BRONZE_IRAIL_STATION)
print(f"{result.count():,} rows in {BRONZE_IRAIL_STATION}")
print(f"{result.select('taf-tap-code').distinct().count():,} distinct taf-tap-code")
result.groupBy("country-code").count().orderBy(F.desc("count")).show(50, truncate=False)
