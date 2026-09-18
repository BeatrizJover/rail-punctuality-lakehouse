# Downloads the Infrabel D-1 export and ingests the landing volume into bronze

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import datetime as dt
from zoneinfo import ZoneInfo

import requests
from pyspark.sql import functions as F

from src.rail.config import (
    LANDING, CHECKPOINTS, BRONZE_RAW,
    ODS_BASE, DATASET_DAILY, CSV_SEP,
)
from src.rail.ingest import validate_d1_export

# Fetch daily export (D-1) and stage to landing volume
service_date = dt.datetime.now(ZoneInfo("Europe/Brussels")).date() - dt.timedelta(days=1)

dbutils.fs.mkdirs(f"{LANDING}/d1")

resp = requests.get(
    f"{ODS_BASE}/{DATASET_DAILY}/exports/csv",
    params={"delimiter": CSV_SEP},
    timeout=600,
)
resp.raise_for_status()

# Raise on a stale export so the task retry policy waits for the upstream refresh
validated_date = validate_d1_export(resp.content, CSV_SEP, service_date)

fetched_at = dt.datetime.now(dt.timezone.utc)
target = f"{LANDING}/d1/{service_date:%Y-%m-%d}_{fetched_at:%Y%m%dT%H%M%SZ}.csv"

with open(target, "wb") as fh:
    fh.write(resp.content)

print(f"validated service date {validated_date} -> landed {target}")

# Incrementally ingest landed CSVs into Bronze Delta table via Auto Loader
stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/bronze_schema")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("header", "true")
    .option("sep", CSV_SEP)
    .load(f"{LANDING}/d1")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

query = (
    stream.writeStream.option("checkpointLocation", f"{CHECKPOINTS}/bronze_stream")
    .trigger(availableNow=True)
    .toTable(BRONZE_RAW)
)

query.awaitTermination()
print(f"{spark.table(BRONZE_RAW).count():,} rows in {BRONZE_RAW}")