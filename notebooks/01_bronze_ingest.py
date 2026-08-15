# Downloads the Infrabel D-1 export and ingests the landing volume into bronze

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import datetime as dt
import requests
from pyspark.sql import functions as F

from src.rail.config import (
    LANDING, CHECKPOINTS, BRONZE_RAW,
    ODS_BASE, DATASET_DAILY, CSV_SEP,
)

# Fetch daily export (D-1) and stage to landing volume
run_date = dt.date.today() - dt.timedelta(days=1)
target = f"{LANDING}/d1/{run_date:%Y-%m-%d}.csv"

dbutils.fs.mkdirs(f"{LANDING}/d1")

resp = requests.get(
    f"{ODS_BASE}/{DATASET_DAILY}/exports/csv",
    params={"delimiter": CSV_SEP},
    timeout=600,
)
resp.raise_for_status()

with open(target, "wb") as fh:
    fh.write(resp.content)

print(f"landed {len(resp.content):,} bytes -> {target}")

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