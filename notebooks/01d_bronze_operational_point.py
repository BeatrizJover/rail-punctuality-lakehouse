# Databricks notebook source
# Downloads the Infrabel operational points reference and lands it in Bronze.
# Ad-hoc, quarterly: run by hand when Infrabel republishes the dataset.
#
# Write semantics: overwrite, not MERGE. `01b` merges because it accumulates a
# crosswalk across successive monthly exports. This dataset is different: each
# publication is a full, authoritative snapshot of the network's operational
# points, including retirements. A MERGE would keep retired points forever --
# the stale-mapping-table failure this design exists to avoid. History lives at
# the file level in the landing volume instead: each snapshot is landed under
# its retrieval date, so overwriting the table loses nothing.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import datetime as dt

import requests
from pyspark.sql import functions as F

from src.rail.config import (
    BRONZE_OPERATIONAL_POINT, CSV_SEP, DATASET_OPERATIONAL_POINT,
    ODS_BASE, OPERATIONAL_POINT_LANDING,
)
from src.rail.ingest import validate_operational_point_export

# COMMAND ----------

# Fetch the full dataset export and stage it to the landing volume
retrieved_at = dt.datetime.now(dt.timezone.utc).date()

dbutils.fs.mkdirs(OPERATIONAL_POINT_LANDING)

resp = requests.get(
    f"{ODS_BASE}/{DATASET_OPERATIONAL_POINT}/exports/csv",
    params={"delimiter": CSV_SEP},
    timeout=600,
)
resp.raise_for_status()

# Fail loud on an empty payload or a missing required column, rather than
# silently writing a table with fields the pipeline depends on gone missing
row_count = validate_operational_point_export(resp.content, CSV_SEP)

target = f"{OPERATIONAL_POINT_LANDING}/operational_point_{retrieved_at:%Y-%m-%d}.csv"
with open(target, "wb") as fh:
    fh.write(resp.content)

print(f"validated {row_count:,} rows -> landed {target}")

# COMMAND ----------

# Read the landed snapshot and overwrite Bronze with it. Every published column
# is kept as text, unmodified: no casting, no derived columns, no pruning. Names
# contain the field separator in some rows, so quoting is honoured explicitly.
raw = (
    spark.read
    .option("header", True)
    .option("sep", CSV_SEP)
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
    .saveAsTable(BRONZE_OPERATIONAL_POINT)
)

spark.sql(f"""
COMMENT ON TABLE {BRONZE_OPERATIONAL_POINT} IS
  'Full quarterly snapshot of the Infrabel operational points reference
   (operationele-punten-van-het-netwerk), CC0, republished by Infrabel every
   quarter. Every published column is kept as text, exactly as received.
   Overwritten on each ingest: this is a full authoritative republication,
   not an accumulating crosswalk, so a MERGE would retain retired points
   forever. History of past snapshots lives at the file level under
   landing/reference/.'
""")

# COMMAND ----------

result = spark.table(BRONZE_OPERATIONAL_POINT)
print(f"{result.count():,} rows in {BRONZE_OPERATIONAL_POINT}")
print(f"{result.select('ptcarid').distinct().count():,} distinct ptcarid")
result.groupBy("class_en").count().orderBy(F.desc("count")).show(50, truncate=False)
