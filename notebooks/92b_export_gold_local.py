# Databricks notebook source
"""
Publishes the Gold layer as date-partitioned Parquet to a Unity Catalog Volume,
using the exact layout the Blob export produces. This is the same contract with a
different transport: when Blob credentials are available, only `EXPORT_ROOT`
changes and the consumer is unaffected.

    {root}/gold/fact_stop_event/date_key=YYYY-MM-DD/*.parquet
    {root}/gold/dim_date/*.parquet
    {root}/gold/dim_station/*.parquet
    {root}/gold/dim_relation/*.parquet
    {root}/gold/_manifest/coverage.json

The manifest is written by the producer, never assembled by the consumer: coverage
is a property of what was published, and deriving it downstream would let the two
drift apart.

Prerequisite (run once):

    CREATE VOLUME IF NOT EXISTS rail_punctuality.gold.export;

Download with the Databricks CLI:

    databricks fs cp -r \
      dbfs:/Volumes/rail_punctuality/gold/export/gold ./gold

Idempotency: the fact is written with dynamic partition overwrite, so re-exporting
a range rewrites only the affected date partitions. Dimensions are overwritten whole.
"""

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import datetime as dt
import json
import re

from pyspark.sql import functions as F

from src.rail.config import GOLD

# Gold table identifiers, derived from the catalog schema declared in config.
GOLD_FACT = f"{GOLD}.fact_stop_event"
GOLD_DIM_DATE = f"{GOLD}.dim_date"
GOLD_DIM_STATION = f"{GOLD}.dim_station"
GOLD_DIM_RELATION = f"{GOLD}.dim_relation"

# Published export layout — the RAG loader mirrors this structure verbatim.
EXPORT_PREFIX = "gold"
DIMENSIONS = [
    (GOLD_DIM_DATE, "dim_date"),
    (GOLD_DIM_STATION, "dim_station"),
    (GOLD_DIM_RELATION, "dim_relation"),
]

# COMMAND ----------

dbutils.widgets.text("start_year", "2024", "Start year (YYYY)")
dbutils.widgets.text("end_year", "2026", "End year (YYYY)")
dbutils.widgets.text(
    "export_root",
    "/Volumes/rail_punctuality/gold/export",
    "Export root (Volume path)",
)

start_year = dbutils.widgets.get("start_year").strip()
end_year = dbutils.widgets.get("end_year").strip()
export_root = dbutils.widgets.get("export_root").strip().rstrip("/")

for name, value in [("start_year", start_year), ("end_year", end_year)]:
    if not re.fullmatch(r"\d{4}", value):
        raise ValueError(f"Invalid {name} widget value: {value!r}")
if start_year > end_year:
    raise ValueError(f"start_year {start_year} is after end_year {end_year}")
if not export_root:
    raise ValueError("export_root widget is required")

range_start = f"{start_year}-01-01"
range_end = f"{end_year}-12-31"
base = f"{export_root}/{EXPORT_PREFIX}"

print(f"Exporting Gold for service dates {range_start} .. {range_end}")
print(f"Target: {base}")

# COMMAND ----------

# Fail fast on an empty window: an export that silently publishes nothing would
# hand the consumer a valid-looking but empty contract.
fact = spark.table(GOLD_FACT).filter(F.col("date_key").between(range_start, range_end))

if fact.isEmpty():
    raise ValueError(
        f"No rows in {GOLD_FACT} between {range_start} and {range_end} — nothing to publish."
    )

# COMMAND ----------

# Fact: one file per date partition, dynamic overwrite so re-running a range
# replaces only its partitions and never the whole dataset.
(
    fact.repartition("date_key")
    .write.mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("date_key")
    .parquet(f"{base}/fact_stop_event")
)
print(f"fact_stop_event exported -> {base}/fact_stop_event")

# COMMAND ----------

# Dimensions: small and unpartitioned, overwritten whole on every publish.
# dim_date is exported in full; the RAG can filter it to the fact's range on load.
for table, folder in DIMENSIONS:
    (
        spark.table(table)
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(f"{base}/{folder}")
    )
    print(f"{folder} exported -> {base}/{folder}")

# COMMAND ----------

# Coverage manifest: the queryable range and per-year volume, so the consumer can
# report its own limits instead of returning silent empty results outside the window.
per_year = {
    str(row["year"]): row["rows"]
    for row in (
        fact.groupBy(F.year("date_key").alias("year"))
        .agg(F.count("*").alias("rows"))
        .orderBy("year")
        .collect()
    )
}
bounds = fact.agg(
    F.min("date_key").alias("min_date"),
    F.max("date_key").alias("max_date"),
    F.count("*").alias("total_rows"),
).first()

manifest = {
    "exported_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    "requested_range": {"start": range_start, "end": range_end},
    "fact_stop_event": {
        "min_date_key": str(bounds["min_date"]),
        "max_date_key": str(bounds["max_date"]),
        "total_rows": bounds["total_rows"],
        "rows_per_year": per_year,
    },
    "dimensions": {folder: spark.table(table).count() for table, folder in DIMENSIONS},
    "layout": {
        "fact_stop_event": "fact_stop_event/date_key=YYYY-MM-DD/*.parquet",
        "dim_date": "dim_date/*.parquet",
        "dim_station": "dim_station/*.parquet",
        "dim_relation": "dim_relation/*.parquet",
    },
}

dbutils.fs.put(
    f"{base}/_manifest/coverage.json",
    json.dumps(manifest, indent=2),
    overwrite=True,
)
print(json.dumps(manifest, indent=2))

# COMMAND ----------

# --- Contract verification ------------------------------------------------------
# Spark's view of the published schema. Informative, but NOT the authoritative
# check for `date_key`: the consumer reads this data with PyArrow, not Spark.
for folder in ["fact_stop_event"] + [f for _, f in DIMENSIONS]:
    print(f"--- {folder} (Spark) ---")
    spark.read.parquet(f"{base}/{folder}").printSchema()

# COMMAND ----------

# PyArrow's view — this is the type the RAG loader will actually receive, since
# Hive partition keys are inferred by the reader rather than stored in the files.
import pyarrow.dataset as ds

fact_ds = ds.dataset(
    f"{base}/fact_stop_event", format="parquet", partitioning="hive"
)
print("--- fact_stop_event (PyArrow) ---")
print(fact_ds.schema)
print("\ndate_key inferred type:", fact_ds.schema.field("date_key").type)

for _, folder in DIMENSIONS:
    print(f"\n--- {folder} (PyArrow) ---")
    print(ds.dataset(f"{base}/{folder}", format="parquet").schema)

# COMMAND ----------

# Nullability against real data, not the DDL: a column declared nullable but never
# null in three years of history is a different contract from one that is often null.
nullable_candidates = [
    "delay_arr_s",
    "delay_dep_s",
    "dwell_delta_s",
    "planned_hour",
    "train_no",
    "relation_key",
]
total = bounds["total_rows"]

fact_nulls = fact.agg(
    *[F.sum(F.col(c).isNull().cast("long")).alias(c) for c in nullable_candidates]
).first()

print(f"fact_stop_event — {total:,} rows")
for col in nullable_candidates:
    n = fact_nulls[col]
    print(f"  {col:<16} nulls: {n:>12,}  ({100 * n / total:.4f}%)")

station = spark.table(GOLD_DIM_STATION)
station_total = station.count()
station_nulls = station.agg(
    F.sum(F.col("ptcar_no").isNull().cast("long")).alias("ptcar_no"),
    F.sum(F.col("station_name").isNull().cast("long")).alias("station_name"),
).first()

print(f"\ndim_station — {station_total:,} rows")
for col in ["ptcar_no", "station_name"]:
    n = station_nulls[col]
    print(f"  {col:<16} nulls: {n:>12,}  ({100 * n / station_total:.4f}%)")
