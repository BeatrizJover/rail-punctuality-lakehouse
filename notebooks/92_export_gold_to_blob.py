# Databricks notebook source
"""
Publishes the Gold layer to Azure Blob as date-partitioned Parquet — the stable,
credential-free interface the downstream RAG project consumes.

This layout is the published contract: the RAG loader mirrors it verbatim,
so any change here is a contract change and must be reflected in its data_contracts.

    {container}/{EXPORT_PREFIX}/fact_stop_event/date_key=YYYY-MM-DD/*.parquet
    {container}/{EXPORT_PREFIX}/dim_date/*.parquet
    {container}/{EXPORT_PREFIX}/dim_station/*.parquet
    {container}/{EXPORT_PREFIX}/dim_relation/*.parquet
    {container}/{EXPORT_PREFIX}/_manifest/coverage.json

`coverage.json` records the queryable range and per-year row counts, seeding the
consumer's coverage-awareness without a round-trip to Databricks.

Authentication uses a container-scoped SAS token held in a Databricks secret, set
into the Spark session as a fixed ABFS token. The token is read-only for the
consumer; this writer needs a read/write SAS. For a hardened setup, prefer a Unity
Catalog external location over a session-level SAS (see the commented block below).

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

from src.rail.config import (
    GOLD_FACT,
    GOLD_DIM_DATE,
    GOLD_DIM_STATION,
    GOLD_DIM_RELATION,
    EXPORT_PREFIX,
)

# COMMAND ----------

dbutils.widgets.text("start_year", "2024", "Start year (YYYY)")
dbutils.widgets.text("end_year", "2026", "End year (YYYY)")
dbutils.widgets.text("storage_account", "", "Azure storage account name")
dbutils.widgets.text("container", "", "Blob container name")
dbutils.widgets.text("secret_scope", "rail_export", "Databricks secret scope")
dbutils.widgets.text("secret_key", "blob_sas_token", "Secret key holding the SAS token")

start_year = dbutils.widgets.get("start_year").strip()
end_year = dbutils.widgets.get("end_year").strip()
storage_account = dbutils.widgets.get("storage_account").strip()
container = dbutils.widgets.get("container").strip()
secret_scope = dbutils.widgets.get("secret_scope").strip()
secret_key = dbutils.widgets.get("secret_key").strip()

for name, value in [("start_year", start_year), ("end_year", end_year)]:
    if not re.fullmatch(r"\d{4}", value):
        raise ValueError(f"Invalid {name} widget value: {value!r}")
if start_year > end_year:
    raise ValueError(f"start_year {start_year} is after end_year {end_year}")
if not storage_account or not container:
    raise ValueError("storage_account and container widgets are required")

range_start = f"{start_year}-01-01"
range_end = f"{end_year}-12-31"

print(f"Exporting Gold for service dates {range_start} .. {range_end}")

# COMMAND ----------

# Bind the container-scoped SAS token as a fixed ABFS credential for this session.
sas_token = dbutils.secrets.get(scope=secret_scope, key=secret_key)

endpoint = f"{storage_account}.dfs.core.windows.net"
spark.conf.set(f"fs.azure.account.auth.type.{endpoint}", "SAS")
spark.conf.set(
    f"fs.azure.sas.token.provider.type.{endpoint}",
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
)
spark.conf.set(f"fs.azure.sas.fixed.token.{endpoint}", sas_token)

# --- Alternative: Unity Catalog external location (no session SAS) ---------------
# Create a storage credential + external location pointing at the container once,
# then drop the three spark.conf.set calls above and let the abfss path resolve
# through Unity Catalog. Preferred for anything beyond a personal Free Edition setup.
# --------------------------------------------------------------------------------

base = f"abfss://{container}@{endpoint}/{EXPORT_PREFIX}"

# COMMAND ----------

# Fact: filter to the published window, one file per date partition, dynamic overwrite
# so re-running a range replaces only its partitions and never the whole dataset.
fact = spark.table(GOLD_FACT).filter(F.col("date_key").between(range_start, range_end))

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
for table, folder in [
    (GOLD_DIM_DATE, "dim_date"),
    (GOLD_DIM_STATION, "dim_station"),
    (GOLD_DIM_RELATION, "dim_relation"),
]:
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
        "min_date_key": str(bounds["min_date"]) if bounds["min_date"] else None,
        "max_date_key": str(bounds["max_date"]) if bounds["max_date"] else None,
        "total_rows": bounds["total_rows"],
        "rows_per_year": per_year,
    },
    "dimensions": {
        "dim_date": spark.table(GOLD_DIM_DATE).count(),
        "dim_station": spark.table(GOLD_DIM_STATION).count(),
        "dim_relation": spark.table(GOLD_DIM_RELATION).count(),
    },
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
