# Databricks notebook source
# ONE-SHOT MIGRATION -- run once, manually, after deploying the enrichment
# refresh. Not part of the scheduled job.
#
# Adds station_type, is_passenger, latitude and longitude to gold.dim_station
# and backfills them for every existing row. Safely re-runnable: it skips
# columns that already exist. Re-running 00_migration_dim_station_reseed.sql
# afterwards would drop these columns again -- if that ever happens, rerun
# this migration to restore them.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.rail.config import GOLD_DIM_STATION
from src.rail.gold_maintenance import refresh_station_enrichment

# COMMAND ----------

NEW_COLUMNS = {
    "station_type": "STRING",
    "is_passenger": "BOOLEAN",
    "latitude": "DOUBLE",
    "longitude": "DOUBLE",
}

existing_columns = {f.name for f in spark.table(GOLD_DIM_STATION).schema.fields}
missing_columns = {c: t for c, t in NEW_COLUMNS.items() if c not in existing_columns}

if missing_columns:
    ddl = ", ".join(f"{c} {t}" for c, t in missing_columns.items())
    spark.sql(f"ALTER TABLE {GOLD_DIM_STATION} ADD COLUMNS ({ddl})")
    print(f"added columns: {sorted(missing_columns)}")
else:
    print("columns already present, skipping ALTER TABLE")

# COMMAND ----------

# One-time full backfill of the new columns over the existing population.
refresh_station_enrichment(spark)
