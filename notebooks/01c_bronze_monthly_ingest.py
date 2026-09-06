# Databricks notebook source
# Ingests landed monthly Infrabel exports into a dedicated bronze table.
# Run one year at a time: each monthly file is ~2M rows / ~300 MB.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import re

from pyspark.sql import functions as F

from src.rail.config import BRONZE_RAW_MONTHLY, MONTHLY_CSV_SEP, MONTHLY_LANDING
from src.rail.transforms import count_unparsed_dates, normalize_monthly_dates

# Column order is fixed here so writes match the table schema by position.
SOURCE_COLUMNS = [
    "DATDEP", "CIRC_TYP", "TRAIN_NO", "RELATION", "TRAIN_SERV",
    "OP1_COD", "THOP1_COD", "PTCAR_NO", "PTCAR_LG_NM_NL", "LINE_NO_DEP",
    "REAL_DATE_ARR", "REAL_TIME_ARR", "REAL_DATE_DEP", "REAL_TIME_DEP",
    "PLANNED_DATE_ARR", "PLANNED_TIME_ARR", "PLANNED_TIME_DEP", "PLANNED_DATE_DEP",
    "DELAY_ARR", "DELAY_DEP", "RELATION_DIRECTION", "LINE_NO_ARR",
]

# COMMAND ----------

dbutils.widgets.text("year", "2026", "Year (YYYY)")
dbutils.widgets.text("months", "all", "Months (all | 1,2,3)")

year = dbutils.widgets.get("year").strip()
months_arg = dbutils.widgets.get("months").strip().lower()

if not re.fullmatch(r"\d{4}", year):
    raise ValueError(f"Invalid year widget value: {year!r}")

wanted = (
    set(range(1, 13))
    if months_arg == "all"
    else {int(m) for m in months_arg.split(",") if m.strip()}
)

available = {
    match.group(1)
    for entry in dbutils.fs.ls(MONTHLY_LANDING)
    if (match := re.search(r"Data_raw_punctuality_(\d{6})\.csv$", entry.path))
}
selected = sorted(
    ym for ym in available if ym[:4] == year and int(ym[4:]) in wanted
)

print(f"{len(selected)} month(s) selected for {year}: {selected}")

# COMMAND ----------

column_ddl = ",\n    ".join(f"{c} STRING" for c in SOURCE_COLUMNS)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_RAW_MONTHLY} (
    {column_ddl},
    source_year_month STRING,
    _source_file      STRING,
    _ingested_at      TIMESTAMP
)
USING DELTA
PARTITIONED BY (source_year_month)
""")

spark.sql(f"""
COMMENT ON TABLE {BRONZE_RAW_MONTHLY} IS
  'Raw monthly Infrabel exports. Dates normalized to ISO strings on ingest; all
   other columns left as strings. Partitioned by source month, which is the unit
   of idempotency: re-ingesting a month replaces it.'
""")

# COMMAND ----------

for ym in selected:
    path = f"{MONTHLY_LANDING}/Data_raw_punctuality_{ym}.csv"

    raw = (
        spark.read
        .option("header", True)
        .option("sep", MONTHLY_CSV_SEP)
        .csv(path)
    )

    missing = set(SOURCE_COLUMNS) - set(raw.columns)
    if missing:
        raise ValueError(f"{ym}: missing expected columns {sorted(missing)}")

    # Abort rather than write NULL dates: a locale mismatch would otherwise be
    # invisible until the whole month vanished in the Silver null filter.
    unparsed = count_unparsed_dates(raw)
    if any(unparsed.values()):
        raise ValueError(f"{ym}: unparseable date literals {unparsed}")

    batch = (
        normalize_monthly_dates(raw)
        .select(*SOURCE_COLUMNS)
        .withColumn("source_year_month", F.lit(ym))
        .withColumn("_source_file", F.lit(path))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    (
        batch.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"source_year_month = '{ym}'")
        .saveAsTable(BRONZE_RAW_MONTHLY)
    )

    print(f"{ym}: ingested")

# COMMAND ----------

(
    spark.table(BRONZE_RAW_MONTHLY)
    .groupBy("source_year_month")
    .agg(
        F.count("*").alias("rows"),
        F.min("DATDEP").alias("min_date"),
        F.max("DATDEP").alias("max_date"),
        F.max("_ingested_at").alias("ingested_at"),
    )
    .orderBy("source_year_month")
    .show(150, truncate=False)
)