# Databricks notebook source
'''
Backfills silver and gold from the monthly bronze table, one year per run. 
Reads bronze.punctuality_raw_monthly (already date-normalized), applies the silver transform with the native monthly PTCAR_NO, and MERGEs into silver and the gold fact over the full year range. 
Dimensions are rebuilt at the end.
'''

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import re

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.rail.config import BRONZE_RAW_MONTHLY, SILVER_STOP, PUNCTUAL_THRESHOLD_S
from src.rail.transforms import typed_stop_events, deduplicate_stop_events

# COMMAND ----------

dbutils.widgets.text("year", "2026", "Year (YYYY)")
year = dbutils.widgets.get("year").strip()

if not re.fullmatch(r"\d{4}", year):
    raise ValueError(f"Invalid year widget value: {year!r}")

year_start = f"{year}-01-01"
year_end = f"{year}-12-31"

months = sorted(
    row.source_year_month
    for row in spark.table(BRONZE_RAW_MONTHLY)
    .select("source_year_month")
    .distinct()
    .collect()
    if row.source_year_month.startswith(year)
)

print(f"{len(months)} month(s) to backfill for {year}: {months}")

# COMMAND ----------

# Initialize target Silver Delta table with Liquid Clustering
silver_table = DeltaTable.forName(spark, SILVER_STOP)

for ym in months:
    monthly = spark.table(BRONZE_RAW_MONTHLY).filter(F.col("source_year_month") == ym)

    typed = typed_stop_events(
        monthly,
        PUNCTUAL_THRESHOLD_S,
        source_feed="monthly",
        with_native_ptcar=True,
    )
    batch = deduplicate_stop_events(typed)

    (
        silver_table.alias("t")
        .merge(
            batch.alias("s"),
            "t.service_date   = s.service_date AND "
            "t.train_no       = s.train_no     AND "
            "t.stop_point_key = s.stop_point_key",
        )
        .whenMatchedUpdateAll(
            condition="t.source_feed = 'daily' OR s.source_feed = 'monthly'"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"{ym}: merged into silver")

# COMMAND ----------

# Merge the year's Silver events into the Gold fact table, one year at a time.
spark.sql(f"""
MERGE INTO rail_punctuality.gold.fact_stop_event t
USING (
    SELECT
        s.service_date   AS date_key,
        s.stop_point_key AS station_key,
        md5(concat_ws('|', s.relation, s.relation_direction, s.operator)) AS relation_key,
        s.train_no,
        s.planned_hour,
        s.delay_arr_s,
        s.delay_dep_s,
        s.dwell_delta_s,
        CAST(s.is_punctual_arr AS INT) AS punctual_arrivals,
        1                              AS stop_events
    FROM rail_punctuality.silver.stop_event s
    WHERE s.service_date BETWEEN DATE'{year_start}' AND DATE'{year_end}'
) s
ON  t.date_key BETWEEN DATE'{year_start}' AND DATE'{year_end}'
AND t.date_key    = s.date_key
AND t.station_key = s.station_key
AND t.train_no    = s.train_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print(f"gold fact merged for {year}")

# COMMAND ----------

# Maintain Gold dimensions incrementally over the backfilled year range.
spark.sql(f"""
MERGE INTO rail_punctuality.gold.dim_station t
USING (
    SELECT
        stop_point_key       AS station_key,
        max(stop_point_name) AS station_name,
        max(ptcar_no)        AS ptcar_no,
        min(service_date)    AS first_seen,
        max(service_date)    AS last_seen
    FROM rail_punctuality.silver.stop_event
    WHERE service_date BETWEEN DATE'{year_start}' AND DATE'{year_end}'
    GROUP BY stop_point_key
) s
ON t.station_key = s.station_key
WHEN MATCHED THEN UPDATE SET
    t.station_name = coalesce(s.station_name, t.station_name),
    t.ptcar_no     = coalesce(s.ptcar_no, t.ptcar_no),
    t.first_seen   = least(t.first_seen, s.first_seen),
    t.last_seen    = greatest(t.last_seen, s.last_seen)
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
MERGE INTO rail_punctuality.gold.dim_relation t
USING (
    SELECT DISTINCT
        md5(concat_ws('|', relation, relation_direction, operator)) AS relation_key,
        relation,
        relation_direction,
        operator
    FROM rail_punctuality.silver.stop_event
    WHERE service_date BETWEEN DATE'{year_start}' AND DATE'{year_end}'
) s
ON t.relation_key = s.relation_key
WHEN NOT MATCHED THEN INSERT *
""")
 
print(f"dimensions merged for {year}")

# COMMAND ----------

(
    spark.table(SILVER_STOP)
    .filter(F.col("service_date").between(year_start, year_end))
    .groupBy("source_feed")
    .agg(F.count("*").alias("rows"), F.countDistinct("service_date").alias("days"))
    .show(truncate=False)
)