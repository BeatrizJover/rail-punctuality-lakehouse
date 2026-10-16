# Maintains gold.dim_station after gold_star_schema: syncs it with
# silver.station_alias, re-points fact rows whose alias changed, then refreshes
# station_type, is_passenger, latitude and longitude from the current
# reference snapshots. Ends by recording the share of the run's fact rows
# still unresolved (station_key = -1).

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import datetime as dt

from pyspark.sql import functions as F

from src.rail.config import DQ_RESULTS, GOLD_FACT
from src.rail.gold_maintenance import (
    refresh_station_dimension, refresh_station_enrichment, repair_fact_station_keys,
)
from src.rail.quality import DQ_RESULT_SCHEMA, GOLD_COVERAGE, evaluate_checks

# COMMAND ----------

refresh_station_dimension(spark)
repair_fact_station_keys(spark)
refresh_station_enrichment(spark)

# COMMAND ----------

service_date = dbutils.jobs.taskValues.get(
    taskKey="silver_transform",
    key="service_date",
    default=None,
    debugValue=str(dt.date.today()),
)
if service_date is None:
    raise ValueError("No service_date received from silver_transform task value.")

fact_scoped = spark.table(GOLD_FACT).filter(F.col("date_key") == F.to_date(F.lit(service_date)))
rows = evaluate_checks(fact_scoped, GOLD_FACT, GOLD_COVERAGE, dt.datetime.now())
spark.createDataFrame(rows, DQ_RESULT_SCHEMA).write.mode("append").saveAsTable(DQ_RESULTS)
