# Databricks notebook source
# ONE-SHOT MIGRATION -- run once, manually, before deploying the station_identity
# task. Not part of the scheduled job.
#
# Seeds silver.station_alias from the full Silver history plus
# bronze.operational_point. Safe to re-run: resolution is deterministic and only
# rows whose values changed are written.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.rail.config import SILVER_STATION_ALIAS, SILVER_STOP
from src.rail.silver_maintenance import refresh_station_aliases

# COMMAND ----------

refresh_station_aliases(spark, spark.table(SILVER_STOP))

# COMMAND ----------

display(spark.sql(f"""
SELECT status, resolution_source, count(*) AS stop_points
FROM {SILVER_STATION_ALIAS}
GROUP BY ALL
ORDER BY ALL
"""))
