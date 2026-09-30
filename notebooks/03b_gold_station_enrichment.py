# Refreshes station_type, is_passenger, latitude and longitude on
# gold.dim_station from the current bronze.operational_point snapshot.
# Runs daily after gold_star_schema: dim_station's own MERGE only sees the
# day's stations, so this task does a full-table refresh instead, picking up
# a new quarterly operational-point snapshot automatically.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.rail.gold_maintenance import refresh_station_enrichment

# COMMAND ----------

refresh_station_enrichment(spark)
