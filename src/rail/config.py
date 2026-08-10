# Configuration for the rail punctuality lakehouse.

CATALOG = "rail_punctuality"

BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD   = f"{CATALOG}.gold"
OPS    = f"{CATALOG}.ops"

LANDING     = f"/Volumes/{CATALOG}/bronze/landing"
CHECKPOINTS = f"/Volumes/{CATALOG}/bronze/checkpoints"

BRONZE_RAW  = f"{BRONZE}.punctuality_raw"
SILVER_STOP = f"{SILVER}.stop_event"
DQ_RESULTS  = f"{OPS}.dq_results"

ODS_BASE        = "https://opendata.infrabel.be/api/explore/v2.1/catalog/datasets"
DATASET_DAILY   = "ruwe-gegevens-van-stiptheid-d-1"
DATASET_MONTHLY = "stiptheid-gegevens-maandelijksebestanden"

CSV_SEP = ";"

# Infrabel definition: a train is punctual below 6 minutes (max 5 min 59 s)
PUNCTUAL_THRESHOLD_S = 360