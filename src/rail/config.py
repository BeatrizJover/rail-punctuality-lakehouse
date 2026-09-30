# Configuration for the rail punctuality lakehouse.

CATALOG = "rail_punctuality"

BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD   = f"{CATALOG}.gold"
OPS    = f"{CATALOG}.ops"

LANDING     = f"/Volumes/{CATALOG}/bronze/landing"
CHECKPOINTS = f"/Volumes/{CATALOG}/bronze/checkpoints"

BRONZE_RAW  = f"{BRONZE}.punctuality_raw"
BRONZE_STATION_REF = f"{BRONZE}.station_ref"
SILVER_STOP = f"{SILVER}.stop_event"
DQ_RESULTS  = f"{OPS}.dq_results"

ODS_BASE        = "https://opendata.infrabel.be/api/explore/v2.1/catalog/datasets"
DATASET_DAILY   = "ruwe-gegevens-van-stiptheid-d-1"
DATASET_MONTHLY = "stiptheid-gegevens-maandelijksebestanden"

CSV_SEP = ";"

# CSV_SEP is a request parameter for the D-1 API export, not a property of the
# monthly files, which are comma-delimited.
MONTHLY_CSV_SEP = ","

# The portal's own download URLs request labels, which are localized; pinning
# use_labels keeps the API export on field names whatever the portal default.
ODS_EXPORT_PARAMS = {"delimiter": CSV_SEP, "use_labels": "false"}

MONTHLY_LANDING = f"{LANDING}/monthly"
BRONZE_RAW_MONTHLY = f"{BRONZE}.punctuality_raw_monthly"

DATASET_OPERATIONAL_POINT = "operationele-punten-van-het-netwerk"
OPERATIONAL_POINT_LANDING = f"{LANDING}/reference"
BRONZE_OPERATIONAL_POINT = f"{BRONZE}.operational_point"

GOLD_DIM_STATION = f"{GOLD}.dim_station"

# CC0-licensed NMBS/SNCB station list, used to derive is_passenger: Infrabel's
# class_en = 'Station' also includes yards and freight points this list omits.
IRAIL_STATIONS_URL = "https://raw.githubusercontent.com/iRail/stations/master/stations.csv"
BRONZE_IRAIL_STATION = f"{BRONZE}.irail_station"

# class_en values on operational_point that count as passenger-facing: staffed
# stations plus unstaffed halts (stopplaatsen); verified against the reference.
PASSENGER_CLASSES = ("Station", "Stop in open track")

# Name tokens marking non-passenger infrastructure, applied only to unmatched
# stations. Do not extend the list.
SERVICE_INFRA_TOKENS = (
    "-BUNDEL", "-FAISCEAU", "-T.W.", "-GASOIL", "-CARWASH", "-DOODSPOOR",
    "-SEA-RO TERMINAL",
)

# Infrabel definition: a train is punctual below 6 minutes (max 5 min 59 s)
PUNCTUAL_THRESHOLD_S = 360