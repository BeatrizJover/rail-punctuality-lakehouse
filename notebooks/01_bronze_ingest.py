# Ingests raw rail punctuality data from the landing volume into the bronze layer

import datetime as dt
import requests

CATALOG = "rail_punctuality"
LANDING = f"/Volumes/{CATALOG}/bronze/landing"

BASE = "https://opendata.infrabel.be/api/explore/v2.1/catalog/datasets"
DATASET = "ruwe-gegevens-van-stiptheid-d-1"

run_date = dt.date.today() - dt.timedelta(days=1)
target = f"{LANDING}/d1/{run_date:%Y-%m-%d}.csv"

dbutils.fs.mkdirs(f"{LANDING}/d1")

resp = requests.get(
    f"{BASE}/{DATASET}/exports/csv",
    params={"delimiter": ";"},
    timeout=600
)

resp.raise_for_status()

with open(target, "wb") as fh:
    fh.write(resp.content)

print(f"landed {len(resp.content):,} bytes -> {target}")