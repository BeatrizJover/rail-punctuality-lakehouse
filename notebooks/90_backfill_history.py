"""
Ad-hoc historical backfill utility.
Extracts and lands targeted monthly datasets outside the primary automated pipeline.
"""

import os
import sys

import requests

# Resolve repository root for local module imports
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.rail.config import LANDING, ODS_BASE, DATASET_MONTHLY

# Execution parameters
# Target execution year and target months filter. Set BACKFILL_MONTHS to None for full-year processing.
BACKFILL_YEAR = 2026
BACKFILL_MONTHS = [7, 8]  # Type: Optional[List[int]]

MONTHLY_DIR = f"{LANDING}/monthly"
dbutils.fs.mkdirs(MONTHLY_DIR)

# Query catalog API with server-side temporal filtering
records_url = f"{ODS_BASE}/{DATASET_MONTHLY}/records"
response = requests.get(
    records_url,
    params={
        "limit": 20,
        "refine": f'mois:"{BACKFILL_YEAR}"',
    },
    timeout=120,
)
response.raise_for_status()

payload = response.json()
records = payload.get("results", [])

print(f"{len(records)} monthly records found for {BACKFILL_YEAR}")


# Filter payload against target execution window
selected = []

for record in records:
    month = record.get("mois")
    url = record.get("link_to_data")

    if not month or not url:
        continue

    try:
        year, month_number = map(int, month.split("-"))
    except ValueError:
        continue

    if year != BACKFILL_YEAR:
        continue

    if BACKFILL_MONTHS is not None and month_number not in BACKFILL_MONTHS:
        continue

    selected.append((month, url))

selected.sort()

print(
    f"{len(selected)} monthly files selected for {BACKFILL_YEAR}: "
    f"{[m for m, _ in selected]}"
)


# Fetch and land missing raw files
landed = 0

# Cache existing directory state to prevent redundant storage API calls
existing = dbutils.fs.ls(MONTHLY_DIR)
existing_names = {os.path.basename(f.path) for f in existing}

for month, url in selected:

    filename = f"Data_raw_punctuality_{month.replace('-', '')}.csv"
    output_path = f"{MONTHLY_DIR}/{filename}"

    if filename in existing_names:
        print(f"Already exists, skipping: {filename}")
        continue

    print(f"Downloading {month} -> {output_path}")

    file_response = requests.get(url, timeout=900)
    file_response.raise_for_status()

    with open(output_path, "wb") as dst:
        dst.write(file_response.content)

    existing_names.add(filename)
    landed += 1
    print(f"Saved {output_path}")


# Post-landing integrity verification
csvs = sorted(
    f.path
    for f in dbutils.fs.ls(MONTHLY_DIR)
    if f.path.lower().endswith(".csv")
)

print(f"\n{len(csvs)} CSV files in {MONTHLY_DIR}")

if csvs:

    first_file = csvs[0]
    local_path = first_file.replace("dbfs:", "")

    with open(
        local_path,
        encoding="utf-8",
        errors="replace",
    ) as fh:
        header = fh.readline().strip()

    print("First file :", first_file)
    print(
        "Separator  :",
        "';' OK" if ";" in header else "NOT ';' — inspect before ingesting",
    )
    print("Header     :", header[:300])

print(f"\nBackfill complete. New files landed: {landed}")