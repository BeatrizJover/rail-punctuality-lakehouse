# Ad-hoc backfill script for reprocessing historical data outside the regular pipeline

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import io
import re
import zipfile
import requests

from src.rail.config import LANDING, ODS_BASE, DATASET_MONTHLY

START_YEAR, END_YEAR = 2024, 2026

MONTHLY_DIR = f"{LANDING}/monthly"
dbutils.fs.mkdirs(MONTHLY_DIR)

# Inspect the dataset metadata to discover available attachments and their fields.

meta = requests.get(f"{ODS_BASE}/{DATASET_MONTHLY}/attachments", timeout=120)
meta.raise_for_status()
payload = meta.json()

attachments = payload.get("attachments", payload) if isinstance(payload, dict) else payload
print(f"{len(attachments)} attachments found. First few:")
for item in attachments[:5]:
    print(item)

# Download files within the configured year range and extract CSVs from ZIP archives when required.

YEAR_RE = re.compile(r"(20\d{2})")

def in_range(name: str) -> bool:
    years = [int(y) for y in YEAR_RE.findall(name)]
    return any(START_YEAR <= y <= END_YEAR for y in years)

landed = 0
for item in attachments:
    name = item.get("title") or item.get("id") or ""
    url = item.get("url") or item.get("href")

    if not url or not in_range(name):
        continue

    blob = requests.get(url, timeout=900)
    blob.raise_for_status()

    is_zip = name.lower().endswith(".zip") or blob.content[:2] == b"PK"

    if is_zip:
        with zipfile.ZipFile(io.BytesIO(blob.content)) as zf:
            for member in zf.namelist():
                if not member.lower().endswith(".csv"):
                    continue
                out = f"{MONTHLY_DIR}/{os.path.basename(member)}"
                with zf.open(member) as src, open(out, "wb") as dst:
                    dst.write(src.read())
                landed += 1
                print("extracted", out)
    else:
        out = f"{MONTHLY_DIR}/{os.path.basename(name)}"
        with open(out, "wb") as dst:
            dst.write(blob.content)
        landed += 1
        print("saved", out)

print(f"{landed} monthly files in {MONTHLY_DIR}")

# Sanity check the landed files before ingestion:

csvs = sorted(f for f in os.listdir(MONTHLY_DIR) if f.lower().endswith(".csv"))
print(f"{len(csvs)} csv files")

if csvs:
    with open(f"{MONTHLY_DIR}/{csvs[0]}", encoding="utf-8", errors="replace") as fh:
        header = fh.readline().strip()
    print("first file :", csvs[0])
    print("separator  :", "';' OK" if ";" in header else "NOT ';' — fix before ingesting")
    print("header     :", header[:300])