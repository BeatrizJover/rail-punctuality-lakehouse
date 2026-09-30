# Databricks notebook source
# Throwaway validation for the dim_station enrichment (station_type,
# is_passenger, latitude, longitude). Not a bundle resource -- run by hand
# after the migration or a daily refresh. Flat on purpose, no abstractions.

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# COMMAND ----------

# Measured Arrivals, aggregated in Spark before toPandas() -- matches the
# Power BI definition. observed_stop_events no longer exists on dim_station.
arrivals = (
    spark.table("rail_punctuality.gold.fact_stop_event")
    .groupBy("station_key")
    .agg(F.count("delay_arr_s").alias("measured_arrivals"))
)

stations = spark.table("rail_punctuality.gold.dim_station").select(
    "station_key", "station_name", "ptcar_no", "station_type", "is_passenger",
    "latitude", "longitude",
)

df = stations.join(arrivals, "station_key", "left").fillna({"measured_arrivals": 0}).toPandas()

# COMMAND ----------

print("passenger vs non-passenger:")
print(df["is_passenger"].value_counts())

print("\nmatched vs unmatched (station_type is null == unmatched):")
print(df["station_type"].isna().value_counts())

# COMMAND ----------

print("\nunmatched rows with their is_passenger verdict:")
unmatched = df[df["station_type"].isna()]
print(unmatched[["station_name", "ptcar_no", "is_passenger"]].to_string(index=False))

# COMMAND ----------

print("\nis_passenger vs measured_arrivals >= 1000 (current Power BI proxy):")
df["high_volume"] = df["measured_arrivals"] >= 1000
print(pd.crosstab(df["is_passenger"], df["high_volume"]))

disagreements = df[df["is_passenger"] != df["high_volume"]]
print(f"\n{len(disagreements)} row(s) where is_passenger and the volume proxy disagree:")
print(disagreements[["station_name", "is_passenger", "measured_arrivals"]].to_string(index=False))

# COMMAND ----------

print("\nmatched passenger stations whose name still carries a service-infra token:")
SERVICE_INFRA_TOKENS = (
    "-BUNDEL", "-FAISCEAU", "-T.W.", "-GASOIL", "-CARWASH", "-DOODSPOOR",
    "-SEA-RO TERMINAL",
)
flagged = df[
    df["station_type"].notna()
    & df["is_passenger"]
    & df["station_name"].str.upper().apply(lambda n: any(t in n for t in SERVICE_INFRA_TOKENS))
]
print(flagged[["station_name", "station_type"]].to_string(index=False))

# COMMAND ----------

# Sanity check: both stations are known real passenger stations absent from
# the current operational-point snapshot, so they have no coordinates.
for name in ["MORTSEL-DEURNESTEENWEG", "BAULERS"]:
    match = df[df["station_name"].str.upper() == name]
    assert not match.empty, f"{name} not found in dim_station"
    assert match["is_passenger"].all(), f"{name} expected is_passenger = true"
print("\nMORTSEL-DEURNESTEENWEG and BAULERS: is_passenger = true, confirmed above (no coordinates, so no map check).")

# COMMAND ----------

with_coords = df.dropna(subset=["latitude", "longitude"])
missing_coords_passenger = df[df["is_passenger"] & df[["latitude", "longitude"]].isna().any(axis=1)]
print(f"\n{len(missing_coords_passenger)} passenger station(s) lack coordinates.")

plt.figure(figsize=(8, 8))
colors = with_coords["is_passenger"].map({True: "tab:blue", False: "tab:red"})
plt.scatter(with_coords["longitude"], with_coords["latitude"], c=colors, s=10, alpha=0.7)
plt.xlabel("longitude")
plt.ylabel("latitude")
plt.title("dim_station coordinates by is_passenger (blue=passenger, red=non-passenger)")
plt.show()
