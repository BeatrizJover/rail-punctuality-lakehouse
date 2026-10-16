# Gold layer maintenance routines that operate over the full dimension rather
# than a single day's MERGE batch.

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.rail.config import (
    BRONZE_IRAIL_STATION, BRONZE_OPERATIONAL_POINT, GOLD_DIM_STATION, GOLD_FACT, SILVER_STATION_ALIAS,
)
from src.rail.identity import UNRESOLVED_STATION_KEY, build_station_dimension
from src.rail.transforms import enrich_stations

DIM_STATION_SCHEMA = (
    "station_key INT, station_name STRING, ptcar_no INT, first_seen DATE, last_seen DATE, "
    "station_type STRING, is_passenger BOOLEAN, latitude DOUBLE, longitude DOUBLE"
)

DIM_STATION_COMMENT = (
    "One row per measuring point, keyed on PTCAR_NO; station_key -1 holds stop "
    "points not yet resolved. Rebuilt daily from silver.station_alias."
)

_DIM_CORE_COLUMNS = ["station_name", "ptcar_no", "first_seen", "last_seen"]


def create_station_dimension(spark) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {GOLD_DIM_STATION} ({DIM_STATION_SCHEMA}) "
        f"USING DELTA COMMENT '{DIM_STATION_COMMENT}'"
    )


def refresh_station_dimension(spark) -> None:
    """Sync dim_station's core columns with silver.station_alias.

    Rows are written only where a core column changed; stations no alias
    resolves to any more are removed. Enrichment columns are left to
    refresh_station_enrichment.
    """
    create_station_dimension(spark)
    rows = build_station_dimension(
        spark.table(SILVER_STATION_ALIAS), spark.table(BRONZE_OPERATIONAL_POINT)
    )
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in _DIM_CORE_COLUMNS)
    (
        DeltaTable.forName(spark, GOLD_DIM_STATION)
        .alias("t")
        .merge(rows.alias("s"), "t.station_key = s.station_key")
        .whenMatchedUpdate(condition=changed, set={c: f"s.{c}" for c in _DIM_CORE_COLUMNS})
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in ["station_key", *_DIM_CORE_COLUMNS]})
        .whenNotMatchedBySourceDelete()
        .execute()
    )


def repair_fact_station_keys(spark) -> None:
    """Re-point fact rows whose station_key no longer matches their alias.

    Covers late resolutions (-1 to a PTCAR_NO) and corrected ones. The join on
    stop_point_key cannot prune, so this reads the fact's two key columns in
    full; only files holding mismatched rows are rewritten.
    """
    resolution = spark.table(SILVER_STATION_ALIAS).select(
        "stop_point_key",
        F.when(F.col("status") == "resolved", F.col("ptcar_no"))
        .otherwise(F.lit(UNRESOLVED_STATION_KEY))
        .cast("int")
        .alias("station_key"),
    )
    (
        DeltaTable.forName(spark, GOLD_FACT)
        .alias("t")
        .merge(
            resolution.alias("s"),
            "t.stop_point_key = s.stop_point_key AND t.station_key <> s.station_key",
        )
        .whenMatchedUpdate(set={"station_key": "s.station_key"})
        .execute()
    )


def refresh_station_enrichment(spark) -> None:
    """Recompute station_type, is_passenger, latitude and longitude for every
    row in gold.dim_station from the current bronze.operational_point and
    bronze.irail_station snapshots.

    dim_station's own MERGE source only holds the stations seen in a given
    run, so it can neither backfill the existing population nor pick up
    reclassifications for stations absent that day. This refresh scans the
    whole table instead -- cheap at dim_station's ~700-row scale -- and writes
    back only the four enrichment columns, only where something changed.
    """
    stations = spark.table(GOLD_DIM_STATION).select("station_key", "station_name", "ptcar_no")
    operational_points = spark.table(BRONZE_OPERATIONAL_POINT)
    irail_stations = spark.table(BRONZE_IRAIL_STATION)

    enriched = enrich_stations(stations, operational_points, irail_stations)

    target = DeltaTable.forName(spark, GOLD_DIM_STATION)
    (
        target.alias("t")
        .merge(enriched.alias("s"), "t.station_key = s.station_key")
        .whenMatchedUpdate(
            condition="""
                NOT (
                    t.station_type <=> s.station_type AND
                    t.is_passenger <=> s.is_passenger AND
                    t.latitude     <=> s.latitude AND
                    t.longitude    <=> s.longitude
                )
            """,
            set={
                "station_type": "s.station_type",
                "is_passenger": "s.is_passenger",
                "latitude": "s.latitude",
                "longitude": "s.longitude",
            },
        )
        .execute()
    )
