# Gold layer maintenance routines that operate over the full dimension rather
# than a single day's MERGE batch.

from delta.tables import DeltaTable

from src.rail.config import BRONZE_IRAIL_STATION, BRONZE_OPERATIONAL_POINT, GOLD_DIM_STATION
from src.rail.transforms import enrich_stations


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
