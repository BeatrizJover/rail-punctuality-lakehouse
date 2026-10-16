# Silver maintenance routines for derived tables that are not part of the
# stop_event MERGE.

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.rail.config import (
    BRONZE_OPERATIONAL_POINT, SILVER_STATION_ALIAS, SILVER_STATION_ALIAS_CHANGES,
)
from src.rail.identity import (
    STATION_ALIAS_CHANGES_SCHEMA, STATION_ALIAS_SCHEMA,
    alias_changes, alias_upserts, resolve_station_aliases,
)


def create_station_alias_tables(spark) -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_STATION_ALIAS} ({STATION_ALIAS_SCHEMA}) USING DELTA")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {SILVER_STATION_ALIAS_CHANGES} "
        f"({STATION_ALIAS_CHANGES_SCHEMA}) USING DELTA"
    )

    # Tables created before evidence_month existed.
    if "evidence_month" not in spark.table(SILVER_STATION_ALIAS).columns:
        spark.sql(
            f"ALTER TABLE {SILVER_STATION_ALIAS} "
            "ADD COLUMNS (evidence_month DATE AFTER resolution_source)"
        )


def refresh_station_aliases(spark, observed: DataFrame) -> None:
    """Resolve the stop points in `observed` (Silver stop_event rows) into
    silver.station_alias, writing only rows whose values changed.

    Change rows are appended before the MERGE: if the MERGE fails, a rerun
    re-emits them rather than losing them.
    """
    create_station_alias_tables(spark)

    existing = spark.table(SILVER_STATION_ALIAS)
    resolved = resolve_station_aliases(observed, spark.table(BRONZE_OPERATIONAL_POINT), existing)
    upserts = alias_upserts(existing, resolved)
    changes = alias_changes(existing, upserts)

    upsert_count = upserts.count()
    change_count = changes.count()

    if change_count:
        (
            changes.withColumn("changed_at", F.current_timestamp())
            .write.mode("append")
            .saveAsTable(SILVER_STATION_ALIAS_CHANGES)
        )

    if upsert_count:
        (
            DeltaTable.forName(spark, SILVER_STATION_ALIAS)
            .alias("t")
            .merge(
                upserts.withColumn("updated_at", F.current_timestamp()).alias("s"),
                "t.stop_point_key = s.stop_point_key",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(f"station_alias: {upsert_count} row(s) written, {change_count} resolution change(s)")
