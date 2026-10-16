# Station identity: resolves Silver's stop_point_key to Infrabel's PTCAR_NO.
# Pure DataFrame logic; table I/O lives in silver_maintenance.

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.rail.transforms import normalize_station_name

STATION_ALIAS_SCHEMA = (
    "stop_point_key STRING, stop_point_name_key STRING, stop_point_name STRING, "
    "ptcar_no INT, status STRING, resolution_source STRING, "
    "first_seen DATE, last_seen DATE, updated_at TIMESTAMP"
)

STATION_ALIAS_CHANGES_SCHEMA = (
    "stop_point_key STRING, old_ptcar_no INT, new_ptcar_no INT, "
    "old_status STRING, new_status STRING, "
    "old_resolution_source STRING, new_resolution_source STRING, changed_at TIMESTAMP"
)

ALIAS_COLUMNS = [
    "stop_point_key", "stop_point_name_key", "stop_point_name", "ptcar_no",
    "status", "resolution_source", "first_seen", "last_seen",
]
RESOLUTION_COLUMNS = ["ptcar_no", "status", "resolution_source"]


def _reference_ptcar_by_name(operational_points: DataFrame) -> DataFrame:
    """Normalized longnamedutch -> PTCAR_NO, kept only where the name maps to exactly one ID."""
    return (
        operational_points.select(
            normalize_station_name("longnamedutch").alias("ref_name_key"),
            F.expr("try_cast(ptcarid AS INT)").alias("ref_ptcar_no"),
        )
        .filter(F.col("ref_name_key").isNotNull() & F.col("ref_ptcar_no").isNotNull())
        .groupBy("ref_name_key")
        .agg(
            F.countDistinct("ref_ptcar_no").alias("ids"),
            F.max("ref_ptcar_no").alias("ref_ptcar_no"),
        )
        .filter(F.col("ids") == 1)
        .drop("ids")
    )


def resolve_station_aliases(
    observed: DataFrame, operational_points: DataFrame, existing: DataFrame
) -> DataFrame:
    """Resolve every stop_point_key in `observed` against prior state and the reference.

    Expects `observed` with Silver's stop_point_key, stop_point_name_key,
    stop_point_name, ptcar_no, source_feed, service_date; `operational_points`
    with ptcarid, longnamedutch; `existing` with the station_alias columns.
    Returns ALIAS_COLUMNS for the observed keys only.

    Precedence: monthly native PTCAR_NO, then a unique reference name, else
    unresolved. Monthly evidence accumulates with the stored monthly result,
    so a name seen with two IDs stays ambiguous whatever the backfill order,
    and the reference never overrides a monthly resolution.
    """
    obs = observed.groupBy("stop_point_key").agg(
        F.max("stop_point_name_key").alias("obs_name_key"),
        F.max("stop_point_name").alias("obs_name"),
        F.min("service_date").alias("obs_first_seen"),
        F.max("service_date").alias("obs_last_seen"),
        # Daily rows carry crosswalk-enriched IDs; only monthly ones are evidence.
        F.collect_set(F.when(F.col("source_feed") == "monthly", F.col("ptcar_no"))).alias("obs_monthly"),
    )
    prev = existing.select(
        "stop_point_key", *[F.col(c).alias(f"prev_{c}") for c in ALIAS_COLUMNS[1:]]
    )
    ref = _reference_ptcar_by_name(operational_points)

    df = (
        obs.join(prev, "stop_point_key", "left")
        .join(ref, F.col("obs_name_key") == F.col("ref_name_key"), "left")
    )

    prev_monthly = F.col("prev_resolution_source") == "monthly_native"
    monthly = F.array_distinct(
        F.concat(
            F.col("obs_monthly"),
            F.when(prev_monthly & F.col("prev_ptcar_no").isNotNull(), F.array("prev_ptcar_no"))
            .otherwise(F.array().cast("array<int>")),
        )
    )
    ambiguous = (prev_monthly & (F.col("prev_status") == "ambiguous")) | (F.size(monthly) > 1)
    by_monthly = F.size(monthly) == 1
    by_reference = F.col("ref_ptcar_no").isNotNull()

    ptcar_no = (
        F.when(ambiguous, F.lit(None))
        .when(by_monthly, F.element_at(monthly, 1))
        .otherwise(F.col("ref_ptcar_no"))
        .cast("int")
    )

    return df.select(
        "stop_point_key",
        F.coalesce("prev_stop_point_name_key", "obs_name_key").alias("stop_point_name_key"),
        F.coalesce("prev_stop_point_name", "obs_name").alias("stop_point_name"),
        ptcar_no.alias("ptcar_no"),
        F.when(ambiguous, "ambiguous")
        .when(ptcar_no.isNotNull(), "resolved")
        .otherwise("unresolved")
        .alias("status"),
        F.when(ambiguous | by_monthly, "monthly_native")
        .when(by_reference, "operational_point_name")
        .alias("resolution_source"),
        F.least("prev_first_seen", "obs_first_seen").alias("first_seen"),
        F.greatest("prev_last_seen", "obs_last_seen").alias("last_seen"),
    )


def _differs(columns: list[str]) -> Column:
    return ~reduce(
        lambda acc, c: acc & F.col(f"r.{c}").eqNullSafe(F.col(f"p.{c}")),
        columns,
        F.lit(True),
    )


def alias_upserts(existing: DataFrame, resolved: DataFrame) -> DataFrame:
    """Resolved rows that are new or differ from the stored alias in any column."""
    return (
        resolved.alias("r")
        .join(existing.select(ALIAS_COLUMNS).alias("p"), "stop_point_key", "left")
        .filter(F.col("p.status").isNull() | _differs(ALIAS_COLUMNS[1:]))
        .select("stop_point_key", *[F.col(f"r.{c}").alias(c) for c in ALIAS_COLUMNS[1:]])
    )


def alias_changes(existing: DataFrame, resolved: DataFrame) -> DataFrame:
    """One row per stored key whose resolution changed; first inserts are not changes."""
    return (
        resolved.alias("r")
        .join(existing.select(ALIAS_COLUMNS).alias("p"), "stop_point_key", "inner")
        .filter(_differs(RESOLUTION_COLUMNS))
        .select(
            "stop_point_key",
            *[
                col
                for c in RESOLUTION_COLUMNS
                for col in (F.col(f"p.{c}").alias(f"old_{c}"), F.col(f"r.{c}").alias(f"new_{c}"))
            ],
        )
    )
