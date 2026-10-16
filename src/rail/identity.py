# Station identity: resolves Silver's stop_point_key to Infrabel's PTCAR_NO.
# Pure DataFrame logic; table I/O lives in silver_maintenance.

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.rail.transforms import normalize_station_name

STATION_ALIAS_SCHEMA = (
    "stop_point_key STRING, stop_point_name_key STRING, stop_point_name STRING, "
    "ptcar_no INT, status STRING, resolution_source STRING, evidence_month DATE, "
    "first_seen DATE, last_seen DATE, updated_at TIMESTAMP"
)

STATION_ALIAS_CHANGES_SCHEMA = (
    "stop_point_key STRING, old_ptcar_no INT, new_ptcar_no INT, "
    "old_status STRING, new_status STRING, "
    "old_resolution_source STRING, new_resolution_source STRING, changed_at TIMESTAMP"
)

ALIAS_COLUMNS = [
    "stop_point_key", "stop_point_name_key", "stop_point_name", "ptcar_no",
    "status", "resolution_source", "evidence_month", "first_seen", "last_seen",
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
    unresolved. Monthly evidence is taken from the key's most recent month in
    the batch, and replaces the stored evidence only when that month is not
    older than the stored evidence_month, so backfill order does not matter.
    Two IDs within that month make the key ambiguous. A reference-resolved
    alias is kept when the reference no longer yields a unique ID, and the
    reference never overrides a monthly resolution.
    """
    obs = observed.groupBy("stop_point_key").agg(
        F.max("stop_point_name_key").alias("obs_name_key"),
        F.max("stop_point_name").alias("obs_name"),
        F.min("service_date").alias("obs_first_seen"),
        F.max("service_date").alias("obs_last_seen"),
    )

    # Only monthly rows carry a native ID; older daily rows may hold crosswalk values.
    monthly = (
        observed.filter((F.col("source_feed") == "monthly") & F.col("ptcar_no").isNotNull())
        .select("stop_point_key", F.trunc("service_date", "month").alias("month"), "ptcar_no")
    )
    latest = monthly.groupBy("stop_point_key").agg(F.max("month").alias("latest_month"))
    evidence = (
        monthly.join(latest, "stop_point_key")
        .filter(F.col("month") == F.col("latest_month"))
        .groupBy("stop_point_key", F.col("latest_month").alias("ev_month"))
        .agg(F.collect_set("ptcar_no").alias("ev_ptcars"))
    )

    prev = existing.select(
        "stop_point_key", *[F.col(c).alias(f"prev_{c}") for c in ALIAS_COLUMNS[1:]]
    )
    ref = _reference_ptcar_by_name(operational_points)

    df = (
        obs.join(prev, "stop_point_key", "left")
        .join(evidence, "stop_point_key", "left")
        .join(ref, F.col("obs_name_key") == F.col("ref_name_key"), "left")
    )

    prev_monthly = F.coalesce(F.col("prev_resolution_source") == "monthly_native", F.lit(False))
    prev_reference = F.coalesce(F.col("prev_resolution_source") == "operational_point_name", F.lit(False))
    # A NULL stored month (legacy row) counts as the oldest evidence.
    use_evidence = F.col("ev_month").isNotNull() & (
        ~prev_monthly
        | F.col("prev_evidence_month").isNull()
        | (F.col("ev_month") >= F.col("prev_evidence_month"))
    )
    single = F.size("ev_ptcars") == 1
    by_reference = F.col("ref_ptcar_no").isNotNull()
    keep_prev = ~use_evidence & (prev_monthly | (prev_reference & ~by_reference))

    return df.select(
        "stop_point_key",
        F.coalesce("prev_stop_point_name_key", "obs_name_key").alias("stop_point_name_key"),
        F.coalesce("prev_stop_point_name", "obs_name").alias("stop_point_name"),
        F.when(use_evidence & single, F.element_at("ev_ptcars", 1))
        .when(use_evidence, F.lit(None))
        .when(keep_prev, F.col("prev_ptcar_no"))
        .otherwise(F.col("ref_ptcar_no"))
        .cast("int")
        .alias("ptcar_no"),
        F.when(use_evidence & single, "resolved")
        .when(use_evidence, "ambiguous")
        .when(keep_prev, F.col("prev_status"))
        .when(by_reference, "resolved")
        .otherwise("unresolved")
        .alias("status"),
        F.when(use_evidence, "monthly_native")
        .when(keep_prev, F.col("prev_resolution_source"))
        .when(by_reference, "operational_point_name")
        .alias("resolution_source"),
        F.when(use_evidence, F.col("ev_month"))
        .when(keep_prev, F.col("prev_evidence_month"))
        .alias("evidence_month"),
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


UNRESOLVED_STATION_KEY = -1


def build_station_dimension(aliases: DataFrame, operational_points: DataFrame) -> DataFrame:
    """Core dim_station rows keyed on PTCAR_NO, plus the unresolved member.

    One row per PTCAR_NO with a resolved alias. first_seen/last_seen are
    recomputed from the aliases rather than accumulated, so a corrected alias
    shrinks the range of the station it no longer points to. station_name is
    the official longnamedutch when the ID is in the reference, otherwise the
    name of the most recently seen alias.
    Returns station_key, station_name, ptcar_no, first_seen, last_seen.
    """
    by_station = (
        aliases.filter(F.col("status") == "resolved")
        .groupBy("ptcar_no")
        .agg(
            F.min("first_seen").alias("first_seen"),
            F.max("last_seen").alias("last_seen"),
            F.max_by("stop_point_name", F.struct("last_seen", "stop_point_name")).alias("alias_name"),
        )
    )
    official = (
        operational_points.select(
            F.expr("try_cast(ptcarid AS INT)").alias("ptcar_no"),
            F.trim("longnamedutch").alias("official_name"),
        )
        .filter(F.col("ptcar_no").isNotNull() & (F.col("official_name") != ""))
        .groupBy("ptcar_no")
        .agg(F.max("official_name").alias("official_name"))
    )
    stations = by_station.join(official, "ptcar_no", "left").select(
        F.col("ptcar_no").alias("station_key"),
        F.coalesce("official_name", "alias_name").alias("station_name"),
        "ptcar_no",
        "first_seen",
        "last_seen",
    )
    unresolved = stations.sparkSession.createDataFrame(
        [(UNRESOLVED_STATION_KEY, "Unresolved", None, None, None)],
        "station_key INT, station_name STRING, ptcar_no INT, first_seen DATE, last_seen DATE",
    )
    return stations.unionByName(unresolved)
