# Silver layer transformations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.rail.config import PASSENGER_CLASSES, SERVICE_INFRA_TOKENS

NATURAL_KEY = ["service_date", "train_no", "stop_point_key"]

def _ts(date_col: str, time_col: str):
    """Combine date and time string columns into a single timestamp."""
    return F.try_to_timestamp(F.concat_ws(" ", F.col(date_col), F.col(time_col)))

_ACCENTED = "ÀÂÄÉÈÊËÎÏÔÖÙÛÜÇ"
_PLAIN    = "AAAEEEEIIOOUUUC"

def normalize_station_name(col: str):
    """Normalize station names by stripping accents, uppercase formatting, and collapsing whitespace."""
    c = F.upper(F.trim(F.col(col)))
    c = F.translate(c, _ACCENTED, _PLAIN)
    c = F.regexp_replace(c, r"\s+", " ")
    return c

def typed_stop_events(
    raw: DataFrame,
    punctual_threshold_s: int = 360,
    source_feed: str = "daily",
    with_native_ptcar: bool = False,
) -> DataFrame:
    """Cast Bronze raw string records into typed Silver schema and derive metrics.

    source_feed labels the row origin so the Silver MERGE can let the richer
    monthly feed win over the daily feed on overlapping service dates.

    with_native_ptcar reads PTCAR_NO directly from the row. Only the monthly
    export carries it; the daily feed leaves it NULL and relies on the
    station_ref left-join downstream.
    """
    name_key = normalize_station_name("PTCAR_LG_NM_NL")

    ptcar_col = (
        F.col("PTCAR_NO").cast("int")
        if with_native_ptcar
        else F.lit(None).cast("int")
    ).alias("ptcar_no")

    return (
        raw.select(
            F.to_date("DATDEP").alias("service_date"),
            F.col("TRAIN_NO").cast("int").alias("train_no"),
            F.trim("RELATION").alias("relation"),
            F.trim("RELATION_DIRECTION").alias("relation_direction"),
            F.trim("TRAIN_SERV").alias("operator"),
            F.trim("PTCAR_LG_NM_NL").alias("stop_point_name"),
            name_key.alias("stop_point_name_key"),
            F.md5(name_key).alias("stop_point_key"),
            ptcar_col,
            F.trim("LINE_NO_DEP").alias("line_no_dep"),
            F.trim("LINE_NO_ARR").alias("line_no_arr"),
            _ts("PLANNED_DATE_ARR", "PLANNED_TIME_ARR").alias("planned_arr_ts"),
            _ts("REAL_DATE_ARR", "REAL_TIME_ARR").alias("real_arr_ts"),
            _ts("PLANNED_DATE_DEP", "PLANNED_TIME_DEP").alias("planned_dep_ts"),
            _ts("REAL_DATE_DEP", "REAL_TIME_DEP").alias("real_dep_ts"),
            F.col("DELAY_ARR").cast("int").alias("delay_arr_s"),
            F.col("DELAY_DEP").cast("int").alias("delay_dep_s"),
            F.col("_ingested_at"),
        )
        .filter(
            F.col("service_date").isNotNull()
            & F.col("train_no").isNotNull()
            & F.col("stop_point_key").isNotNull()
        )
        .withColumn("source_feed", F.lit(source_feed))
        # Negative delays mean the train was early: valid data, keep them.
        .withColumn("is_punctual_arr", F.col("delay_arr_s") < punctual_threshold_s)
        .withColumn("delay_arr_min", F.round(F.col("delay_arr_s") / 60, 1))
        .withColumn("dwell_delta_s", F.col("delay_dep_s") - F.col("delay_arr_s"))
        .withColumn("planned_hour", F.hour("planned_arr_ts"))
    )

def enrich_stations(stations: DataFrame, operational_points: DataFrame) -> DataFrame:
    """Classify dim_station rows and attach coordinates from Infrabel's
    operational-point reference (bronze.operational_point).

    Resolution order: exact PTCAR ID match (`ptcar_no = try_cast(ptcarid AS
    INT)`), then a normalized-name fallback for rows the ID misses. A
    reference name that maps to more than one PTCAR ID is ambiguous and
    excluded from the fallback -- such rows are left unmatched rather than
    guessed at. station_type, latitude and longitude are NULL when unmatched;
    is_passenger is never NULL, falling back to a naming-token heuristic for
    known non-passenger infrastructure.

    Expects `stations` with station_key, station_name, ptcar_no, and
    `operational_points` with ptcarid, longnamedutch, class_en, geo_point_2d.
    Returns station_key, station_type, is_passenger, latitude, longitude.
    """
    ops = operational_points.select(
        F.expr("try_cast(ptcarid AS INT)").alias("op_ptcar_no"),
        F.col("class_en").alias("op_station_type"),
        F.col("longnamedutch").alias("op_longnamedutch"),
        F.col("geo_point_2d").alias("op_geo_point_2d"),
    )

    base = stations.select("station_key", "station_name", "ptcar_no")
    id_joined = base.join(ops, F.col("ptcar_no") == F.col("op_ptcar_no"), "left")

    matched = id_joined.filter(F.col("op_ptcar_no").isNotNull())
    unmatched_by_id = id_joined.filter(F.col("op_ptcar_no").isNull()).select(*base.columns)

    # A reference name usable for the fallback must resolve to exactly one
    # PTCAR ID; ambiguous names are dropped and logged rather than guessed at.
    ops_by_name = ops.withColumn("op_name_key", normalize_station_name("op_longnamedutch"))
    name_fanout = ops_by_name.groupBy("op_name_key").agg(
        F.countDistinct("op_ptcar_no").alias("fanout")
    )
    ambiguous_names = name_fanout.filter(F.col("fanout") > 1).count()
    if ambiguous_names:
        print(
            f"enrich_stations: {ambiguous_names} reference name(s) map to more "
            "than one PTCAR ID; excluded from the name fallback"
        )
    ops_unique_by_name = ops_by_name.join(
        name_fanout.filter(F.col("fanout") == 1).select("op_name_key"), "op_name_key"
    )

    name_joined = (
        unmatched_by_id
        .withColumn("op_name_key", normalize_station_name("station_name"))
        .join(ops_unique_by_name, "op_name_key", "left")
        .drop("op_name_key")
    )

    resolved = matched.unionByName(name_joined)

    name_matched_count = name_joined.filter(F.col("op_ptcar_no").isNotNull()).count()
    unmatched_count = name_joined.filter(F.col("op_ptcar_no").isNull()).count()
    print(
        f"enrich_stations: {matched.count()} ID-matched, "
        f"{name_matched_count} name-matched, {unmatched_count} unmatched"
    )

    name_upper = F.upper(F.col("station_name"))
    has_service_token = F.lit(False)
    for token in SERVICE_INFRA_TOKENS:
        has_service_token = has_service_token | name_upper.contains(token)

    enriched = (
        resolved
        .withColumn("station_type", F.col("op_station_type"))
        .withColumn(
            "latitude",
            F.expr("try_cast(trim(element_at(split(op_geo_point_2d, ','), 1)) AS DOUBLE)"),
        )
        .withColumn(
            "longitude",
            F.expr("try_cast(trim(element_at(split(op_geo_point_2d, ','), 2)) AS DOUBLE)"),
        )
        .withColumn(
            "is_passenger",
            F.when(
                F.col("op_station_type").isNotNull(),
                F.col("op_station_type").isin(*PASSENGER_CLASSES),
            ).otherwise(~has_service_token),
        )
        .select("station_key", "station_type", "is_passenger", "latitude", "longitude")
    )

    passenger_count = enriched.filter(F.col("is_passenger")).count()
    non_passenger_count = enriched.filter(~F.col("is_passenger")).count()
    print(f"enrich_stations: {passenger_count} passenger, {non_passenger_count} non-passenger")

    return enriched


def deduplicate_stop_events(df: DataFrame) -> DataFrame:
    """Deduplicate records by natural key, retaining the latest ingested row."""
    w = Window.partitionBy(*NATURAL_KEY).orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

# The monthly export writes dates as 01JUL2026; the daily feed writes ISO.
MONTHLY_DATE_FORMAT = "ddMMMyyyy"

MONTHLY_DATE_COLUMNS = [
    "DATDEP",
    "PLANNED_DATE_ARR",
    "PLANNED_DATE_DEP",
    "REAL_DATE_ARR",
    "REAL_DATE_DEP",
]


def _try_to_date(col: str, date_format: str):
    # SQL expression sidesteps PySpark signature drift on try_to_date's format argument.
    return F.expr(f"try_to_date({col}, '{date_format}')")


def normalize_monthly_dates(raw: DataFrame, date_format: str = MONTHLY_DATE_FORMAT) -> DataFrame:
    """Rewrite monthly date literals as ISO strings so Silver sees one input contract."""
    df = raw
    for col in MONTHLY_DATE_COLUMNS:
        df = df.withColumn(col, F.date_format(_try_to_date(col, date_format), "yyyy-MM-dd"))
    return df


def count_unparsed_dates(
    raw: DataFrame, date_format: str = MONTHLY_DATE_FORMAT
) -> dict[str, int]:
    """Count non-null date literals that fail to parse.

    Month abbreviations resolve against the session locale, so a historical file
    written in another language would silently normalize to NULL. Callers abort
    on a non-zero count rather than ingest unparseable dates.
    """
    exprs = [
        F.count(
            F.when(F.col(c).isNotNull() & _try_to_date(c, date_format).isNull(), True)
        ).alias(c)
        for c in MONTHLY_DATE_COLUMNS
    ]
    row = raw.agg(*exprs).first()
    return {c: row[c] for c in MONTHLY_DATE_COLUMNS}