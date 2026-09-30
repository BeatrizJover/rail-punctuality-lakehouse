# Silver layer transformations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

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

def _match_key(c: Column) -> Column:
    """Alphanumeric-only uppercase key: same station name, any punctuation or spacing.

    Ligatures are expanded before uppercasing rather than after, since
    upper() case-folding œ/æ to their capital form is not guaranteed across
    JVM locales.
    """
    c = F.trim(c)
    c = F.regexp_replace(c, "[œŒ]", "OE")
    c = F.regexp_replace(c, "[æÆ]", "AE")
    c = F.upper(c)
    c = F.translate(c, _ACCENTED, _PLAIN)
    return F.regexp_replace(c, "[^A-Z0-9]", "")

def station_match_key(col: str) -> Column:
    """Name-matching key used to join dim_station against the iRail station list.

    Separate from normalize_station_name, which feeds stop_point_key and must
    not change: this key is punctuation-insensitive (no spaces or hyphens
    survive), which normalize_station_name deliberately is not.
    """
    return _match_key(F.col(col))

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

def enrich_stations(
    stations: DataFrame, operational_points: DataFrame, irail_stations: DataFrame
) -> DataFrame:
    """Classify dim_station rows and attach coordinates from Infrabel's
    operational-point reference (bronze.operational_point).

    station_type resolution order: exact PTCAR ID match (`ptcar_no =
    try_cast(ptcarid AS INT)`), then a normalized-name fallback for rows the
    ID misses. A reference name that maps to more than one PTCAR ID is
    ambiguous and excluded from the fallback -- such rows are left unmatched
    rather than guessed at. station_type, latitude and longitude are NULL
    when unmatched.

    is_passenger comes from the iRail NMBS/SNCB station list instead of
    class_en, which lumps yards and freight points under 'Station'. Pass 1
    matches on station name (station_match_key); pass 2 falls back to
    taftapcode, but only for iRail rows whose code is unique among candidates
    and that pass 1 did not already claim -- iRail's code is known to be
    wrong for a subset of stations, so an ambiguous code proves nothing.
    Candidate iRail rows are Belgian ('be') with a non-empty taf-tap-code;
    rows without one are border points or closed stations.

    Expects `stations` with station_key, station_name, ptcar_no;
    `operational_points` with ptcarid, longnamedutch, class_en, geo_point_2d,
    taftapcode; `irail_stations` with name, alternative-nl, alternative-fr,
    country-code, taf-tap-code.
    Returns station_key, station_type, is_passenger, latitude, longitude.
    """
    ops = operational_points.select(
        F.expr("try_cast(ptcarid AS INT)").alias("op_ptcar_no"),
        F.col("class_en").alias("op_station_type"),
        F.col("longnamedutch").alias("op_longnamedutch"),
        F.col("geo_point_2d").alias("op_geo_point_2d"),
        F.trim(F.col("taftapcode")).alias("op_taftapcode"),
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

    # Belgian iRail rows with a code: rows without one are border points or
    # closed stations and cannot be used as a passenger signal either way.
    irail_candidates = (
        irail_stations
        .filter(
            (F.lower(F.col("country-code")) == "be")
            & F.col("taf-tap-code").isNotNull()
            & (F.trim(F.col("taf-tap-code")) != "")
        )
        .withColumn("irail_row_id", F.monotonically_increasing_id())
        .select(
            "irail_row_id",
            F.trim(F.col("taf-tap-code")).alias("irail_code"),
            F.col("name").alias("irail_name"),
            F.col("alternative-nl").alias("irail_alt_nl"),
            F.col("alternative-fr").alias("irail_alt_fr"),
        )
    )

    # Bilingual names such as "Haren-Sud/Haren-Zuid" carry two valid keys.
    name_candidates = F.filter(
        F.array_distinct(
            F.concat(
                F.array(_match_key(F.col("irail_name"))),
                F.transform(F.split(F.coalesce(F.col("irail_name"), F.lit("")), "/"), _match_key),
                F.array(_match_key(F.col("irail_alt_nl"))),
                F.array(_match_key(F.col("irail_alt_fr"))),
            )
        ),
        lambda k: (k.isNotNull()) & (k != ""),
    )
    irail_names = (
        irail_candidates
        .select("irail_row_id", "irail_code", F.explode(name_candidates).alias("name_key"))
    )

    base_keys = base.withColumn("match_key", station_match_key("station_name"))

    # Pass 1: match by name. A station can only be claimed once; an iRail row
    # claimed by more than one station is logged, not silently kept or dropped.
    pass1_hits = (
        base_keys.join(irail_names, base_keys.match_key == irail_names.name_key, "inner")
        .select("station_key", "irail_row_id")
        .distinct()
    )
    pass1_station_keys = pass1_hits.select("station_key").distinct()
    claimed_row_ids = pass1_hits.select("irail_row_id").distinct()
    multi_claimed_count = (
        pass1_hits.groupBy("irail_row_id")
        .agg(F.countDistinct("station_key").alias("n"))
        .filter(F.col("n") > 1)
        .count()
    )

    # Pass 2: match by code, restricted to codes unique among candidates and
    # not already claimed by name -- iRail's code is known wrong for some rows.
    code_fanout = irail_candidates.groupBy("irail_code").agg(F.count("*").alias("fanout"))
    unique_unclaimed_codes = (
        irail_candidates
        .join(code_fanout.filter(F.col("fanout") == 1).select("irail_code"), "irail_code")
        .join(claimed_row_ids, "irail_row_id", "left_anti")
        .select("irail_code")
        .distinct()
    )
    station_taftapcode = base.join(
        ops.select("op_ptcar_no", "op_taftapcode"), base.ptcar_no == F.col("op_ptcar_no"), "left"
    ).select("station_key", "op_taftapcode")
    pass2_station_keys = (
        station_taftapcode
        .join(
            unique_unclaimed_codes,
            station_taftapcode.op_taftapcode == unique_unclaimed_codes.irail_code,
            "inner",
        )
        .join(pass1_station_keys, "station_key", "left_anti")
        .select("station_key")
        .distinct()
    )

    print(
        f"enrich_stations: is_passenger {pass1_station_keys.count()} pass-1 name match(es), "
        f"{pass2_station_keys.count()} pass-2 code match(es), "
        f"{multi_claimed_count} iRail row(s) claimed by more than one station"
    )

    passenger_station_keys = pass1_station_keys.unionByName(pass2_station_keys).distinct()

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
        .join(
            passenger_station_keys.withColumn("is_passenger", F.lit(True)),
            "station_key",
            "left",
        )
        .withColumn("is_passenger", F.coalesce(F.col("is_passenger"), F.lit(False)))
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