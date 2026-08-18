# Silver layer transformations

from pyspark.sql import DataFrame, Window
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

def typed_stop_events(raw: DataFrame, punctual_threshold_s: int = 360) -> DataFrame:
    """Cast Bronze raw string records into typed Silver schema and derive metrics."""
    name_key = normalize_station_name("PTCAR_LG_NM_NL")
    
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
        # Negative delays mean the train was early: valid data, keep them.
        .withColumn("is_punctual_arr", F.col("delay_arr_s") < punctual_threshold_s)
        .withColumn("delay_arr_min", F.round(F.col("delay_arr_s") / 60, 1))
        .withColumn("dwell_delta_s", F.col("delay_dep_s") - F.col("delay_arr_s"))
        .withColumn("planned_hour", F.hour("planned_arr_ts"))
    )

def deduplicate_stop_events(df: DataFrame) -> DataFrame:
    """Deduplicate records by natural key, retaining the latest ingested row."""
    w = Window.partitionBy(*NATURAL_KEY).orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

