import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

COLS = (
    "DATDEP TRAIN_NO RELATION RELATION_DIRECTION TRAIN_SERV PTCAR_NO "
    "PTCAR_LG_NM_NL LINE_NO_DEP LINE_NO_ARR PLANNED_DATE_ARR PLANNED_TIME_ARR "
    "PLANNED_DATE_DEP PLANNED_TIME_DEP REAL_DATE_ARR REAL_TIME_ARR "
    "REAL_DATE_DEP REAL_TIME_DEP DELAY_ARR DELAY_DEP _ingested_at"
).split()

RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in COLS])

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("rail-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )