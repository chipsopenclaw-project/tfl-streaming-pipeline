"""
TfL Streaming Pipeline - Glue Job
Silver (cleaned Parquet) -> Gold (aggregated analytics tables)
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Job Init ──────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_bucket",
    "gold_bucket",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_PATH = f"s3://{args['silver_bucket']}/arrivals/"
GOLD_PATH   = f"s3://{args['gold_bucket']}"

# ── Read Silver ───────────────────────────────────────────────────────────────
df = spark.read.parquet(SILVER_PATH)

# ── Gold Table 1: line_performance ────────────────────────────────────────────
# Average / min / max delay per line per day
line_performance = (
    df
    .groupBy("line_id", "line_name", "arrival_date")
    .agg(
        F.avg("time_to_station").alias("avg_wait_seconds"),
        F.min("time_to_station").alias("min_wait_seconds"),
        F.max("time_to_station").alias("max_wait_seconds"),
        F.count("id").alias("record_count"),
    )
    .withColumn("avg_wait_minutes",
        F.round(F.col("avg_wait_seconds") / 60, 2))
)

(
    line_performance
    .write
    .mode("overwrite")
    .partitionBy("arrival_date")
    .parquet(f"{GOLD_PATH}/line_performance/")
)
print(f"Gold line_performance complete. Records: {line_performance.count()}")

# ── Gold Table 2: station_arrivals ────────────────────────────────────────────
# Arrival count by station and hour
station_arrivals = (
    df
    .groupBy("station_name", "naptan_id", "arrival_date", "arrival_hour", "line_id")
    .agg(
        F.count("id").alias("arrival_count"),
        F.avg("time_to_station").alias("avg_wait_seconds"),
    )
    .orderBy("arrival_count", ascending=False)
)

(
    station_arrivals
    .write
    .mode("overwrite")
    .partitionBy("arrival_date")
    .parquet(f"{GOLD_PATH}/station_arrivals/")
)
print(f"Gold station_arrivals complete. Records: {station_arrivals.count()}")

# ── Gold Table 3: current_wait_times ─────────────────────────────────────────
# Latest wait time per station (most recent record per station)
window = Window.partitionBy("station_name", "line_id").orderBy(F.desc("expected_arrival"))

current_wait_times = (
    df
    .withColumn("rank", F.row_number().over(window))
    .filter(F.col("rank") == 1)
    .select(
        "station_name",
        "naptan_id",
        "line_id",
        "line_name",
        "platform_name",
        "towards",
        "time_to_station",
        "expected_arrival",
        "current_location",
    )
    .withColumn("wait_minutes",
        F.round(F.col("time_to_station") / 60, 1))
)

(
    current_wait_times
    .write
    .mode("overwrite")
    .parquet(f"{GOLD_PATH}/current_wait_times/")
)
print(f"Gold current_wait_times complete. Records: {current_wait_times.count()}")

job.commit()
