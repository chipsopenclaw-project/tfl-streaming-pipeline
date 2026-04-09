"""
TfL Streaming Pipeline - Glue Streaming Job
Bronze (raw JSON) -> Silver (cleaned Parquet)
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ── Job Init ──────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_bucket",
    "silver_bucket",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_PATH = f"s3://{args['bronze_bucket']}/arrivals/"
SILVER_PATH = f"s3://{args['silver_bucket']}/arrivals/"

# ── Bronze Schema ─────────────────────────────────────────────────────────────
bronze_schema = StructType([
    StructField("id",               StringType(),    True),
    StructField("vehicle_id",       StringType(),    True),
    StructField("naptan_id",        StringType(),    True),
    StructField("station_name",     StringType(),    True),
    StructField("line_id",          StringType(),    True),
    StructField("line_name",        StringType(),    True),
    StructField("platform_name",    StringType(),    True),
    StructField("direction",        StringType(),    True),
    StructField("towards",          StringType(),    True),
    StructField("current_location", StringType(),    True),
    StructField("destination_name", StringType(),    True),
    StructField("expected_arrival", StringType(),    True),
    StructField("time_to_station",  IntegerType(),   True),
    StructField("timestamp",        StringType(),    True),
    StructField("ingestion_time",   StringType(),    True),
])

# ── Read Bronze ───────────────────────────────────────────────────────────────
df = spark.read.schema(bronze_schema).json(BRONZE_PATH)

# ── Transformations ───────────────────────────────────────────────────────────
silver_df = (
    df
    # Cast timestamps
    .withColumn("expected_arrival",
        F.to_timestamp("expected_arrival"))
    .withColumn("timestamp",
        F.to_timestamp("timestamp"))
    .withColumn("ingestion_time",
        F.to_timestamp("ingestion_time"))

    # Add partition columns
    .withColumn("arrival_date",
        F.to_date("expected_arrival"))
    .withColumn("arrival_hour",
        F.hour("expected_arrival"))

    # Clean station name (remove "Underground Station" suffix)
    .withColumn("station_name",
        F.regexp_replace("station_name", " Underground Station", ""))

    # Drop duplicates on id + expected_arrival
    .dropDuplicates(["id", "expected_arrival"])

    # Drop nulls on key fields
    .filter(F.col("id").isNotNull())
    .filter(F.col("line_id").isNotNull())
    .filter(F.col("expected_arrival").isNotNull())
    .filter(F.col("time_to_station") >= 0)
)

# ── Write Silver (Parquet, partitioned) ───────────────────────────────────────
(
    silver_df
    .write
    .mode("append")
    .partitionBy("arrival_date", "line_id")
    .parquet(SILVER_PATH)
)

print(f"Silver write complete. Records: {silver_df.count()}")
job.commit()
