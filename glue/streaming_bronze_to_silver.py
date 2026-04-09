"""
TfL Streaming Pipeline - Glue STREAMING Job
Bronze (Kinesis Stream) -> Silver (S3 Parquet)

NOTE: This script is designed for production use with Glue Streaming.
Dev environment uses scheduled batch jobs instead due to cost considerations.
To enable: change job Command.Name from 'glueetl' to 'gluestreaming'
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
    "kinesis_stream_arn",
    "silver_bucket",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_PATH = f"s3://{args['silver_bucket']}/arrivals/"

# ── Bronze Schema ─────────────────────────────────────────────────────────────
bronze_schema = StructType([
    StructField("id",               StringType(),  True),
    StructField("vehicle_id",       StringType(),  True),
    StructField("naptan_id",        StringType(),  True),
    StructField("station_name",     StringType(),  True),
    StructField("line_id",          StringType(),  True),
    StructField("line_name",        StringType(),  True),
    StructField("platform_name",    StringType(),  True),
    StructField("direction",        StringType(),  True),
    StructField("towards",          StringType(),  True),
    StructField("current_location", StringType(),  True),
    StructField("destination_name", StringType(),  True),
    StructField("expected_arrival", StringType(),  True),
    StructField("time_to_station",  IntegerType(), True),
    StructField("timestamp",        StringType(),  True),
    StructField("ingestion_time",   StringType(),  True),
])

# ── Read from Kinesis Stream (Streaming) ─────────────────────────────────────
kinesis_stream = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData":       "kinesis",
        "streamARN":        args["kinesis_stream_arn"],
        "classification":   "json",
        "startingPosition": "LATEST",
        "inferSchema":      "false",
    },
    format="json",
    format_options={"jsonPath": "$"},
    transformation_ctx="kinesis_stream",
)

# ── Process each micro-batch ──────────────────────────────────────────────────
def process_batch(data_frame, batchId):
    if data_frame.count() == 0:
        return

    # Parse JSON from Kinesis
    df = data_frame.select(
        F.from_json(F.col("$json$"), bronze_schema).alias("data")
    ).select("data.*")

    # Transformations (same as batch job)
    silver_df = (
        df
        .withColumn("expected_arrival",  F.to_timestamp("expected_arrival"))
        .withColumn("timestamp",         F.to_timestamp("timestamp"))
        .withColumn("ingestion_time",    F.to_timestamp("ingestion_time"))
        .withColumn("arrival_date",      F.to_date("expected_arrival"))
        .withColumn("arrival_hour",      F.hour("expected_arrival"))
        .withColumn("station_name",
            F.regexp_replace("station_name", " Underground Station", ""))
        .dropDuplicates(["id", "expected_arrival"])
        .filter(F.col("id").isNotNull())
        .filter(F.col("line_id").isNotNull())
        .filter(F.col("expected_arrival").isNotNull())
        .filter(F.col("time_to_station") >= 0)
    )

    # Write to Silver
    (
        silver_df
        .write
        .mode("append")
        .partitionBy("arrival_date", "line_id")
        .parquet(SILVER_PATH)
    )

    print(f"Batch {batchId}: wrote {silver_df.count()} records to Silver")

# ── Start Streaming ───────────────────────────────────────────────────────────
glueContext.forEachBatch(
    frame=kinesis_stream,
    batch_function=process_batch,
    options={
        "windowSize":        "60 seconds",
        "checkpointLocation": f"s3://{args['silver_bucket']}/_checkpoint/",
    }
)

job.commit()
