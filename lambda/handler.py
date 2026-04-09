"""
TfL Streaming Pipeline - Lambda Handler
Fetches TfL arrivals and publishes each record to Kinesis Data Stream.
"""

import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TFL_BASE_URL = "https://api.tfl.gov.uk"
HEADERS = {
    "User-Agent": "tfl-streaming-pipeline/1.0",
    "Accept": "application/json",
}

kinesis = boto3.client("kinesis", region_name="eu-west-2")
STREAM_NAME = os.environ["KINESIS_STREAM_NAME"]
TFL_LINES = os.environ["TFL_LINES"].split(",")


def fetch_arrivals(line_id: str) -> list:
    url = f"{TFL_BASE_URL}/Line/{line_id}/Arrivals"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read())


def extract_fields(record: dict, line_id: str) -> dict:
    return {
        "id":               record.get("id"),
        "vehicle_id":       record.get("vehicleId"),
        "naptan_id":        record.get("naptanId"),
        "station_name":     record.get("stationName"),
        "line_id":          line_id,
        "line_name":        record.get("lineName"),
        "platform_name":    record.get("platformName"),
        "direction":        record.get("direction", ""),
        "towards":          record.get("towards", ""),
        "current_location": record.get("currentLocation", ""),
        "destination_name": record.get("destinationName", ""),
        "expected_arrival": record.get("expectedArrival"),
        "time_to_station":  record.get("timeToStation"),
        "timestamp":        record.get("timestamp"),
        "ingestion_time":   datetime.now(timezone.utc).isoformat(),
    }


def publish_to_kinesis(records: list) -> dict:
    entries = [
        {
            "Data": json.dumps(r).encode("utf-8"),
            "PartitionKey": r["line_id"],
        }
        for r in records
    ]

    # Kinesis PutRecords accepts max 500 records per call
    success_count = 0
    error_count = 0

    for i in range(0, len(entries), 500):
        batch = entries[i:i + 500]
        response = kinesis.put_records(
            StreamName=STREAM_NAME,
            Records=batch,
        )
        error_count += response.get("FailedRecordCount", 0)
        success_count += len(batch) - response.get("FailedRecordCount", 0)

    return {"success": success_count, "errors": error_count}


def lambda_handler(event, context):
    logger.info("TfL fetcher started")
    total_success = 0
    total_errors = 0

    for line in TFL_LINES:
        try:
            raw = fetch_arrivals(line)
            records = [extract_fields(r, line) for r in raw]
            result = publish_to_kinesis(records)
            total_success += result["success"]
            total_errors += result["errors"]
            logger.info(f"Line {line}: fetched={len(records)}, published={result['success']}, errors={result['errors']}")
        except Exception as e:
            logger.error(f"Line {line} failed: {e}")
            total_errors += 1

    logger.info(f"Done. Total published={total_success}, errors={total_errors}")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "published": total_success,
            "errors": total_errors,
        })
    }
