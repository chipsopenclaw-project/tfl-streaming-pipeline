"""
Step 2: TfL API Explorer
Local script to fetch and inspect arrival data.
Run: python3 lambda/fetch.py
"""

import json
import urllib.request
from datetime import datetime, timezone

TFL_BASE_URL = "https://api.tfl.gov.uk"
LINES = ["central", "jubilee", "northern"]

HEADERS = {
    "User-Agent": "tfl-streaming-pipeline/1.0",
    "Accept": "application/json",
}


def fetch_arrivals(line_id: str) -> list:
    url = f"{TFL_BASE_URL}/Line/{line_id}/Arrivals"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read())


def extract_fields(record: dict, line_id: str) -> dict:
    """Keep only the fields we need for Bronze layer."""
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


def main():
    all_records = []

    for line in LINES:
        print(f"\n--- Fetching line: {line} ---")
        try:
            raw = fetch_arrivals(line)
            records = [extract_fields(r, line) for r in raw]
            all_records.extend(records)
            print(f"  Records fetched : {len(records)}")
            print(f"  Sample record   :")
            print(json.dumps(records[0], indent=4))
        except Exception as e:
            print(f"  ERROR: {e}")

    print(f"\n=== Total records across all lines: {len(all_records)} ===")


if __name__ == "__main__":
    main()
