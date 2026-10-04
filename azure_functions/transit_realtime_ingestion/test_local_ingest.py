#!/usr/bin/env python3
"""Local test script for TransLink GTFS-Realtime Ingestion.
Tests API connectivity, ADLS Gen2 authentication, timestamped partitioning,
and payload validation without touching existing files.
"""

import os
import sys
from pathlib import Path

# Add function directory to sys.path
sys.path.insert(0, str(Path(__file__).parent))

import function_app

if __name__ == "__main__":
    print("=" * 60)
    print("STARTING LOCAL INGESTION TEST")
    print("=" * 60)

    # Verify environment
    url = os.environ.get("TRANSLINK_TRIP_UPDATES_URL")
    print(f"Target URL: {url}")
    print(f"Storage Account: {os.environ.get('STORAGE_ACCOUNT_NAME', 'stransitanalytics')}")
    print(f"Bronze Container: {os.environ.get('BRONZE_CONTAINER', 'bronze')}")
    print(f"Realtime Path: {os.environ.get('BRONZE_REALTIME_PATH', 'gtfs_realtime/trip_updates')}")
    print("-" * 60)

    try:
        result = function_app.run_ingestion_pipeline()
        print("\nSUCCESS! Ingestion pipeline executed cleanly:")
        for k, v in result.items():
            print(f"  {k}: {v}")
    except Exception as e:
        print(f"\nExecution failed with error: {e}")
        sys.exit(1)
