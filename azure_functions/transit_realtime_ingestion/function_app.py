import datetime
import logging
import os
import time
import json
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import requests

app = func.FunctionApp()


def run_ingestion_pipeline() -> dict:
    """Core ingestion routine that downloads TransLink GTFS-Realtime protobuf

    and writes it directly to ADLS Gen2 Bronze with partitioned paths.
    """
    start_time = time.time()
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    timestamp_log = utc_now.strftime("%Y-%m-%d %H:%M:%S UTC")
    timestamp_file = utc_now.strftime("%Y%m%d_%H%M%S")

    # Load configuration from environment settings
    api_url = os.environ.get("TRANSLINK_TRIP_UPDATES_URL")
    storage_account = os.environ.get("STORAGE_ACCOUNT_NAME", "stransitanalytics")
    container_name = os.environ.get("BRONZE_CONTAINER", "bronze")
    base_path = os.environ.get("BRONZE_REALTIME_PATH", "gtfs_realtime/trip_updates")

    if not api_url:
        err_msg = "TRANSLINK_TRIP_UPDATES_URL environment variable is not configured."
        logging.error(f"[INGESTION_ERROR] {timestamp_log} - {err_msg}")
        raise ValueError(err_msg)

    # 1. Download raw protobuf from TransLink API
    logging.info(f"[INGESTION_START] {timestamp_log} - Fetching GTFS-RT from {api_url.split('?')[0]}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*"
    }
    try:
        response = requests.get(api_url, headers=headers, timeout=30)
    except requests.exceptions.Timeout as ex:
        logging.error(f"[INGESTION_ERROR] {timestamp_log} - Request timed out after 30 seconds: {ex}")
        raise
    except requests.exceptions.RequestException as ex:
        logging.error(f"[INGESTION_ERROR] {timestamp_log} - Network/HTTP error: {ex}")
        raise

    api_status = response.status_code
    if api_status != 200:
        err_msg = f"TransLink API returned non-200 HTTP status: {api_status}. Response: {response.text[:200]}"
        logging.error(f"[INGESTION_ERROR] {timestamp_log} - {err_msg}")
        raise RuntimeError(err_msg)

    # 2. Validate payload
    payload = response.content
    payload_size = len(payload)
    if payload_size == 0:
        err_msg = "Received empty payload (0 bytes) from TransLink API."
        logging.error(f"[INGESTION_ERROR] {timestamp_log} - {err_msg}")
        raise ValueError(err_msg)

    # 3. Construct timestamped partitioned destination path in Bronze
    # Format: bronze/gtfs_realtime/trip_updates/year=YYYY/month=MM/day=DD/hour=HH/trip_updates_YYYYMMDD_HHMMSS.pb
    partition_path = (
        f"{base_path}/year={utc_now.year}/month={utc_now.month:02d}/"
        f"day={utc_now.day:02d}/hour={utc_now.hour:02d}"
    )
    blob_name = f"{partition_path}/trip_updates_{timestamp_file}.pb"
    adls_uri = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{blob_name}"

    # 4. Upload raw protobuf bytes to ADLS Gen2 using Managed Identity (or local Azure CLI credential)
    account_url = f"https://{storage_account}.blob.core.windows.net"
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Idempotent write: overwrite=False prevents overwriting an existing snapshot
    blob_client.upload_blob(payload, overwrite=False)

    duration = round(time.time() - start_time, 2)

    # 5. Structured logging
    log_summary = (
        f"\n============================================================\n"
        f"TRANSLINK REALTIME INGESTION LOG\n"
        f"============================================================\n"
        f"Timestamp:          {timestamp_log}\n"
        f"API status:         {api_status}\n"
        f"Payload size:       {payload_size:,} bytes\n"
        f"Destination ADLS:   {adls_uri}\n"
        f"Duration:           {duration}s\n"
        f"Status:             SUCCESS\n"
        f"============================================================"
    )
    logging.info(log_summary)

    return {
        "status": "SUCCESS",
        "timestamp": timestamp_log,
        "api_status": api_status,
        "payload_size_bytes": payload_size,
        "destination_path": adls_uri,
        "blob_name": blob_name,
        "duration_seconds": duration,
    }


@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
def ingest_translink_gtfs_realtime(myTimer: func.TimerRequest) -> None:
    """Timer trigger function running every 5 minutes."""
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if myTimer.past_due:
        logging.warning(f"Timer trigger is past due at {utc_timestamp}!")

    logging.info(f"Python timer trigger function started at {utc_timestamp}")
    try:
        result = run_ingestion_pipeline()
        logging.info(f"Ingestion completed successfully: {result['blob_name']}")
    except Exception as e:
        logging.error(f"Ingestion execution failed: {e}", exc_info=True)
        # We do not swallow the error silently, allowing Azure Functions telemetry to record failure
        raise


@app.route(route="manual_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def manual_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger allowing on-demand testing and manual execution."""
    logging.info("Manual ingestion trigger received via HTTP.")
    try:
        result = run_ingestion_pipeline()
        return func.HttpResponse(
            body=json.dumps(result, indent=2),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Manual ingestion failed: {e}", exc_info=True)
        return func.HttpResponse(
            body=json.dumps({"status": "FAILED", "error": str(e)}, indent=2),
            status_code=500,
            mimetype="application/json"
        )
