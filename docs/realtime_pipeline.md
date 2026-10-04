# Realtime Transit Ingestion & Processing Pipeline

## 1. Architecture Overview

The Metro Vancouver Transit Analytics pipeline ingests real-time GTFS feeds from TransLink, preserves immutable raw snapshots in the cloud data lake, and incrementally transforms them into business-ready Silver analytical datasets using Apache Spark on Azure Synapse.

```text
                    TransLink GTFS-RT API
                              │
                              │ HTTP GET (every 5 min)
                              ▼
                    ┌───────────────────┐
                    │  Azure Function   │
                    │    Python 3.11    │
                    │  Timer Trigger    │
                    └─────────┬─────────┘
                              │
                              │ Raw Protobuf (.pb)
                              │ System Managed Identity
                              ▼
                    ┌───────────────────┐
                    │ ADLS Gen2 Bronze  │
                    │ stransitanalytics │
                    │ (Partitioned .pb) │
                    └─────────┬─────────┘
                              │
                              │ Incremental Discovery
                              ▼
                    ┌───────────────────┐
                    │   Synapse Spark   │
                    │    Pool: Small    │
                    └─────────┬─────────┘
                              │
                              │ Delta / Snappy Parquet
                              ▼
                    ┌───────────────────┐
                    │  Realtime Silver  │
                    │   Trip Updates    │
                    └───────────────────┘
```

---

## 2. Component Specifications

### 2.1 TransLink GTFS-Realtime API
* **Endpoint:** `https://gtfsapi.translink.ca/v3/gtfsrealtime`
* **Protocol:** Protocol Buffers (GTFS-RT v2.0/v3.0 specification)
* **Authentication:** API key passed via query parameter (`?apikey=[KEY]`)
* **Headers:** Standard browser / application `User-Agent` required by Azure Application Gateway.
* **Payload:** Binary GTFS-Realtime feed containing active transit vehicle trips, route IDs, stop-time updates, arrival/departure delays, and uncertainty estimates.

### 2.2 Azure Function App (`func-transit-realtime-ingestion`)
* **Hosting Model:** Serverless Linux Consumption Plan
* **Runtime:** Python 3.11 (Azure Functions v4 programming model)
* **Triggers:**
  * **Timer Trigger (`ingest_translink_gtfs_realtime`):** Executes every 5 minutes (`0 */5 * * * *`).
  * **HTTP Trigger (`manual_trigger`):** Allows immediate on-demand invocation for ad-hoc runs.
* **Security & Auth:** System-Assigned Managed Identity (`Storage Blob Data Contributor` on `stransitanalytics`). Zero hardcoded secrets, connection strings, or passwords.
* **Idempotency:** Generates UTC timestamped snapshot files and writes with `overwrite=False` to prevent race conditions.

### 2.3 Bronze Storage & Snapshot Partitioning
* **Storage Account:** `stransitanalytics` (ADLS Gen2 with Hierarchical Namespace enabled)
* **Container:** `bronze`
* **Path Pattern:**
  ```text
  bronze/gtfs_realtime/trip_updates/year=YYYY/month=MM/day=DD/hour=HH/trip_updates_YYYYMMDD_HHMMSS.pb
  ```
* **Format:** Unaltered, raw binary protobuf bytes (`.pb`). Preserves source data fidelity without schema loss.

### 2.4 Synapse Spark Incremental Silver Pipeline
* **Script:** `scripts/build_silver_realtime.py`
* **Notebook:** `notebooks/03_build_silver_realtime.ipynb`
* **Compute:** Azure Synapse Analytics Spark Pool (`sparkpool`, 3 Small nodes &times; 4 vCPUs).
* **Control State:** `abfss://silver@stransitanalytics.dfs.core.windows.net/control/processed_realtime_files`

---

## 3. Ingestion & Transformation Logic

### 3.1 Dynamic Snapshot Discovery
The Spark pipeline uses Spark's native `binaryFile` reader with `recursiveFileLookup=True` and `pathGlobFilter="*.pb"` to discover all timestamped snapshots across any directory partition.

### 3.2 Incremental Control Tracking
1. Reads `control/processed_realtime_files` to identify previously ingested snapshot filenames.
2. Identifies newly arrived snapshots (`files_to_process = candidate_files - already_processed`).
3. If no new snapshots have arrived, the pipeline exits cleanly without duplicating records (**Idempotency Guarantee**).
4. If new snapshots exist, it parses only the delta files and appends them to Silver.

### 3.3 Timestamp Preservation
To enable deep historical time-series analytics, two distinct timestamps are captured:
1. **`feed_timestamp` (Epoch / UTC Timestamp):** When TransLink's dispatch servers generated the realtime prediction.
2. **`observed_at` (UTC Timestamp):** When our automated pipeline captured the snapshot.

### 3.4 Conflation with Static Silver Dimensions
Each realtime stop update is joined against the conformed Silver static dimensional model:
* **`dim_trip`:** Resolves `route_id`, `direction_id`, and enriches with `direction`, `trip_headsign`, and `service_id`.
* **`dim_route`:** Enriches with `route_short_name` (e.g., "99 B-Line") and `route_long_name`.
* **`dim_stop`:** Enriches with `stop_name`, `stop_lat`, and `stop_lon`.

### 3.5 Delay Feature Engineering
* **`effective_delay_seconds`:** Coalesces `arrival_delay_seconds` (preferred) with `departure_delay_seconds`.
* **`effective_delay_minutes`:** Delay in minutes rounded to 2 decimal places.
* **`delay_category`:** Mutually exclusive business classification:
  * **Early:** `< -60` seconds (more than 1 minute early)
  * **On Time:** `[-60, 60]` seconds (&plusmn;1 minute schedule adherence)
  * **Delayed:** `(60, 300]` seconds (1 to 5 minutes late)
  * **Severely Delayed:** `> 300` seconds (more than 5 minutes late)
  * **Unknown:** `NULL` delay observations
* **Flags:**
  * `on_time_flag`: 1 if On Time, 0 otherwise
  * `extreme_delay_flag`: 1 if `abs(delay) > 3600` seconds (retains extreme outliers for data quality inspection rather than silently deleting them).

---

## 4. Realtime Silver Schema

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `service_date` | `DateType` | Vancouver local calendar date (partition key) |
| `observed_at` | `TimestampType` | Ingestion observation timestamp (UTC) |
| `observation_hour` | `IntegerType` | Vancouver local observation hour (0–23) |
| `feed_timestamp` | `LongType` | TransLink feed generation epoch timestamp |
| `trip_id` | `StringType` | GTFS trip identifier |
| `route_id` | `StringType` | GTFS route identifier |
| `route_short_name` | `StringType` | Route display number/code |
| `route_long_name` | `StringType` | Route destination / corridor name |
| `direction_id` | `StringType` | Direction code (0 or 1) |
| `direction` | `StringType` | Cardinal direction name (Eastbound, Westbound, etc.) |
| `trip_headsign` | `StringType` | Scheduled trip headsign |
| `service_id` | `StringType` | Schedule calendar service code |
| `stop_id` | `StringType` | GTFS stop identifier |
| `stop_name` | `StringType` | Physical stop or station name |
| `stop_lat` | `DoubleType` | Stop latitude coordinate |
| `stop_lon` | `DoubleType` | Stop longitude coordinate |
| `stop_sequence` | `IntegerType` | Stop sequence order along the trip |
| `arrival_delay_seconds` | `DoubleType` | Raw arrival delay in seconds |
| `departure_delay_seconds` | `DoubleType` | Raw departure delay in seconds |
| `effective_delay_seconds` | `DoubleType` | Resolved arrival/departure delay |
| `arrival_delay_minutes` | `DoubleType` | Arrival delay in minutes |
| `departure_delay_minutes` | `DoubleType` | Departure delay in minutes |
| `effective_delay_minutes` | `DoubleType` | Resolved delay in minutes |
| `delay_category` | `StringType` | Early, On Time, Delayed, Severely Delayed, Unknown |
| `is_early` | `IntegerType` | Binary indicator for Early (< -60s) |
| `is_on_time` | `IntegerType` | Binary indicator for On Time ([-60s, 60s]) |
| `is_delayed` | `IntegerType` | Binary indicator for Delayed ((60s, 300s]) |
| `is_severely_delayed` | `IntegerType` | Binary indicator for Severely Delayed (> 300s) |
| `on_time_flag` | `IntegerType` | On-time compliance flag |
| `extreme_delay_flag` | `IntegerType` | Flag for delay anomalies (> 1 hour) |
| `source_file` | `StringType` | Name of source Bronze snapshot |

---

## 5. Data Quality & Idempotency Controls

1. **Grain Uniqueness:** Observation grain is strictly defined as `(observed_at, trip_id, stop_id, stop_sequence)`. Deduplication is enforced prior to writing.
2. **Referential Integrity:** 100% of realtime trips match `dim_trip`.
3. **Partitioning:** Partitioned by `service_date` to prevent the "small-file problem" while enabling efficient partition pruning for downstream Gold rollups and SQL queries.
4. **Audit Trail:** Every pipeline run persists a machine-readable summary at `abfss://silver@stransitanalytics.dfs.core.windows.net/realtime_validation_report`.
