import json
import sys
import subprocess
import re
from datetime import datetime, timezone

from google.transit import gtfs_realtime_pb2

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType, DateType
)
from pyspark.sql.functions import (
    col, trim, coalesce, from_unixtime, from_utc_timestamp, to_date, hour,
    when, round as spark_round, abs as spark_abs, min as spark_min, max as spark_max, avg as spark_avg, count as spark_count,
    lit, current_timestamp
)


def parse_timestamp_from_filename(filename: str):
    """Extract UTC datetime from filename format: trip_updates_YYYYMMDD_HHMMSS.pb"""
    match = re.search(r"trip_updates_(\d{8})_(\d{6})\.pb", filename)
    if match:
        date_part, time_part = match.group(1), match.group(2)
        try:
            dt = datetime.strptime(f"{date_part}_{time_part}", "%Y%m%d_%H%M%S")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def run_silver_realtime_pipeline(spark: SparkSession, force_reprocess_all: bool = False):
    storage_account = "stransitanalytics"
    bronze_base = f"abfss://bronze@{storage_account}.dfs.core.windows.net"
    silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"
    control_path = f"{silver_base}/control/processed_realtime_files"
    output_path = f"{silver_base}/realtime_trip_updates"

    print("=" * 80)
    print("INCREMENTAL REALTIME SILVER PIPELINE (realtime_trip_updates)")
    print("=" * 80)

    # -------------------------------------------------------------------------
    # 1. Discover Bronze Snapshots
    # -------------------------------------------------------------------------
    bronze_rt_path = f"{bronze_base}/gtfs_realtime/trip_updates"
    print(f"\n[1/6] Scanning Bronze for timestamped snapshots at: {bronze_rt_path}")

    # Use binaryFile reader with recursive lookup
    bin_df = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.pb")
        .load(bronze_rt_path)
    )

    # Collect list of candidate files (filtering for timestamped files: trip_updates_*.pb)
    candidate_rows = bin_df.select("path", "modificationTime", "length", "content").collect()
    candidate_files = []
    for row in candidate_rows:
        file_path = row["path"]
        file_name = file_path.split("/")[-1]
        # Ignore legacy non-timestamped single snapshot if timestamped ones exist
        if "trip_updates_" in file_name and file_name.endswith(".pb"):
            candidate_files.append({
                "path": file_path,
                "file_name": file_name,
                "length": row["length"],
                "content": row["content"]
            })

    total_discovered = len(candidate_files)
    print(f"Total timestamped snapshots discovered: {total_discovered}")

    # -------------------------------------------------------------------------
    # 2. Check Control State for Already Processed Files
    # -------------------------------------------------------------------------
    already_processed = set()
    if not force_reprocess_all:
        try:
            ctrl_df = spark.read.parquet(control_path)
            already_processed = set([r[0] for r in ctrl_df.select("file_name").collect()])
            print(f"Already processed snapshots recorded in control state: {len(already_processed)}")
        except Exception:
            print("No existing control state found. Initializing new ingestion state.")
            already_processed = set()
    else:
        print("Force reprocess requested: ignoring previous control state.")

    # Identify newly arrived files to process
    files_to_process = [f for f in candidate_files if f["file_name"] not in already_processed]
    print(f"New snapshots to process in this run: {len(files_to_process)}")

    # Idempotency check: if no new files exist and Silver already exists, exit cleanly
    if len(files_to_process) == 0:
        print("\n>>> IDEMPOTENCY CHECK: No new Bronze snapshots to process.")
        print(">>> Silver dataset remains unchanged. 0 duplicate records added.")
        current_silver_rows = spark.read.parquet(output_path).count()
        return {
            "status": "IDEMPOTENT_NOOP",
            "files_discovered": total_discovered,
            "files_processed": 0,
            "new_records": 0,
            "silver_row_count": current_silver_rows,
            "message": "Pipeline ran idempotently with no new files."
        }

    # -------------------------------------------------------------------------
    # 3. Parse Protobuf Records from New Snapshots
    # -------------------------------------------------------------------------
    print(f"\n[2/6] Parsing {len(files_to_process)} Protobuf snapshot(s)...")
    records = []
    file_metadata_records = []

    for f in sorted(files_to_process, key=lambda x: x["file_name"]):
        file_name = f["file_name"]
        file_path = f["path"]

        # Parse observed_at from timestamp in filename
        observed_at = parse_timestamp_from_filename(file_name)

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(bytes(f["content"]))
        feed_timestamp = int(feed.header.timestamp)

        # Fallback if observed_at could not be parsed from filename
        if not observed_at:
            observed_at = datetime.fromtimestamp(feed_timestamp, tz=timezone.utc)

        snapshot_record_count = 0
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            trip = entity.trip_update.trip
            trip_id = str(trip.trip_id).strip() if trip.trip_id else None
            route_id = str(trip.route_id).strip() if trip.route_id else None
            direction_id = str(trip.direction_id).strip() if trip.HasField("direction_id") else None

            for stop_update in entity.trip_update.stop_time_update:
                arrival_delay = None
                departure_delay = None

                if stop_update.HasField("arrival") and stop_update.arrival.HasField("delay"):
                    arrival_delay = float(stop_update.arrival.delay)
                if stop_update.HasField("departure") and stop_update.departure.HasField("delay"):
                    departure_delay = float(stop_update.departure.delay)

                stop_sequence = int(stop_update.stop_sequence) if stop_update.HasField("stop_sequence") else None
                stop_id = str(stop_update.stop_id).strip() if stop_update.stop_id else None

                records.append((
                    observed_at,
                    feed_timestamp,
                    trip_id,
                    route_id,
                    direction_id,
                    stop_id,
                    stop_sequence,
                    arrival_delay,
                    departure_delay,
                    file_name
                ))
                snapshot_record_count += 1

        print(f"  Processed {file_name}: extracted {snapshot_record_count:,} records (feed timestamp: {feed_timestamp})")
        file_metadata_records.append((
            file_name,
            file_path,
            datetime.now(timezone.utc),
            snapshot_record_count,
            feed_timestamp,
            observed_at
        ))

    print(f"Total new raw observation records extracted: {len(records):,}")

    # -------------------------------------------------------------------------
    # 4. Standardize Schema, Types & Delay Features
    # -------------------------------------------------------------------------
    print("\n[3/6] Standardizing schema and engineering delay features...")

    raw_schema = StructType([
        StructField("observed_at", TimestampType(), True),
        StructField("feed_timestamp", LongType(), True),
        StructField("trip_id", StringType(), True),
        StructField("route_id_rt", StringType(), True),
        StructField("direction_id_rt", StringType(), True),
        StructField("stop_id", StringType(), True),
        StructField("stop_sequence", IntegerType(), True),
        StructField("arrival_delay_seconds", DoubleType(), True),
        StructField("departure_delay_seconds", DoubleType(), True),
        StructField("source_file", StringType(), True)
    ])

    new_raw_df = spark.createDataFrame(records, raw_schema)

    # Time dimensions (Vancouver local time)
    new_df = (
        new_raw_df
        .withColumn("observed_at_vancouver", from_utc_timestamp(col("observed_at"), "America/Vancouver"))
        .withColumn("service_date", to_date(col("observed_at_vancouver")))
        .withColumn("observation_hour", hour(col("observed_at_vancouver")))
        .drop("observed_at_vancouver")
    )

    # Delay engineering
    new_df = new_df.withColumn(
        "effective_delay_seconds",
        coalesce(col("arrival_delay_seconds"), col("departure_delay_seconds"))
    )

    new_df = (
        new_df
        .withColumn("arrival_delay_minutes", spark_round(col("arrival_delay_seconds") / 60.0, 2))
        .withColumn("departure_delay_minutes", spark_round(col("departure_delay_seconds") / 60.0, 2))
        .withColumn("effective_delay_minutes", spark_round(col("effective_delay_seconds") / 60.0, 2))
    )

    # Mutually exclusive delay classifications
    delay_col = col("effective_delay_seconds")
    new_df = new_df.withColumn(
        "delay_category",
        when(delay_col.isNull(), "Unknown")
        .when(delay_col < -60, "Early")
        .when((delay_col >= -60) & (delay_col <= 60), "On Time")
        .when((delay_col > 60) & (delay_col <= 300), "Delayed")
        .when(delay_col > 300, "Severely Delayed")
        .otherwise("Unknown")
    )

    new_df = (
        new_df
        .withColumn("is_early", when(delay_col < -60, 1).otherwise(0))
        .withColumn("is_on_time", when((delay_col >= -60) & (delay_col <= 60), 1).otherwise(0))
        .withColumn("is_delayed", when((delay_col > 60) & (delay_col <= 300), 1).otherwise(0))
        .withColumn("is_severely_delayed", when(delay_col > 300, 1).otherwise(0))
        .withColumn("on_time_flag", when((delay_col >= -60) & (delay_col <= 60), 1).otherwise(0))
        .withColumn("extreme_delay_flag", when(spark_abs(delay_col) > 3600, 1).otherwise(0))
    )

    # -------------------------------------------------------------------------
    # 5. Enrich with Static Silver Dimensions
    # -------------------------------------------------------------------------
    print("\n[4/6] Conflating with Silver static dimensions...")
    dim_trip = spark.read.parquet(f"{silver_base}/dim_trip")
    dim_route = spark.read.parquet(f"{silver_base}/dim_route")
    dim_stop = spark.read.parquet(f"{silver_base}/dim_stop")

    dim_trip_sub = dim_trip.select(
        col("trip_id"),
        col("route_id").alias("trip_route_id"),
        col("direction_id").alias("trip_direction_id"),
        col("direction"),
        col("trip_headsign"),
        col("service_id")
    )

    enriched = new_df.join(dim_trip_sub, on="trip_id", how="left")

    # Resolve route_id and direction_id
    enriched = (
        enriched
        .withColumn("route_id", coalesce(col("route_id_rt"), col("trip_route_id")))
        .withColumn("direction_id", coalesce(col("direction_id_rt"), col("trip_direction_id")))
        .drop("route_id_rt", "direction_id_rt", "trip_route_id", "trip_direction_id")
    )

    # Join dim_route
    dim_route_sub = dim_route.select(
        col("route_id"),
        col("route_short_name"),
        col("route_long_name")
    )
    enriched = enriched.join(dim_route_sub, on="route_id", how="left")

    # Join dim_stop
    dim_stop_sub = dim_stop.select(
        col("stop_id"),
        col("stop_name"),
        col("stop_lat"),
        col("stop_lon")
    )
    enriched = enriched.join(dim_stop_sub, on="stop_id", how="left")

    # Deduplicate within batch on grain: observed_at + trip_id + stop_id + stop_sequence
    enriched = enriched.dropDuplicates(["observed_at", "trip_id", "stop_id", "stop_sequence"])

    final_columns = [
        "service_date",
        "observed_at",
        "observation_hour",
        "feed_timestamp",
        "trip_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "direction_id",
        "direction",
        "trip_headsign",
        "service_id",
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "stop_sequence",
        "arrival_delay_seconds",
        "departure_delay_seconds",
        "effective_delay_seconds",
        "arrival_delay_minutes",
        "departure_delay_minutes",
        "effective_delay_minutes",
        "delay_category",
        "is_early",
        "is_on_time",
        "is_delayed",
        "is_severely_delayed",
        "on_time_flag",
        "extreme_delay_flag",
        "source_file"
    ]

    final_new_df = enriched.select(final_columns)

    # -------------------------------------------------------------------------
    # 6. Write Silver Table & Update Control State
    # -------------------------------------------------------------------------
    print(f"\n[5/6] Writing Silver realtime dataset to: {output_path}")

    # Determine write mode: overwrite if first run/force reprocess, append if incremental
    silver_exists = False
    if not force_reprocess_all:
        try:
            spark.read.parquet(output_path).take(1)
            silver_exists = True
        except Exception:
            silver_exists = False

    if silver_exists and len(already_processed) > 0:
        print("Existing Silver table found. Appending new records partitioned by service_date...")
        final_new_df.write.mode("append").partitionBy("service_date").parquet(output_path)
    else:
        print("Creating initial Silver table partitioned by service_date...")
        final_new_df.write.mode("overwrite").partitionBy("service_date").parquet(output_path)

    # Update control state
    ctrl_schema = StructType([
        StructField("file_name", StringType(), False),
        StructField("file_path", StringType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("records_extracted", LongType(), True),
        StructField("feed_timestamp", LongType(), True),
        StructField("observed_at", TimestampType(), True)
    ])
    new_ctrl_df = spark.createDataFrame(file_metadata_records, ctrl_schema)
    if silver_exists and len(already_processed) > 0:
        new_ctrl_df.write.mode("append").parquet(control_path)
    else:
        new_ctrl_df.write.mode("overwrite").parquet(control_path)
    print(f"Updated control state table at: {control_path}")

    # -------------------------------------------------------------------------
    # 7. Validation & Data Quality Reporting
    # -------------------------------------------------------------------------
    print("\n[6/6] Running data quality validations over Silver realtime dataset...")
    silver_rt = spark.read.parquet(output_path)

    total_rows = silver_rt.count()
    null_trip = silver_rt.filter(col("trip_id").isNull()).count()
    null_route = silver_rt.filter(col("route_id").isNull()).count()
    null_stop = silver_rt.filter(col("stop_id").isNull()).count()

    # Observation grain duplicates
    grain_cols = ["observed_at", "trip_id", "stop_id", "stop_sequence"]
    duplicate_grain = total_rows - silver_rt.select(*grain_cols).distinct().count()

    # Dimension matching
    unmatched_trips = silver_rt.filter(col("direction").isNull() & col("trip_headsign").isNull()).count()
    matched_trips = total_rows - unmatched_trips
    unmatched_routes = silver_rt.filter(col("route_short_name").isNull()).count()
    unmatched_stops = silver_rt.filter(col("stop_name").isNull()).count()

    # Delay nulls & statistics
    null_delays = silver_rt.filter(col("effective_delay_seconds").isNull()).count()
    extreme_delays = silver_rt.filter(col("extreme_delay_flag") == 1).count()

    cat_df = silver_rt.groupBy("delay_category").agg(spark_count("*").alias("count")).toPandas()
    cat_distribution = dict(zip(cat_df["delay_category"], [int(x) for x in cat_df["count"]]))

    time_bounds = silver_rt.select(
        spark_min("observed_at").alias("earliest_observed_at"),
        spark_max("observed_at").alias("latest_observed_at")
    ).toPandas().to_dict(orient="records")[0]

    delay_stats = silver_rt.select(
        spark_min("effective_delay_seconds").alias("min_delay_sec"),
        spark_max("effective_delay_seconds").alias("max_delay_sec"),
        spark_round(spark_avg("effective_delay_seconds"), 1).alias("avg_delay_sec"),
        spark_min("effective_delay_minutes").alias("min_delay_min"),
        spark_max("effective_delay_minutes").alias("max_delay_min"),
        spark_round(spark_avg("effective_delay_minutes"), 2).alias("avg_delay_min")
    ).toPandas().to_dict(orient="records")[0]

    for k, v in delay_stats.items():
        delay_stats[k] = float(v) if v is not None else None

    # Number of snapshots processed in total
    total_snapshots_in_control = spark.read.parquet(control_path).count()

    validation_report = {
        "status": "SUCCESS",
        "table_name": "realtime_trip_updates",
        "total_silver_rows": total_rows,
        "new_records_added": len(records),
        "snapshots_processed_this_run": len(files_to_process),
        "total_snapshots_processed": total_snapshots_in_control,
        "earliest_observed_at": str(time_bounds["earliest_observed_at"]),
        "latest_observed_at": str(time_bounds["latest_observed_at"]),
        "null_counts": {
            "null_trip_id": null_trip,
            "null_route_id": null_route,
            "null_stop_id": null_stop,
            "null_effective_delay": null_delays
        },
        "duplicate_grain_count": duplicate_grain,
        "dimension_matching": {
            "matched_trips": matched_trips,
            "unmatched_trips": unmatched_trips,
            "trip_match_pct": round((matched_trips / total_rows) * 100, 2) if total_rows > 0 else 0,
            "unmatched_routes": unmatched_routes,
            "unmatched_stops": unmatched_stops
        },
        "delay_category_distribution": cat_distribution,
        "delay_statistics": delay_stats,
        "extreme_delay_count": extreme_delays
    }

    report_json = json.dumps(validation_report, indent=2)
    print("\n" + "=" * 80)
    print("REALTIME SILVER VALIDATION REPORT")
    print("=" * 80)
    print(report_json)

    # Write report to Silver container
    spark.createDataFrame([(report_json,)], ["report_json"]).coalesce(1).write.mode("overwrite").text(f"{silver_base}/realtime_validation_report")
    print(f"\nReport persisted to: {silver_base}/realtime_validation_report")

    return validation_report


def main():
    spark = SparkSession.builder.appName("BuildIncrementalSilverRealtime").getOrCreate()
    run_silver_realtime_pipeline(spark, force_reprocess_all=False)


if __name__ == "__main__":
    main()
