import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

def main():
    spark = SparkSession.builder.appName("BuildSilverStaticGTFS").getOrCreate()
    
    storage_account = "stransitanalytics"
    bronze_base = f"abfss://bronze@{storage_account}.dfs.core.windows.net/gtfs_static"
    silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"
    
    print("=" * 70)
    print("STEP 2A: BUILDING SILVER STATIC TABLES IN ADLS GEN2")
    print("=" * 70)
    
    # -------------------------------------------------------------------------
    # 1. Build dim_route
    # -------------------------------------------------------------------------
    print("\n[1/4] Processing dim_route...")
    routes_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{bronze_base}/routes.txt")
    )
    
    dim_route = (
        routes_raw
        .select(
            trim(col("route_id").cast("string")).alias("route_id"),
            trim(col("route_short_name").cast("string")).alias("route_short_name"),
            trim(col("route_long_name").cast("string")).alias("route_long_name"),
            col("route_type").cast("int").alias("route_type")
        )
        .dropDuplicates(["route_id"])
    )
    
    dim_route_path = f"{silver_base}/dim_route"
    dim_route.write.mode("overwrite").parquet(dim_route_path)
    print(f"dim_route written to: {dim_route_path}")
    
    # -------------------------------------------------------------------------
    # 2. Build dim_stop
    # -------------------------------------------------------------------------
    print("\n[2/4] Processing dim_stop...")
    stops_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{bronze_base}/stops.txt")
    )
    
    dim_stop = (
        stops_raw
        .select(
            trim(col("stop_id").cast("string")).alias("stop_id"),
            trim(col("stop_code").cast("string")).alias("stop_code"),
            trim(col("stop_name").cast("string")).alias("stop_name"),
            col("stop_lat").cast("double").alias("stop_lat"),
            col("stop_lon").cast("double").alias("stop_lon"),
            trim(col("zone_id").cast("string")).alias("zone_id"),
            col("wheelchair_boarding").cast("int").alias("wheelchair_boarding")
        )
        .dropDuplicates(["stop_id"])
    )
    
    dim_stop_path = f"{silver_base}/dim_stop"
    dim_stop.write.mode("overwrite").parquet(dim_stop_path)
    print(f"dim_stop written to: {dim_stop_path}")
    
    # -------------------------------------------------------------------------
    # 3. Build dim_trip (Enriched with directions.txt)
    # -------------------------------------------------------------------------
    print("\n[3/4] Processing dim_trip...")
    trips_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{bronze_base}/trips.txt")
    )
    
    directions_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{bronze_base}/directions.txt")
    )
    
    directions_lookup = (
        directions_raw
        .select(
            trim(col("route_id").cast("string")).alias("route_id"),
            trim(col("direction_id").cast("string")).alias("direction_id"),
            trim(col("direction").cast("string")).alias("direction")
        )
        .dropDuplicates(["route_id", "direction_id"])
    )
    
    trips_clean = (
        trips_raw
        .select(
            trim(col("trip_id").cast("string")).alias("trip_id"),
            trim(col("route_id").cast("string")).alias("route_id"),
            trim(col("service_id").cast("string")).alias("service_id"),
            trim(col("direction_id").cast("string")).alias("direction_id"),
            trim(col("trip_headsign").cast("string")).alias("trip_headsign"),
            trim(col("shape_id").cast("string")).alias("shape_id")
        )
        .dropDuplicates(["trip_id"])
    )
    
    dim_trip = (
        trips_clean
        .join(directions_lookup, on=["route_id", "direction_id"], how="left")
        .select(
            "trip_id",
            "route_id",
            "service_id",
            "direction_id",
            "direction",
            "trip_headsign",
            "shape_id"
        )
    )
    
    dim_trip_path = f"{silver_base}/dim_trip"
    dim_trip.write.mode("overwrite").parquet(dim_trip_path)
    print(f"dim_trip written to: {dim_trip_path}")
    
    # -------------------------------------------------------------------------
    # 4. Build fact_scheduled_stop (Enriched with route_id from trips)
    # -------------------------------------------------------------------------
    print("\n[4/4] Processing fact_scheduled_stop...")
    stop_times_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{bronze_base}/stop_times.txt")
    )
    
    trip_route_lookup = trips_clean.select("trip_id", "route_id")
    
    fact_scheduled_stop = (
        stop_times_raw
        .select(
            trim(col("trip_id").cast("string")).alias("trip_id"),
            trim(col("stop_id").cast("string")).alias("stop_id"),
            col("stop_sequence").cast("int").alias("stop_sequence"),
            trim(col("arrival_time").cast("string")).alias("arrival_time"),
            trim(col("departure_time").cast("string")).alias("departure_time"),
            col("pickup_type").cast("int").alias("pickup_type"),
            col("drop_off_type").cast("int").alias("drop_off_type")
        )
        .join(trip_route_lookup, on="trip_id", how="left")
        .select(
            "trip_id",
            "route_id",
            "stop_id",
            "stop_sequence",
            "arrival_time",
            "departure_time",
            "pickup_type",
            "drop_off_type"
        )
    )
    
    fact_scheduled_stop_path = f"{silver_base}/fact_scheduled_stop"
    fact_scheduled_stop.write.mode("overwrite").parquet(fact_scheduled_stop_path)
    print(f"fact_scheduled_stop written to: {fact_scheduled_stop_path}")
    
    # =========================================================================
    # VALIDATION CHECKS (Read back from Parquet)
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 2A: VALIDATING SILVER PARQUET OUTPUTS")
    print("=" * 70)
    
    silver_routes = spark.read.parquet(dim_route_path)
    silver_stops = spark.read.parquet(dim_stop_path)
    silver_trips = spark.read.parquet(dim_trip_path)
    silver_facts = spark.read.parquet(fact_scheduled_stop_path)
    
    # 1. Row counts
    cnt_routes = silver_routes.count()
    cnt_stops = silver_stops.count()
    cnt_trips = silver_trips.count()
    cnt_facts = silver_facts.count()
    
    # 2. Null counts on important keys
    null_route_pk = silver_routes.filter(col("route_id").isNull()).count()
    null_stop_pk = silver_stops.filter(col("stop_id").isNull()).count()
    null_trip_pk = silver_trips.filter(col("trip_id").isNull()).count()
    null_trip_fk_route = silver_trips.filter(col("route_id").isNull()).count()
    null_fact_pk_trip = silver_facts.filter(col("trip_id").isNull()).count()
    null_fact_fk_route = silver_facts.filter(col("route_id").isNull()).count()
    null_fact_fk_stop = silver_facts.filter(col("stop_id").isNull()).count()
    null_fact_seq = silver_facts.filter(col("stop_sequence").isNull()).count()
    
    # 3. Duplicate Primary Keys
    dup_routes = cnt_routes - silver_routes.select("route_id").distinct().count()
    dup_stops = cnt_stops - silver_stops.select("stop_id").distinct().count()
    dup_trips = cnt_trips - silver_trips.select("trip_id").distinct().count()
    
    # 4. Foreign Key / Orphan Checks
    # dim_trip.route_id -> dim_route.route_id
    orphan_trip_route = silver_trips.join(silver_routes, on="route_id", how="left_anti").count()
    
    # fact_scheduled_stop.trip_id -> dim_trip.trip_id
    orphan_fact_trip = silver_facts.join(silver_trips, on="trip_id", how="left_anti").count()
    
    # fact_scheduled_stop.route_id -> dim_route.route_id
    orphan_fact_route = silver_facts.join(silver_routes, on="route_id", how="left_anti").count()
    
    # fact_scheduled_stop.stop_id -> dim_stop.stop_id
    orphan_fact_stop = silver_facts.join(silver_stops, on="stop_id", how="left_anti").count()
    
    validation_summary = {
        "status": "SUCCESS",
        "row_counts": {
            "dim_route": cnt_routes,
            "dim_stop": cnt_stops,
            "dim_trip": cnt_trips,
            "fact_scheduled_stop": cnt_facts
        },
        "null_counts": {
            "dim_route_null_route_id": null_route_pk,
            "dim_stop_null_stop_id": null_stop_pk,
            "dim_trip_null_trip_id": null_trip_pk,
            "dim_trip_null_route_id": null_trip_fk_route,
            "fact_null_trip_id": null_fact_pk_trip,
            "fact_null_route_id": null_fact_fk_route,
            "fact_null_stop_id": null_fact_fk_stop,
            "fact_null_stop_sequence": null_fact_seq
        },
        "primary_key_duplicates": {
            "dim_route_duplicates": dup_routes,
            "dim_stop_duplicates": dup_stops,
            "dim_trip_duplicates": dup_trips
        },
        "foreign_key_orphans": {
            "dim_trip_orphan_route_id": orphan_trip_route,
            "fact_scheduled_stop_orphan_trip_id": orphan_fact_trip,
            "fact_scheduled_stop_orphan_route_id": orphan_fact_route,
            "fact_scheduled_stop_orphan_stop_id": orphan_fact_stop
        }
    }
    
    summary_json = json.dumps(validation_summary, indent=2)
    print(summary_json)
    
    # Save validation summary report in silver container
    spark.createDataFrame([(summary_json,)], ["summary_json"]).coalesce(1).write.mode("overwrite").text(f"{silver_base}/validation_report")
    print(f"\nSilver validation report written to: {silver_base}/validation_report")

if __name__ == "__main__":
    main()
