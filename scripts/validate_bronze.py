import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("ValidateBronzeGTFS").getOrCreate()
    
    storage_account = "stransitanalytics"
    base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/gtfs_static"
    
    print("=" * 60)
    print("PHASE 6 & 7: READING GTFS STATIC DATASETS FROM BRONZE")
    print("=" * 60)
    
    # 1. Routes
    routes_path = f"{base_path}/routes.txt"
    routes_df = spark.read.option("header", True).option("inferSchema", True).csv(routes_path)
    routes_count = routes_df.count()
    print(f"routes.txt Row Count: {routes_count}")
    print(f"routes.txt Columns: {routes_df.columns}")
    
    # 2. Trips
    trips_path = f"{base_path}/trips.txt"
    trips_df = spark.read.option("header", True).option("inferSchema", True).csv(trips_path)
    trips_count = trips_df.count()
    print(f"trips.txt Row Count: {trips_count}")
    print(f"trips.txt Columns: {trips_df.columns}")
    
    # 3. Stops
    stops_path = f"{base_path}/stops.txt"
    stops_df = spark.read.option("header", True).option("inferSchema", True).csv(stops_path)
    stops_count = stops_df.count()
    print(f"stops.txt Row Count: {stops_count}")
    print(f"stops.txt Columns: {stops_df.columns}")
    
    # 4. Stop Times
    stop_times_path = f"{base_path}/stop_times.txt"
    stop_times_df = spark.read.option("header", True).option("inferSchema", True).csv(stop_times_path)
    stop_times_count = stop_times_df.count()
    print(f"stop_times.txt Row Count: {stop_times_count}")
    print(f"stop_times.txt Columns: {stop_times_df.columns}")
    
    print("=" * 60)
    print("PHASE 8: VALIDATING RELATIONSHIPS AND CHECKING ORPHANS")
    print("=" * 60)
    
    # Cast join keys to string
    routes_clean = routes_df.withColumn("route_id", col("route_id").cast("string"))
    trips_clean = trips_df.withColumn("trip_id", col("trip_id").cast("string")).withColumn("route_id", col("route_id").cast("string"))
    stops_clean = stops_df.withColumn("stop_id", col("stop_id").cast("string"))
    stop_times_clean = stop_times_df.withColumn("trip_id", col("trip_id").cast("string")).withColumn("stop_id", col("stop_id").cast("string"))
    
    # Validation 1: trips.route_id -> routes.route_id
    orphan_trips_routes = trips_clean.join(routes_clean, on="route_id", how="left_anti")
    orphan_trips_routes_count = orphan_trips_routes.count()
    
    # Validation 2: stop_times.trip_id -> trips.trip_id
    orphan_st_trips = stop_times_clean.join(trips_clean, on="trip_id", how="left_anti")
    orphan_st_trips_count = orphan_st_trips.count()
    
    # Validation 3: stop_times.stop_id -> stops.stop_id
    orphan_st_stops = stop_times_clean.join(stops_clean, on="stop_id", how="left_anti")
    orphan_st_stops_count = orphan_st_stops.count()
    
    results = {
        "status": "SUCCESS",
        "row_counts": {
            "routes": routes_count,
            "trips": trips_count,
            "stops": stops_count,
            "stop_times": stop_times_count
        },
        "orphan_counts": {
            "orphan_trips_routes": orphan_trips_routes_count,
            "orphan_st_trips": orphan_st_trips_count,
            "orphan_st_stops": orphan_st_stops_count
        },
        "routes_columns": routes_df.columns,
        "trips_columns": trips_df.columns,
        "stops_columns": stops_df.columns,
        "stop_times_columns": stop_times_df.columns
    }
    
    # Write results JSON directly to bronze
    results_json = json.dumps(results, indent=2)
    print(results_json)
    
    output_df = spark.createDataFrame([(results_json,)], ["validation_json"])
    output_df.coalesce(1).write.mode("overwrite").text(f"abfss://bronze@{storage_account}.dfs.core.windows.net/validation_report")
    print("Validation report saved successfully to ADLS!")

if __name__ == "__main__":
    main()
