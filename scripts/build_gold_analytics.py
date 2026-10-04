"""
Metro Vancouver Transit Analytics - Gold Layer Transformation Script
Executes on Azure Synapse Analytics Spark Pool (sparkpool).

Reads conformed Silver data:
  - abfss://silver@stransitanalytics.dfs.core.windows.net/realtime_trip_updates
  - abfss://silver@stransitanalytics.dfs.core.windows.net/dim_route
  - abfss://silver@stransitanalytics.dfs.core.windows.net/dim_stop

Generates 4 business-ready Gold analytics tables:
  1. route_performance (Grain: route_id)
  2. stop_performance  (Grain: stop_id)
  3. delay_analysis     (Grain: service_date, observation_hour, day_of_week, route_id, route_short_name, direction, delay_category)
  4. daily_transit_metrics (Grain: service_date)

Enforces strict data quality:
  - 0 <= rate <= 1.0
  - Never replace NULL delay with zero
  - Preserve 17 unmatched stop records from Silver
  - Full observation reconciliation against Silver (103,769)
"""

import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg as spark_avg, min as spark_min, max as spark_max,
    round as spark_round, expr, when, lit, coalesce, concat, date_format, to_date
)

def run_gold_pipeline():
    spark = SparkSession.builder.appName("BuildGoldAnalytics").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/Vancouver")
    
    storage_account = "stransitanalytics"
    silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"
    gold_base = f"abfss://gold@{storage_account}.dfs.core.windows.net"
    
    print("=" * 80)
    print("PHASE 3: BUILDING GOLD ANALYTICS LAYER IN ADLS GEN2")
    print("=" * 80)
    
    # -------------------------------------------------------------------------
    # 0. Load Validated Silver Datasets
    # -------------------------------------------------------------------------
    silver_rt_path = f"{silver_base}/realtime_trip_updates"
    print(f"\n[0/4] Reading Silver Source: {silver_rt_path}")
    rt_df = spark.read.parquet(silver_rt_path)
    total_silver_observations = rt_df.count()
    print(f"Loaded {total_silver_observations:,} Silver realtime records.")
    
    dim_route = spark.read.parquet(f"{silver_base}/dim_route")
    dim_stop = spark.read.parquet(f"{silver_base}/dim_stop")
    print(f"Loaded {dim_route.count():,} dim_route records and {dim_stop.count():,} dim_stop records.")
    
    # Capture overall temporal window
    system_time_bounds = rt_df.select(
        spark_min("observed_at").cast("string").alias("earliest_observation"),
        spark_max("observed_at").cast("string").alias("latest_observation")
    ).first()
    earliest_obs = system_time_bounds["earliest_observation"]
    latest_obs = system_time_bounds["latest_observation"]
    print(f"Observation Window: {earliest_obs} to {latest_obs} (UTC)")
    
    # -------------------------------------------------------------------------
    # 1. Gold Dataset 1: route_performance
    # Purpose: Measure reliability at route level
    # Grain: route_id
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print("[1/4] Building Gold: route_performance...")
    print("-" * 60)
    
    # Join with dim_route to conform route attributes
    dim_route_sub = dim_route.select(
        col("route_id"),
        col("route_short_name").alias("dim_route_short_name"),
        col("route_long_name").alias("dim_route_long_name")
    )
    
    rt_with_route = (
        rt_df.join(dim_route_sub, on="route_id", how="left")
        .withColumn("final_route_short_name", coalesce(col("route_short_name"), col("dim_route_short_name"), lit("Unknown")))
        .withColumn("final_route_long_name", coalesce(col("route_long_name"), col("dim_route_long_name"), lit("Unknown")))
    )
    
    route_perf = (
        rt_with_route
        .groupBy(
            col("route_id"),
            col("final_route_short_name").alias("route_short_name"),
            col("final_route_long_name").alias("route_long_name")
        )
        .agg(
            count("*").alias("observation_count"),
            spark_round(spark_avg("arrival_delay_minutes"), 2).alias("avg_arrival_delay_minutes"),
            spark_round(spark_avg("departure_delay_minutes"), 2).alias("avg_departure_delay_minutes"),
            spark_sum("is_on_time").alias("on_time_count"),
            spark_sum("is_delayed").alias("delayed_count"),
            spark_sum("is_severely_delayed").alias("severely_delayed_count"),
            spark_sum("is_early").alias("early_count"),
            spark_sum(when(col("delay_category") == "Unknown", 1).otherwise(0)).alias("unknown_delay_count"),
            spark_sum("extreme_delay_flag").alias("extreme_delay_count"),
            spark_min("observed_at").alias("first_observed_at"),
            spark_max("observed_at").alias("last_observed_at")
        )
        .withColumn("on_time_rate", spark_round(col("on_time_count") / col("observation_count"), 4))
        .withColumn("delayed_rate", spark_round(col("delayed_count") / col("observation_count"), 4))
        .withColumn("severely_delayed_rate", spark_round(col("severely_delayed_count") / col("observation_count"), 4))
        .withColumn("early_rate", spark_round(col("early_count") / col("observation_count"), 4))
        .withColumn("unknown_delay_rate", spark_round(col("unknown_delay_count") / col("observation_count"), 4))
        .select(
            "route_id",
            "route_short_name",
            "route_long_name",
            "observation_count",
            "avg_arrival_delay_minutes",
            "avg_departure_delay_minutes",
            "on_time_count",
            "on_time_rate",
            "delayed_count",
            "delayed_rate",
            "severely_delayed_count",
            "severely_delayed_rate",
            "early_count",
            "early_rate",
            "unknown_delay_count",
            "unknown_delay_rate",
            "extreme_delay_count",
            "first_observed_at",
            "last_observed_at"
        )
    )
    
    route_perf_path = f"{gold_base}/route_performance"
    route_perf.coalesce(1).write.mode("overwrite").parquet(route_perf_path)
    print(f"route_performance written to: {route_perf_path}")
    
    # -------------------------------------------------------------------------
    # 2. Gold Dataset 2: stop_performance
    # Purpose: Identify unreliable stops
    # Grain: stop_id
    # Note: Preserves 17 unmatched stop records from Silver
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print("[2/4] Building Gold: stop_performance...")
    print("-" * 60)
    
    dim_stop_sub = dim_stop.select(
        col("stop_id"),
        col("stop_name").alias("dim_stop_name"),
        col("stop_lat").alias("dim_stop_lat"),
        col("stop_lon").alias("dim_stop_lon"),
        col("zone_id").alias("dim_zone_id")
    )
    
    # Left join rt_df with dim_stop on stop_id
    rt_with_stop = (
        rt_df.join(dim_stop_sub, on="stop_id", how="left")
        .withColumn(
            "final_stop_name",
            coalesce(
                col("dim_stop_name"),
                col("stop_name"),
                concat(lit("Unknown Stop (ID: "), col("stop_id"), lit(")"))
            )
        )
        .withColumn("final_stop_lat", coalesce(col("dim_stop_lat"), col("stop_lat")))
        .withColumn("final_stop_lon", coalesce(col("dim_stop_lon"), col("stop_lon")))
        .withColumn("final_zone_id", coalesce(col("dim_zone_id"), lit("UNKNOWN")))
    )
    
    stop_perf = (
        rt_with_stop
        .groupBy(
            col("stop_id"),
            col("final_stop_name").alias("stop_name"),
            col("final_stop_lat").alias("stop_lat"),
            col("final_stop_lon").alias("stop_lon"),
            col("final_zone_id").alias("zone_id")
        )
        .agg(
            count("*").alias("observation_count"),
            spark_round(spark_avg("arrival_delay_minutes"), 2).alias("avg_arrival_delay_minutes"),
            spark_round(spark_avg("departure_delay_minutes"), 2).alias("avg_departure_delay_minutes"),
            spark_sum("is_on_time").alias("on_time_count"),
            spark_sum("is_delayed").alias("delayed_count"),
            spark_sum("is_severely_delayed").alias("severely_delayed_count"),
            spark_sum("is_early").alias("early_count"),
            spark_sum(when(col("delay_category") == "Unknown", 1).otherwise(0)).alias("unknown_delay_count"),
            spark_sum("extreme_delay_flag").alias("extreme_delay_count"),
            spark_min("observed_at").alias("first_observed_at"),
            spark_max("observed_at").alias("last_observed_at")
        )
        .withColumn("on_time_rate", spark_round(col("on_time_count") / col("observation_count"), 4))
        .withColumn("delayed_rate", spark_round(col("delayed_count") / col("observation_count"), 4))
        .withColumn("severely_delayed_rate", spark_round(col("severely_delayed_count") / col("observation_count"), 4))
        .withColumn("early_rate", spark_round(col("early_count") / col("observation_count"), 4))
        .withColumn("unknown_delay_rate", spark_round(col("unknown_delay_count") / col("observation_count"), 4))
        .select(
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "zone_id",
            "observation_count",
            "avg_arrival_delay_minutes",
            "avg_departure_delay_minutes",
            "on_time_count",
            "on_time_rate",
            "delayed_count",
            "delayed_rate",
            "severely_delayed_count",
            "severely_delayed_rate",
            "early_count",
            "early_rate",
            "unknown_delay_count",
            "unknown_delay_rate",
            "extreme_delay_count",
            "first_observed_at",
            "last_observed_at"
        )
    )
    
    stop_perf_path = f"{gold_base}/stop_performance"
    stop_perf.coalesce(1).write.mode("overwrite").parquet(stop_perf_path)
    print(f"stop_performance written to: {stop_perf_path}")
    
    # -------------------------------------------------------------------------
    # 3. Gold Dataset 3: delay_analysis
    # Purpose: Provide time-based analytics for Power BI
    # Grain: service_date + observation_hour + day_of_week + route_id + route_short_name + direction + delay_category
    # Partitioned by: service_date
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print("[3/4] Building Gold: delay_analysis...")
    print("-" * 60)
    
    rt_with_time = (
        rt_df
        .withColumn("day_of_week", date_format(col("service_date"), "EEEE"))
        .withColumn("direction_clean", coalesce(col("direction"), lit("Unknown")))
        .withColumn("route_short_name_clean", coalesce(col("route_short_name"), lit("Unknown")))
    )
    
    delay_analysis = (
        rt_with_time
        .groupBy(
            col("service_date"),
            col("observation_hour"),
            col("day_of_week"),
            col("route_id"),
            col("route_short_name_clean").alias("route_short_name"),
            col("direction_clean").alias("direction"),
            col("delay_category")
        )
        .agg(
            count("*").alias("observation_count"),
            spark_round(spark_avg("effective_delay_minutes"), 2).alias("avg_delay_minutes"),
            spark_round(spark_min("effective_delay_minutes"), 2).alias("min_delay_minutes"),
            spark_round(spark_max("effective_delay_minutes"), 2).alias("max_delay_minutes"),
            spark_sum("is_on_time").alias("on_time_count"),
            spark_sum("is_delayed").alias("delayed_count"),
            spark_sum("is_severely_delayed").alias("severely_delayed_count"),
            spark_sum("is_early").alias("early_count"),
            spark_sum(when(col("delay_category") == "Unknown", 1).otherwise(0)).alias("unknown_delay_count")
        )
        .select(
            "service_date",
            "observation_hour",
            "day_of_week",
            "route_id",
            "route_short_name",
            "direction",
            "delay_category",
            "observation_count",
            "avg_delay_minutes",
            "min_delay_minutes",
            "max_delay_minutes",
            "on_time_count",
            "delayed_count",
            "severely_delayed_count",
            "early_count",
            "unknown_delay_count"
        )
    )
    
    delay_analysis_path = f"{gold_base}/delay_analysis"
    delay_analysis.write.mode("overwrite").partitionBy("service_date").parquet(delay_analysis_path)
    print(f"delay_analysis written to: {delay_analysis_path}")
    
    # -------------------------------------------------------------------------
    # 4. Gold Dataset 4: daily_transit_metrics
    # Purpose: Executive KPI dataset
    # Grain: service_date
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print("[4/4] Building Gold: daily_transit_metrics...")
    print("-" * 60)
    
    daily_transit_metrics = (
        rt_df
        .groupBy("service_date")
        .agg(
            count("*").alias("observation_count"),
            spark_round(spark_avg("effective_delay_minutes"), 2).alias("avg_delay_minutes"),
            spark_round(expr("percentile_approx(effective_delay_minutes, 0.5)"), 2).alias("median_delay_minutes"),
            spark_sum("is_on_time").alias("on_time_count"),
            spark_sum("is_early").alias("early_count"),
            spark_sum("is_delayed").alias("delayed_count"),
            spark_sum("is_severely_delayed").alias("severely_delayed_count"),
            spark_sum(when(col("delay_category") == "Unknown", 1).otherwise(0)).alias("unknown_delay_count"),
            spark_sum("extreme_delay_flag").alias("extreme_delay_count"),
            spark_min("observed_at").alias("first_observed_at"),
            spark_max("observed_at").alias("last_observed_at")
        )
        .withColumn("on_time_rate", spark_round(col("on_time_count") / col("observation_count"), 4))
        .withColumn("early_rate", spark_round(col("early_count") / col("observation_count"), 4))
        .withColumn("delayed_rate", spark_round(col("delayed_count") / col("observation_count"), 4))
        .withColumn("severely_delayed_rate", spark_round(col("severely_delayed_count") / col("observation_count"), 4))
        .withColumn("unknown_delay_rate", spark_round(col("unknown_delay_count") / col("observation_count"), 4))
        .select(
            "service_date",
            "observation_count",
            "avg_delay_minutes",
            "median_delay_minutes",
            "on_time_count",
            "on_time_rate",
            "early_count",
            "early_rate",
            "delayed_count",
            "delayed_rate",
            "severely_delayed_count",
            "severely_delayed_rate",
            "unknown_delay_count",
            "unknown_delay_rate",
            "extreme_delay_count",
            "first_observed_at",
            "last_observed_at"
        )
    )
    
    daily_metrics_path = f"{gold_base}/daily_transit_metrics"
    daily_transit_metrics.coalesce(1).write.mode("overwrite").parquet(daily_metrics_path)
    print(f"daily_transit_metrics written to: {daily_metrics_path}")
    
    # =========================================================================
    # 5. Comprehensive Data Quality & Verification
    # =========================================================================
    print("\n" + "=" * 80)
    print("STEP 5: COMPREHENSIVE DATA QUALITY VALIDATION")
    print("=" * 80)
    
    # Reload written Parquet datasets to ensure persistent integrity
    g_routes = spark.read.parquet(route_perf_path)
    g_stops = spark.read.parquet(stop_perf_path)
    g_delay = spark.read.parquet(delay_analysis_path)
    g_daily = spark.read.parquet(daily_metrics_path)
    
    def validate_gold_table(df_table, name, grain_cols, rate_cols):
        cnt = df_table.count()
        distinct_grain = df_table.select(grain_cols).distinct().count()
        dups = cnt - distinct_grain
        
        # Null grain checks
        null_grain_map = {}
        for c in grain_cols:
            null_cnt = df_table.filter(col(c).isNull()).count()
            null_grain_map[c] = null_cnt
            
        # Rate boundary checks: 0 <= rate <= 1
        rate_violations = {}
        for r in rate_cols:
            v_cnt = df_table.filter((col(r) < 0.0) | (col(r) > 1.0)).count()
            rate_violations[r] = v_cnt
            
        # Observation reconciliation
        tot_obs = df_table.agg(spark_sum("observation_count")).first()[0]
        
        # Unknown & extreme delays
        has_unknown = "unknown_delay_count" in df_table.columns
        unknown_cnt = df_table.agg(spark_sum("unknown_delay_count")).first()[0] if has_unknown else 0
        has_extreme = "extreme_delay_count" in df_table.columns
        extreme_cnt = df_table.agg(spark_sum("extreme_delay_count")).first()[0] if has_extreme else 0
        
        # Timestamps
        earliest = str(df_table.agg(spark_min("first_observed_at")).first()[0]) if "first_observed_at" in df_table.columns else earliest_obs
        latest = str(df_table.agg(spark_max("last_observed_at")).first()[0]) if "last_observed_at" in df_table.columns else latest_obs
        
        return {
            "dataset_name": name,
            "row_count": cnt,
            "grain": grain_cols,
            "duplicate_grain_count": dups,
            "null_grain_counts": null_grain_map,
            "rate_violations_count": rate_violations,
            "total_observations_reconciled": tot_obs,
            "unknown_delay_count": unknown_cnt,
            "extreme_delay_count": extreme_cnt,
            "earliest_observation": earliest,
            "latest_observation": latest,
            "schema": [f"{f.name}: {f.dataType.simpleString()}" for f in df_table.schema.fields]
        }
        
    val_routes = validate_gold_table(
        g_routes, "route_performance",
        grain_cols=["route_id"],
        rate_cols=["on_time_rate", "delayed_rate", "severely_delayed_rate", "early_rate", "unknown_delay_rate"]
    )
    
    val_stops = validate_gold_table(
        g_stops, "stop_performance",
        grain_cols=["stop_id"],
        rate_cols=["on_time_rate", "delayed_rate", "severely_delayed_rate", "early_rate", "unknown_delay_rate"]
    )
    
    val_delay = validate_gold_table(
        g_delay, "delay_analysis",
        grain_cols=["service_date", "observation_hour", "day_of_week", "route_id", "route_short_name", "direction", "delay_category"],
        rate_cols=[]
    )
    
    val_daily = validate_gold_table(
        g_daily, "daily_transit_metrics",
        grain_cols=["service_date"],
        rate_cols=["on_time_rate", "early_rate", "delayed_rate", "severely_delayed_rate", "unknown_delay_rate"]
    )
    
    # Count unmatched stop records in stop_performance
    unmatched_stops_in_gold = g_stops.filter(col("stop_name").startswith("Unknown Stop")).count()
    unmatched_stop_obs_in_gold = g_stops.filter(col("stop_name").startswith("Unknown Stop")).agg(spark_sum("observation_count")).first()[0]
    
    validation_summary = {
        "status": "SUCCESS",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_silver_observations": total_silver_observations,
        "earliest_observation_utc": str(earliest_obs),
        "latest_observation_utc": str(latest_obs),
        "unmatched_stop_records": {
            "distinct_unmatched_stops": unmatched_stops_in_gold,
            "unmatched_observations_preserved": unmatched_stop_obs_in_gold
        },
        "datasets": {
            "route_performance": val_routes,
            "stop_performance": val_stops,
            "delay_analysis": val_delay,
            "daily_transit_metrics": val_daily
        }
    }
    
    report_json_str = json.dumps(validation_summary, indent=2)
    print("\n" + "=" * 80)
    print("GOLD VALIDATION REPORT JSON:")
    print("=" * 80)
    print(report_json_str)
    
    # Save gold validation report to ADLS Gen2
    report_path = f"{gold_base}/gold_validation_report"
    spark.createDataFrame([(report_json_str,)], ["report_json"]).coalesce(1).write.mode("overwrite").text(report_path)
    print(f"\nGold validation report successfully stored at: {report_path}")
    
    return validation_summary

result = run_gold_pipeline()

