# Metro Vancouver Transit Analytics (TransLink)

An end-to-end cloud data engineering and business intelligence solution analyzing Metro Vancouver (TransLink) transit reliability, schedule adherence, and delay patterns using GTFS Static schedules and GTFS-Realtime feeds.

---

## 1. End-to-End Cloud Architecture

```text
               TransLink GTFS Static + Realtime API
                               │
                               │ HTTP GET (every 5 min)
                               ▼
               ┌─────────────────────────────────┐
               │         Azure Function          │
               │    Python 3.11 (Serverless)     │
               │    System Managed Identity      │
               └───────────────┬─────────────────┘
                               │
                               │ Raw Protobuf (.pb)
                               │ Zero-Secret RBAC
                               ▼
               ┌─────────────────────────────────┐
               │        ADLS Gen2 Bronze         │
               │        stransitanalytics        │
               │    Immutable Raw Data Lake      │
               └───────────────┬─────────────────┘
                               │
                               │ Incremental Discovery
                               ▼
               ┌─────────────────────────────────┐
               │          Synapse Spark          │
               │        (sparkpool nodes)        │
               └───────────────┬─────────────────┘
                               │
                               │ Dimensional Conflation & Cleanse
                               ▼
               ┌─────────────────────────────────┐
               │        ADLS Gen2 Silver         │
               │  dim_route, dim_stop, dim_trip  │
               │     realtime_trip_updates       │
               └───────────────┬─────────────────┘
                               │
                               │ Aggregation & Business Metrics
                               ▼
               ┌─────────────────────────────────┐
               │         ADLS Gen2 Gold          │
               │       route_performance         │
               │       stop_performance          │
               │        delay_analysis           │
               │     daily_transit_metrics       │
               └───────────────┬─────────────────┘
                               │
                               │ External Lakehouse Views
                               ▼
               ┌─────────────────────────────────┐
               │     Synapse Serverless SQL      │
               │      Database: transit_gold     │
               └───────────────┬─────────────────┘
                               │
                               │ DirectQuery / Import
                               ▼
               ┌─────────────────────────────────┐
               │       Power BI Dashboard        │
               │  Executive & Route Reliability  │
               └─────────────────────────────────┘
```

---

## 2. Technology Stack

* **Data Lake Storage:** Azure Data Lake Storage Gen2 (`stransitanalytics`, Canada Central)
* **Realtime Ingestion:** Azure Function App (`func-transit-realtime-ingestion`, Python 3.11, Timer Trigger every 5 min)
* **Distributed Compute:** Azure Synapse Analytics Spark Pool (`sparkpool`, 3 Small nodes)
* **Serving Layer:** Azure Synapse Serverless SQL (`transit_gold`, zero-cost idle compute)
* **Orchestration & Security:** System-Assigned Managed Identity, Azure RBAC, Azure CLI
* **Visualization Layer:** Microsoft Power BI

---

## 3. Data Flow & Medallion Design

1. **Bronze (Raw Lake):**
   * Static GTFS schedules (16 standard GTFS `.txt` tables).
   * Realtime GTFS snapshots partitioned by `year=YYYY/month=MM/day=DD/hour=HH/` as immutable raw Protocol Buffer files (`.pb`).
2. **Silver (Conformed & Cleansed):**
   * Dimensions: `dim_route`, `dim_stop`, `dim_trip`, `fact_scheduled_stop`.
   * Realtime Fact: `realtime_trip_updates` (parsed protobuf records enriched with static dimensions and business delay categories: Early, On Time, Delayed, Severely Delayed).
   * Incremental ingestion tracked via `control/processed_realtime_files` ensuring 100% idempotency.
3. **Gold (Curated Analytical Datasets):**
   * `route_performance`: Grain `(route_id)` - Route reliability, arrival/departure avg delays, and delay categories.
   * `stop_performance`: Grain `(stop_id)` - Station/stop punctuality, spatial coordinates, fare zones (17 unmatched stops preserved).
   * `delay_analysis`: Grain `(service_date, observation_hour, day_of_week, route_id, route_short_name, direction, delay_category)` - Multi-dimensional drill-downs for Power BI.
   * `daily_transit_metrics`: Grain `(service_date)` - Executive KPI daily system reliability metrics.
4. **SQL Layer:**
   * Serverless SQL views in `transit_gold`: `vw_route_performance`, `vw_stop_performance`, `vw_delay_analysis`, `vw_daily_transit_metrics`.
