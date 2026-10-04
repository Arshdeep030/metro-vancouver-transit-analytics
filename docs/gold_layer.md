# Gold Analytics Layer Documentation

## 1. Architecture Overview

The Gold Analytics Layer serves as the curated, business-level presentation layer of the Metro Vancouver Transit Analytics data platform. It aggregates and conforms high-volume granular transit observations from the Silver layer into optimized, analytics-ready datasets designed specifically for:

1. **Executive KPI Reporting:** System-wide daily punctuality, severe delay exposure, and reliability indices.
2. **Route Reliability Analytics:** Identifying chronic underperforming transit corridors and directional bottlenecks.
3. **Stop & Spatial Analytics:** Pinpointing high-congestion transit hubs and station delay bottlenecks with geographic coordinates and fare zones.
4. **Time & Dimensional Delay Decomposition:** Enabling interactive slicing across service dates, hours of the day, weekdays, routes, and directions in Power BI.

```text
                                TransLink Feeds
                                       │
                  ┌────────────────────┴────────────────────┐
                  │                                         │
             Static GTFS                              Realtime API
                  │                                         │
                  │ (Daily / Periodic)               Azure Function
                  │                                    every 5 min
                  ▼                                         ▼
            ┌───────────┐                             ┌───────────┐
            │  Bronze   │                             │  Bronze   │
            │  Static   │                             │ Realtime  │
            └─────┬─────┘                             └─────┬─────┘
                  │                                         │
                  │ Synapse Spark                           │ Synapse Spark
                  ▼                                         ▼
            ┌───────────┐                             ┌───────────┐
            │  Silver   │                             │  Silver   │
            │ Static    │ ◄────────────────────────── │ Realtime  │
            │   Dims    │    Trip / Route / Stop Join │ Updates   │
            └─────┬─────┘                             └─────┬─────┘
                  │                                         │
                  └────────────────────┬────────────────────┘
                                       │
                                 Synapse Spark
                                       │
                                       ▼
                       ═════════════════════════════
                             GOLD ANALYTICS
                       ═════════════════════════════
                                       │
         ┌───────────────────┬─────────┴─────────┬───────────────────┐
         ▼                   ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────┐
│route_performance│ │stop_performance │ │ delay_analysis  │ │daily_transit_metrics│
│Grain: route_id  │ │Grain: stop_id   │ │Grain: hour+route│ │Grain: service_date  │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘ └──────────┬──────────┘
         │                   │                   │                     │
         └───────────────────┼───────────────────┼─────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   Synapse SQL   │
                    │ Serverless Pool │
                    └────────┬────────┘
                             │ DirectQuery / Import
                             ▼
                    ┌─────────────────┐
                    │    Power BI     │
                    │   Dashboards    │
                    └─────────────────┘
```

---

## 2. Gold Datasets & Specifications

Target Storage Account: `stransitanalytics.dfs.core.windows.net`  
Target Container: `gold`  
Format: Apache Parquet (Snappy compressed)

### 2.1 `route_performance`
* **Target Path:** `abfss://gold@stransitanalytics.dfs.core.windows.net/route_performance/`
* **Purpose:** Measures transit reliability and schedule adherence at the route level. Answers: *Which routes are most reliable or unreliable?*
* **Grain:** `route_id`
* **Schema Definition:**

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `route_id` | `string` | No | GTFS Route Identifier (Primary Key) |
| `route_short_name` | `string` | No | Public route number / code (e.g. "099", "019", "R4") |
| `route_long_name` | `string` | No | Full route description / corridor name |
| `observation_count` | `bigint` | No | Total realtime arrival/departure observations for the route |
| `avg_arrival_delay_minutes` | `double` | Yes | Average arrival delay (minutes). NULL values excluded from average. |
| `avg_departure_delay_minutes` | `double` | Yes | Average departure delay (minutes). NULL values excluded from average. |
| `on_time_count` | `bigint` | No | Count of observations within [-60s, +60s] window |
| `on_time_rate` | `double` | No | `on_time_count / observation_count` ($0.0 \le \text{rate} \le 1.0$) |
| `delayed_count` | `bigint` | No | Count of observations with delay in (60s, 300s] |
| `delayed_rate` | `double` | No | `delayed_count / observation_count` ($0.0 \le \text{rate} \le 1.0$) |
| `severely_delayed_count` | `bigint` | No | Count of observations with delay > 300s (> 5 minutes) |
| `severely_delayed_rate` | `double` | No | `severely_delayed_count / observation_count` ($0.0 \le \text{rate} \le 1.0$) |
| `early_count` | `bigint` | No | Count of observations with delay < -60s |
| `early_rate` | `double` | No | `early_count / observation_count` ($0.0 \le \text{rate} \le 1.0$) |
| `unknown_delay_count` | `bigint` | No | Count of observations where delay was omitted in RT feed |
| `unknown_delay_rate` | `double` | No | `unknown_delay_count / observation_count` ($0.0 \le \text{rate} \le 1.0$) |
| `extreme_delay_count` | `bigint` | No | Count of outlier delays where $|delay| > 3600\text{s}$ (1 hour) |
| `first_observed_at` | `timestamp` | No | Earliest observation timestamp in the dataset |
| `last_observed_at` | `timestamp` | No | Latest observation timestamp in the dataset |

---

### 2.2 `stop_performance`
* **Target Path:** `abfss://gold@stransitanalytics.dfs.core.windows.net/stop_performance/`
* **Purpose:** Identifies spatial transit bottlenecks, chronic station delays, and geographic reliability hotspots. Answers: *Which stops experience the worst delays across the network?*
* **Grain:** `stop_id`
* **Special Handling:** Preserves the 17 unmatched stop records discovered in Silver by assigning explicit surrogate placeholders (`Unknown Stop (ID: <stop_id>)`) with `zone_id = 'UNKNOWN'`, guaranteeing 100% observation retention.
* **Schema Definition:**

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `stop_id` | `string` | No | GTFS Stop Identifier (Primary Key) |
| `stop_name` | `string` | No | Public station or stop name (conformed from dim_stop) |
| `stop_lat` | `double` | Yes | WGS84 Latitude coordinate for GIS mapping |
| `stop_lon` | `double` | Yes | WGS84 Longitude coordinate for GIS mapping |
| `zone_id` | `string` | No | TransLink fare zone or tariff zone identifier |
| `observation_count` | `bigint` | No | Total stop arrival/departure observations |
| `avg_arrival_delay_minutes` | `double` | Yes | Average arrival delay (minutes), NULL delays excluded |
| `avg_departure_delay_minutes` | `double` | Yes | Average departure delay (minutes), NULL delays excluded |
| `on_time_count` | `bigint` | No | Count of on-time stop observations |
| `on_time_rate` | `double` | No | Punctuality rate ($0.0 \le \text{rate} \le 1.0$) |
| `delayed_count` | `bigint` | No | Moderate delay count |
| `delayed_rate` | `double` | No | Moderate delay rate ($0.0 \le \text{rate} \le 1.0$) |
| `severely_delayed_count` | `bigint` | No | Severe delay count (> 5 minutes) |
| `severely_delayed_rate` | `double` | No | Severe delay rate ($0.0 \le \text{rate} \le 1.0$) |
| `early_count` | `bigint` | No | Early arrival/departure count |
| `early_rate` | `double` | No | Early departure rate ($0.0 \le \text{rate} \le 1.0$) |
| `unknown_delay_count` | `bigint` | No | Observations with unknown delay status |
| `unknown_delay_rate` | `double` | No | Unknown delay rate ($0.0 \le \text{rate} \le 1.0$) |
| `extreme_delay_count` | `bigint` | No | Extreme outlier delay count ($|delay| > 1\text{ hr}$) |
| `first_observed_at` | `timestamp` | No | Earliest observation at this stop |
| `last_observed_at` | `timestamp` | No | Latest observation at this stop |

---

### 2.3 `delay_analysis`
* **Target Path:** `abfss://gold@stransitanalytics.dfs.core.windows.net/delay_analysis/`
* **Purpose:** Multi-dimensional analytical dataset optimized for Power BI drill-downs and interactive slicing across temporal, geographic, and categorical dimensions.
* **Grain:** `service_date` + `observation_hour` + `day_of_week` + `route_id` + `route_short_name` + `direction` + `delay_category`
* **Partitioning:** `service_date` (Optimizes partition pruning for time-series reports)
* **Schema Definition:**

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `service_date` | `date` | No | Calendar service date (Partition Key) |
| `observation_hour` | `integer` | No | Local Vancouver hour of day [0, 23] |
| `day_of_week` | `string` | No | Full day name (e.g. "Sunday", "Monday") |
| `route_id` | `string` | No | GTFS Route Identifier |
| `route_short_name` | `string` | No | Public route number / code |
| `direction` | `string` | No | Conformed direction ("North", "South", "East", "West") |
| `delay_category` | `string` | No | Category ("Early", "On Time", "Delayed", "Severely Delayed", "Unknown") |
| `observation_count` | `bigint` | No | Count of observations in this multidimensional slice |
| `avg_delay_minutes` | `double` | Yes | Average delay (minutes) within the slice |
| `min_delay_minutes` | `double` | Yes | Minimum delay recorded |
| `max_delay_minutes` | `double` | Yes | Maximum delay recorded |
| `on_time_count` | `bigint` | No | On-time observations |
| `delayed_count` | `bigint` | No | Moderate delay observations |
| `severely_delayed_count` | `bigint` | No | Severe delay observations |
| `early_count` | `bigint` | No | Early observations |
| `unknown_delay_count` | `bigint` | No | Unknown delay observations |

---

### 2.4 `daily_transit_metrics`
* **Target Path:** `abfss://gold@stransitanalytics.dfs.core.windows.net/daily_transit_metrics/`
* **Purpose:** High-level executive KPI dataset summarizing daily network health and operational efficiency.
* **Grain:** `service_date`
* **Schema Definition:**

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `service_date` | `date` | No | Calendar service date (Primary Key) |
| `observation_count` | `bigint` | No | System-wide total observations recorded |
| `avg_delay_minutes` | `double` | No | System-wide mean delay (minutes), excluding NULLs |
| `median_delay_minutes` | `double` | No | System-wide median delay (50th percentile), excluding NULLs |
| `on_time_count` | `bigint` | No | System-wide on-time count |
| `on_time_rate` | `double` | No | System on-time percentage ($0.0 \le \text{rate} \le 1.0$) |
| `early_count` | `bigint` | No | System-wide early count |
| `early_rate` | `double` | No | System early percentage ($0.0 \le \text{rate} \le 1.0$) |
| `delayed_count` | `bigint` | No | System-wide moderate delay count |
| `delayed_rate` | `double` | No | System moderate delay percentage ($0.0 \le \text{rate} \le 1.0$) |
| `severely_delayed_count` | `bigint` | No | System-wide severe delay count (> 5 min) |
| `severely_delayed_rate` | `double` | No | System severe delay percentage ($0.0 \le \text{rate} \le 1.0$) |
| `unknown_delay_count` | `bigint` | No | Total observations with unstated delay status |
| `unknown_delay_rate` | `double` | No | Unknown delay percentage ($0.0 \le \text{rate} \le 1.0$) |
| `extreme_delay_count` | `bigint` | No | Outlier delays exceeding 1 hour |
| `first_observed_at` | `timestamp` | No | Earliest observation timestamp in the day |
| `last_observed_at` | `timestamp` | No | Latest observation timestamp in the day |

---

## 3. Data Quality & Business Rules

1. **Null Delay Handling:** In standard SQL and PySpark, `avg(delay)` automatically excludes NULL values. We never impute NULL delays with 0.0, as unstated delay does not imply punctual execution. Observations with unknown delays are tracked in a dedicated `unknown_delay_count` column.
2. **Mutual Exclusivity:** The five delay classifications (`Early`, `On Time`, `Delayed`, `Severely Delayed`, `Unknown`) partition 100% of all observations. For every table, the sum of category counts equals `observation_count`.
3. **Strict Rate Bounds:** Every calculated rate is verified to satisfy $0.0 \le \text{rate} \le 1.0$.
4. **Primary Key / Grain Integrity:** Zero duplicate keys or duplicate grains are permitted in any Gold table.
5. **Observation Count Reconciliation:** The sum of `observation_count` across all routes in `route_performance`, across all stops in `stop_performance`, across all slices in `delay_analysis`, and in `daily_transit_metrics` must exactly reconcile with the total row count of the Silver layer (103,769).
6. **Temporal Context Integrity:** During current development, data spans an initial 32-minute observation window (02:03–02:35 UTC on October 4, 2026). Analytical narratives must describe findings as *"During the observed period"* rather than full-day generalizations until further automated 5-minute snapshots accumulate.
