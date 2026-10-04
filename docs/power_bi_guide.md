# Power BI Dashboard Specification & DAX Guide

## 1. Overview & Connection Architecture

* **Source Database:** Azure Synapse Serverless SQL (`syn-transit-analytics-ondemand.sql.azuresynapse.net`)
* **Database Name:** `TransitAnalytics`
* **Data Connectivity Mode:** Import (recommended for optimal performance) or DirectQuery
* **Views Loaded:**
  1. `dbo.vw_daily_transit_metrics` (Executive KPI card metrics)
  2. `dbo.vw_route_performance` (Route reliability & schedule adherence)
  3. `dbo.vw_stop_performance` (Station-level delays & spatial coordinates)
  4. `dbo.vw_delay_analysis` (Multidimensional time & category slices)

---

## 2. Essential DAX Measures (Prevents Rate Averaging Errors)

> [!CRITICAL]
> In Power BI, dragging `on_time_rate` into an aggregated card or visual defaults to `AVERAGE(on_time_rate)`, which calculates an **unweighted arithmetic mean** across routes/stops.
> Always create explicit DAX measures using `DIVIDE(SUM(count), SUM(total))` for accurate weighted metrics across slices.

Create these measures in a dedicated `_Measures` table (or within the respective tables):

### Executive & Network Measures
```dax
Total Observations = 
SUM('vw_delay_analysis'[observation_count])

Network On-Time Rate = 
DIVIDE(
    SUM('vw_delay_analysis'[on_time_count]),
    SUM('vw_delay_analysis'[observation_count]),
    0
)

Network Moderate Delay Rate = 
DIVIDE(
    SUM('vw_delay_analysis'[delayed_count]),
    SUM('vw_delay_analysis'[observation_count]),
    0
)

Network Severe Delay Rate = 
DIVIDE(
    SUM('vw_delay_analysis'[severely_delayed_count]),
    SUM('vw_delay_analysis'[observation_count]),
    0
)

Network Early Rate = 
DIVIDE(
    SUM('vw_delay_analysis'[early_count]),
    SUM('vw_delay_analysis'[observation_count]),
    0
)

Weighted Avg Delay (Min) = 
DIVIDE(
    SUMX('vw_delay_analysis', 'vw_delay_analysis'[avg_delay_minutes] * 'vw_delay_analysis'[observation_count]),
    SUM('vw_delay_analysis'[observation_count]),
    0
)
```

### Route-Level Measures
```dax
Route Weighted On-Time Rate = 
DIVIDE(
    SUM('vw_route_performance'[on_time_count]),
    SUM('vw_route_performance'[observation_count]),
    0
)

Route Weighted Severe Delay Rate = 
DIVIDE(
    SUM('vw_route_performance'[severely_delayed_count]),
    SUM('vw_route_performance'[observation_count]),
    0
)
```

### Stop-Level Measures
```dax
Stop Weighted Severe Delay Rate = 
DIVIDE(
    SUM('vw_stop_performance'[severely_delayed_count]),
    SUM('vw_stop_performance'[observation_count]),
    0
)
```

---

## 3. Data Modeling & Field Formatting Checklist

Before placing visuals, apply these formatting settings in Power BI:

| Field Name | Table | Format / Data Category |
| :--- | :--- | :--- |
| `stop_lat` | `vw_stop_performance` | Data Category: **Latitude** |
| `stop_lon` | `vw_stop_performance` | Data Category: **Longitude** |
| `on_time_rate`, `delayed_rate`, etc. | All tables | Format: **Percentage** (`0.0%`) |
| `avg_arrival_delay_minutes`, etc. | All tables | Format: **Decimal Number** (`0.00`) |
| `observation_count`, `*_count` | All tables | Format: **Whole Number** with `,` separator |
| `service_date` | `vw_delay_analysis`, `vw_daily_transit_metrics` | Data Type: **Date** |
| `observation_hour` | `vw_delay_analysis` | Data Type: **Whole Number** |

---

## 4. Theme & Color Palette (Transportation Design System)

To ensure a cohesive, professional aesthetic aligned with modern transit analytics:

* **Primary Dark / Navy:** `#0A192F` or `#003B71` (TransLink deep blue)
* **On-Time (Punctual):** `#10B981` (Emerald Green)
* **Early Departure:** `#3B82F6` (Sky Blue)
* **Moderate Delay (1–5 min):** `#F59E0B` (Amber / Warning)
* **Severe Delay (> 5 min):** `#EF4444` (Crimson / Alert)
* **Unknown Status:** `#9CA3AF` (Neutral Slate Gray)
* **Background Canvas:** `#F8FAFC` (Clean light off-white) or `#0F172A` (Dark mode)

---

## 5. Visual Layout & Threshold Guidelines

### Page 1: Executive Summary
* **Top Header:** Report Title + Service Date Slicer (`service_date`).
* **KPI Ribbon (5 Cards):**
  1. Total Observations (`103,769`)
  2. On-Time Rate (`30.6%`)
  3. Average Delay (`1.17 min`)
  4. Severe Delay Rate (`16.8%`)
  5. Extreme Delays (`2`)
* **Middle Visuals:**
  * Left: Network Delay Distribution (Clustered column chart by `delay_category`).
  * Right: Average Delay by Hour (Line chart, `observation_hour` on X-axis).
* **Bottom Visuals:**
  * Left: Route On-Time Performance (Bar chart sorted ascending, filtered to `observation_count >= 50`).
  * Right: Network Reliability Mix (100% stacked horizontal bar).

### Page 2: Route Reliability
* **KPI Ribbon:** Total Active Routes (`175`), Total Route Observations (`103,769`), Avg Arrival Delay, Avg Departure Delay, Overall On-Time Rate.
* **Core Visuals:**
  * Top Left: Routes by On-Time Rate (Ascending bar chart, `observation_count >= 50`).
  * Top Right: Routes with Highest Severe Delay Rate (Descending bar chart, highlights Routes 250, 240, 006).
  * Center Left: Arrival vs Departure Delay by Route (Clustered column chart, Top 10 routes).
  * Center Right: Route Reliability vs Delay Scatter (X = `avg_arrival_delay_minutes`, Y = `on_time_rate`, Bubble Size = `observation_count`).
* **Bottom Table:** Detailed Route Performance Grid with Data Bars or Conditional Formatting on Severe Delay %.

### Page 3: Stop & Delay Analysis
* **KPI Ribbon:** Total Stops (`7,108`), Total Stop Observations (`103,769`), Avg Delay, Severe Delay %, Problem Stops.
* **Core Visuals:**
  * Top Left: Top Stops by Average Arrival Delay (Top 10, highlights Stop 4500, Stop 985 on Georgia St).
  * Top Right: Stops with Highest Severe Delay Rate (Top 10, `observation_count >= 20`).
  * Center: Stop Delay Map (Azure Map visual using `stop_lat` and `stop_lon`, bubble size = `observation_count`, color = severe delay rate).
  * Bottom Left: Hourly Delay Pattern (Line chart by `observation_hour`).
  * Bottom Right: Stop Performance Details Table (Searchable by `stop_name` and `zone_id`).
