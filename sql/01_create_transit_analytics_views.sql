-- ============================================================================
-- SCRIPT: 01_create_transit_analytics_views.sql
-- PROJECT: Metro Vancouver Transit Analytics
-- DATABASE: TransitAnalytics (Synapse Serverless SQL)
-- DATA SOURCE: GoldDataLake (https://stransitanalytics.dfs.core.windows.net/gold)
--
-- PURPOSE:
-- Establishes the Serverless SQL Serving Layer over Gold Parquet datasets in ADLS Gen2.
-- Creates zero-copy analytical views consumable by Power BI and business analysts.
--
-- AUTHENTICATION:
-- Uses Synapse Workspace Managed Identity / Azure AD Passthrough (Zero secrets, zero keys).
--
-- EXECUTION INSTRUCTIONS:
-- 1. In Synapse Studio: Set 'Connect to' -> 'Built-in', 'Use database' -> 'master',
--    run Part 1 (database creation), then switch database to 'TransitAnalytics' and run the rest.
-- 2. Via sqlcmd: sqlcmd -S syn-transit-analytics-ondemand.sql.azuresynapse.net -d TransitAnalytics --authentication-method ActiveDirectoryAzCli -i sql/01_create_transit_analytics_views.sql
-- ============================================================================

-- ============================================================================
-- PART 1: DATABASE & EXTERNAL DATA ACCESS CONFIGURATION
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TransitAnalytics')
BEGIN
    CREATE DATABASE TransitAnalytics;
END;
GO

USE TransitAnalytics;
GO

-- Database Master Key for credential encryption
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name LIKE '%DatabaseMasterKey%')
BEGIN
    -- Replace '<YourComplexMasterKeyPassword123!>' with your secure password
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<YourComplexMasterKeyPassword123!>';
END;
GO

-- Database Scoped Credential using Synapse Workspace Managed Identity
IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'WorkspaceIdentity')
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL WorkspaceIdentity
    WITH IDENTITY = 'Managed Identity';
END;
GO

-- External Data Source pointing to the ADLS Gen2 Gold container
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'GoldDataLake')
BEGIN
    CREATE EXTERNAL DATA SOURCE GoldDataLake
    WITH (
        LOCATION = 'https://stransitanalytics.dfs.core.windows.net/gold',
        CREDENTIAL = WorkspaceIdentity
    );
END;
GO

-- ============================================================================
-- PART 2: GOLD SERVING VIEWS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. VIEW: vw_route_performance
-- Grain: route_id
-- Purpose: Measures transit reliability and schedule adherence at route level.
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_route_performance AS
SELECT
    CAST([route_id] AS VARCHAR(50)) AS [route_id],
    CAST([route_short_name] AS VARCHAR(50)) AS [route_short_name],
    CAST([route_long_name] AS VARCHAR(255)) AS [route_long_name],
    CAST([observation_count] AS BIGINT) AS [observation_count],
    CAST([avg_arrival_delay_minutes] AS FLOAT) AS [avg_arrival_delay_minutes],
    CAST([avg_departure_delay_minutes] AS FLOAT) AS [avg_departure_delay_minutes],
    CAST([on_time_count] AS BIGINT) AS [on_time_count],
    CAST([on_time_rate] AS FLOAT) AS [on_time_rate],
    CAST([delayed_count] AS BIGINT) AS [delayed_count],
    CAST([delayed_rate] AS FLOAT) AS [delayed_rate],
    CAST([severely_delayed_count] AS BIGINT) AS [severely_delayed_count],
    CAST([severely_delayed_rate] AS FLOAT) AS [severely_delayed_rate],
    CAST([early_count] AS BIGINT) AS [early_count],
    CAST([early_rate] AS FLOAT) AS [early_rate],
    CAST([unknown_delay_count] AS BIGINT) AS [unknown_delay_count],
    CAST([unknown_delay_rate] AS FLOAT) AS [unknown_delay_rate],
    CAST([extreme_delay_count] AS BIGINT) AS [extreme_delay_count],
    CAST([first_observed_at] AS DATETIME2) AS [first_observed_at],
    CAST([last_observed_at] AS DATETIME2) AS [last_observed_at]
FROM OPENROWSET(
    BULK 'route_performance/**',
    DATA_SOURCE = 'GoldDataLake',
    FORMAT = 'PARQUET'
) AS [r];
GO

-- ----------------------------------------------------------------------------
-- 2. VIEW: vw_stop_performance
-- Grain: stop_id
-- Purpose: Identifies unreliable stops, congestion hotspots, and spatial delay patterns.
-- Note: Preserves 17 unmatched stop records from Silver.
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_stop_performance AS
SELECT
    CAST([stop_id] AS VARCHAR(50)) AS [stop_id],
    CAST([stop_name] AS VARCHAR(255)) AS [stop_name],
    CAST([stop_lat] AS FLOAT) AS [stop_lat],
    CAST([stop_lon] AS FLOAT) AS [stop_lon],
    CAST([zone_id] AS VARCHAR(50)) AS [zone_id],
    CAST([observation_count] AS BIGINT) AS [observation_count],
    CAST([avg_arrival_delay_minutes] AS FLOAT) AS [avg_arrival_delay_minutes],
    CAST([avg_departure_delay_minutes] AS FLOAT) AS [avg_departure_delay_minutes],
    CAST([on_time_count] AS BIGINT) AS [on_time_count],
    CAST([on_time_rate] AS FLOAT) AS [on_time_rate],
    CAST([delayed_count] AS BIGINT) AS [delayed_count],
    CAST([delayed_rate] AS FLOAT) AS [delayed_rate],
    CAST([severely_delayed_count] AS BIGINT) AS [severely_delayed_count],
    CAST([severely_delayed_rate] AS FLOAT) AS [severely_delayed_rate],
    CAST([early_count] AS BIGINT) AS [early_count],
    CAST([early_rate] AS FLOAT) AS [early_rate],
    CAST([unknown_delay_count] AS BIGINT) AS [unknown_delay_count],
    CAST([unknown_delay_rate] AS FLOAT) AS [unknown_delay_rate],
    CAST([extreme_delay_count] AS BIGINT) AS [extreme_delay_count],
    CAST([first_observed_at] AS DATETIME2) AS [first_observed_at],
    CAST([last_observed_at] AS DATETIME2) AS [last_observed_at]
FROM OPENROWSET(
    BULK 'stop_performance/**',
    DATA_SOURCE = 'GoldDataLake',
    FORMAT = 'PARQUET'
) AS [s];
GO

-- ----------------------------------------------------------------------------
-- 3. VIEW: vw_delay_analysis
-- Grain: service_date + observation_hour + day_of_week + route_id + route_short_name + direction + delay_category
-- Purpose: Power BI multi-dimensional slicing and drill-down analysis.
-- Partitioning: service_date (leveraging filepath(1) for partition pruning).
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_delay_analysis AS
SELECT
    CAST(d.filepath(1) AS DATE) AS [service_date],
    CAST([observation_hour] AS INT) AS [observation_hour],
    CAST([day_of_week] AS VARCHAR(20)) AS [day_of_week],
    CAST([route_id] AS VARCHAR(50)) AS [route_id],
    CAST([route_short_name] AS VARCHAR(50)) AS [route_short_name],
    CAST([direction] AS VARCHAR(50)) AS [direction],
    CAST([delay_category] AS VARCHAR(50)) AS [delay_category],
    CAST([observation_count] AS BIGINT) AS [observation_count],
    CAST([avg_delay_minutes] AS FLOAT) AS [avg_delay_minutes],
    CAST([min_delay_minutes] AS FLOAT) AS [min_delay_minutes],
    CAST([max_delay_minutes] AS FLOAT) AS [max_delay_minutes],
    CAST([on_time_count] AS BIGINT) AS [on_time_count],
    CAST([delayed_count] AS BIGINT) AS [delayed_count],
    CAST([severely_delayed_count] AS BIGINT) AS [severely_delayed_count],
    CAST([early_count] AS BIGINT) AS [early_count],
    CAST([unknown_delay_count] AS BIGINT) AS [unknown_delay_count]
FROM OPENROWSET(
    BULK 'delay_analysis/service_date=*/**',
    DATA_SOURCE = 'GoldDataLake',
    FORMAT = 'PARQUET'
) AS [d];
GO

-- ----------------------------------------------------------------------------
-- 4. VIEW: vw_daily_transit_metrics
-- Grain: service_date
-- Purpose: Executive KPI dataset for network-wide punctuality and reliability indices.
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vw_daily_transit_metrics AS
SELECT
    CAST([service_date] AS DATE) AS [service_date],
    CAST([observation_count] AS BIGINT) AS [observation_count],
    CAST([avg_delay_minutes] AS FLOAT) AS [avg_delay_minutes],
    CAST([median_delay_minutes] AS FLOAT) AS [median_delay_minutes],
    CAST([on_time_count] AS BIGINT) AS [on_time_count],
    CAST([on_time_rate] AS FLOAT) AS [on_time_rate],
    CAST([early_count] AS BIGINT) AS [early_count],
    CAST([early_rate] AS FLOAT) AS [early_rate],
    CAST([delayed_count] AS BIGINT) AS [delayed_count],
    CAST([delayed_rate] AS FLOAT) AS [delayed_rate],
    CAST([severely_delayed_count] AS BIGINT) AS [severely_delayed_count],
    CAST([severely_delayed_rate] AS FLOAT) AS [severely_delayed_rate],
    CAST([unknown_delay_count] AS BIGINT) AS [unknown_delay_count],
    CAST([unknown_delay_rate] AS FLOAT) AS [unknown_delay_rate],
    CAST([extreme_delay_count] AS BIGINT) AS [extreme_delay_count],
    CAST([first_observed_at] AS DATETIME2) AS [first_observed_at],
    CAST([last_observed_at] AS DATETIME2) AS [last_observed_at]
FROM OPENROWSET(
    BULK 'daily_transit_metrics/**',
    DATA_SOURCE = 'GoldDataLake',
    FORMAT = 'PARQUET'
) AS [m];
GO

-- ============================================================================
-- PART 3: VALIDATION & ANALYTICAL QUERIES
-- Note: Results represent operational conditions "During the observed period
--       (2026-10-04 02:03–02:35 UTC / 2026-10-03 19:03–19:35 Vancouver local)".
-- ============================================================================

-- Query A: Top 10 routes by observation count
SELECT TOP 10
    [route_id],
    [route_short_name],
    [route_long_name],
    [observation_count],
    [on_time_rate],
    [delayed_rate],
    [severely_delayed_rate],
    [avg_arrival_delay_minutes]
FROM dbo.vw_route_performance
ORDER BY [observation_count] DESC;

-- Query B: Top 10 routes with lowest on-time rate (minimum 50 observations)
SELECT TOP 10
    [route_id],
    [route_short_name],
    [route_long_name],
    [observation_count],
    [on_time_rate],
    [severely_delayed_rate],
    [avg_arrival_delay_minutes]
FROM dbo.vw_route_performance
WHERE [observation_count] >= 50
ORDER BY [on_time_rate] ASC;

-- Query C: Top 10 routes with highest severe-delay rate (minimum 50 observations)
SELECT TOP 10
    [route_id],
    [route_short_name],
    [route_long_name],
    [observation_count],
    [severely_delayed_count],
    [severely_delayed_rate],
    [avg_arrival_delay_minutes]
FROM dbo.vw_route_performance
WHERE [observation_count] >= 50
ORDER BY [severely_delayed_rate] DESC;

-- Query D: Top 10 stops with highest average delay (minimum 20 observations)
SELECT TOP 10
    [stop_id],
    [stop_name],
    [zone_id],
    [observation_count],
    [avg_arrival_delay_minutes],
    [severely_delayed_rate],
    [on_time_rate]
FROM dbo.vw_stop_performance
WHERE [observation_count] >= 20
ORDER BY [avg_arrival_delay_minutes] DESC;

-- Query E: Observation hour with highest average delay
SELECT
    [observation_hour],
    SUM([observation_count]) AS [total_observations],
    ROUND(SUM([avg_delay_minutes] * [observation_count]) / NULLIF(SUM(CASE WHEN [avg_delay_minutes] IS NOT NULL THEN [observation_count] ELSE 0 END), 0), 2) AS [weighted_avg_delay_minutes],
    ROUND(CAST(SUM([on_time_count]) AS FLOAT) / SUM([observation_count]), 4) AS [hourly_on_time_rate],
    ROUND(CAST(SUM([severely_delayed_count]) AS FLOAT) / SUM([observation_count]), 4) AS [hourly_severe_delay_rate]
FROM dbo.vw_delay_analysis
GROUP BY [observation_hour]
ORDER BY [observation_hour];

-- Query F: Daily network KPIs (Executive Summary)
SELECT
    [service_date],
    [observation_count],
    [avg_delay_minutes],
    [median_delay_minutes],
    [on_time_rate],
    [early_rate],
    [delayed_rate],
    [severely_delayed_rate],
    [unknown_delay_rate],
    [extreme_delay_count],
    [first_observed_at],
    [last_observed_at]
FROM dbo.vw_daily_transit_metrics;

-- Query G: Delay category distribution across network
SELECT
    [delay_category],
    SUM([observation_count]) AS [total_observations],
    ROUND(CAST(SUM([observation_count]) AS FLOAT) / (SELECT SUM([observation_count]) FROM dbo.vw_delay_analysis), 4) AS [pct_of_total],
    ROUND(AVG([avg_delay_minutes]), 2) AS [category_avg_delay_minutes],
    ROUND(MIN([min_delay_minutes]), 2) AS [category_min_delay_minutes],
    ROUND(MAX([max_delay_minutes]), 2) AS [category_max_delay_minutes]
FROM dbo.vw_delay_analysis
GROUP BY [delay_category]
ORDER BY [total_observations] DESC;
GO
