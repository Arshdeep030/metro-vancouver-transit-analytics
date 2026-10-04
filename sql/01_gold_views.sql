-- ============================================================================
-- SCRIPT: 01_gold_views.sql
-- PROJECT: Metro Vancouver Transit Analytics
-- DATABASE: transit_gold (Synapse Serverless SQL)
-- DATA SOURCE: GoldLakehouse (abfss://gold@stransitanalytics.dfs.core.windows.net)
--
-- IMPORTANT SYNAPSE STUDIO NOTE:
-- In the Synapse Studio toolbar above this editor:
--   1. Set 'Connect to' -> 'Built-in'
--   2. Set 'Use database' -> 'transit_gold' (DO NOT run against 'master')
-- ============================================================================

-- Ensure master key and credential exist in transit_gold
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name LIKE '%DatabaseMasterKey%')
BEGIN
    -- Replace '<YourComplexMasterKeyPassword123!>' with your secure password
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<YourComplexMasterKeyPassword123!>';
END;
GO

IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'WorkspaceIdentity')
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL WorkspaceIdentity
    WITH IDENTITY = 'Managed Identity';
END;
GO

-- Create External Data Source pointing to ADLS Gen2 Gold container
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'GoldLakehouse')
BEGIN
    CREATE EXTERNAL DATA SOURCE GoldLakehouse
    WITH (
        LOCATION = 'https://stransitanalytics.dfs.core.windows.net/gold',
        CREDENTIAL = WorkspaceIdentity
    );
END;
GO

-- ============================================================================
-- 1. VIEW: vw_route_performance
-- Grain: service_date + route_id + direction
-- ============================================================================
CREATE OR ALTER VIEW dbo.vw_route_performance AS
SELECT
    [service_date],
    [route_id],
    [route_short_name],
    [route_long_name],
    [direction],
    [total_observations],
    [avg_delay_minutes],
    [median_delay_minutes],
    [max_delay_minutes],
    [min_delay_minutes],
    [on_time_count],
    [early_count],
    [delayed_count],
    [severely_delayed_count],
    [extreme_delay_count],
    [on_time_rate],
    [delay_rate]
FROM OPENROWSET(
    BULK 'route_performance/**',
    DATA_SOURCE = 'GoldLakehouse',
    FORMAT = 'PARQUET'
) AS [r];
GO

-- ============================================================================
-- 2. VIEW: vw_stop_performance
-- Grain: service_date + stop_id + route_id
-- ============================================================================
CREATE OR ALTER VIEW dbo.vw_stop_performance AS
SELECT
    [service_date],
    [stop_id],
    [stop_name],
    [route_id],
    [route_short_name],
    [total_observations],
    [avg_delay_minutes],
    [median_delay_minutes],
    [max_delay_minutes],
    [min_delay_minutes],
    [on_time_count],
    [early_count],
    [delayed_count],
    [severely_delayed_count],
    [extreme_delay_count],
    [on_time_rate],
    [delay_rate]
FROM OPENROWSET(
    BULK 'stop_performance/**',
    DATA_SOURCE = 'GoldLakehouse',
    FORMAT = 'PARQUET'
) AS [s];
GO

-- ============================================================================
-- 3. VIEW: vw_delay_analysis
-- Grain: service_date + observation_hour + route_id
-- ============================================================================
CREATE OR ALTER VIEW dbo.vw_delay_analysis AS
SELECT
    [service_date],
    [observation_hour],
    [route_id],
    [route_short_name],
    [observation_count],
    [avg_delay_minutes],
    [median_delay_minutes],
    [on_time_count],
    [early_count],
    [delayed_count],
    [severely_delayed_count],
    [extreme_delay_count],
    [on_time_rate],
    [delay_rate]
FROM OPENROWSET(
    BULK 'delay_analysis/**',
    DATA_SOURCE = 'GoldLakehouse',
    FORMAT = 'PARQUET'
) AS [d];
GO

-- ============================================================================
-- 4. VIEW: vw_daily_transit_metrics
-- Grain: service_date
-- ============================================================================
CREATE OR ALTER VIEW dbo.vw_daily_transit_metrics AS
SELECT
    [service_date],
    [total_observations],
    [avg_delay_minutes],
    [median_delay_minutes],
    [early_count],
    [on_time_count],
    [delayed_count],
    [severely_delayed_count],
    [extreme_delay_count],
    [on_time_rate],
    [delay_rate]
FROM OPENROWSET(
    BULK 'daily_transit_metrics/**',
    DATA_SOURCE = 'GoldLakehouse',
    FORMAT = 'PARQUET'
) AS [m];
GO

-- ============================================================================
-- VERIFICATION QUERIES (Highlight and run in transit_gold)
-- ============================================================================
-- SELECT TOP 10 * FROM dbo.vw_daily_transit_metrics;
-- SELECT TOP 10 * FROM dbo.vw_route_performance ORDER BY total_observations DESC;
-- SELECT TOP 10 * FROM dbo.vw_stop_performance ORDER BY total_observations DESC;
-- SELECT TOP 10 * FROM dbo.vw_delay_analysis ORDER BY observation_count DESC;

