-- AQI Database Initialization Script
-- This script creates the necessary tables for storing AQI data

-- Create database if it doesn't exist
-- (This is handled by docker-compose, but included for completeness)

-- Switch to the aqi_db database
\c aqi_db;

-- Create realtime_metrics_enhanced table for aggregated AQI data
CREATE TABLE IF NOT EXISTS realtime_metrics_enhanced (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    region VARCHAR(50) NOT NULL,
    avg_aqi DECIMAL(10,2) NOT NULL,
    avg_temperature DECIMAL(10,2) NOT NULL,
    avg_humidity DECIMAL(10,2) NOT NULL,
    avg_pm25 DECIMAL(10,2) NOT NULL,
    avg_pm10 DECIMAL(10,2) NOT NULL,
    avg_no2 DECIMAL(10,2) NOT NULL,
    avg_so2 DECIMAL(10,2) NOT NULL,
    avg_co DECIMAL(10,2) NOT NULL,
    avg_o3 DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster queries by region and time
CREATE INDEX IF NOT EXISTS idx_realtime_metrics_region_time
ON realtime_metrics_enhanced(region, window_start, window_end);

-- Create index for time-based queries
CREATE INDEX IF NOT EXISTS idx_realtime_metrics_time
ON realtime_metrics_enhanced(window_start, window_end);

-- Create raw_sensor_data table for storing individual sensor readings
CREATE TABLE IF NOT EXISTS raw_sensor_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    region VARCHAR(50) NOT NULL,
    aqi DECIMAL(10,2) NOT NULL,
    pm25 DECIMAL(10,2) NOT NULL,
    pm10 DECIMAL(10,2) NOT NULL,
    temperature DECIMAL(10,2) NOT NULL,
    humidity DECIMAL(10,2) NOT NULL,
    no2 DECIMAL(10,2) NOT NULL,
    so2 DECIMAL(10,2) NOT NULL,
    co DECIMAL(10,2) NOT NULL,
    o3 DECIMAL(10,2) NOT NULL,
    wind_speed DECIMAL(10,2),
    precipitation DECIMAL(10,2),
    hospital_visits INTEGER,
    emergency_visits INTEGER,
    mobility_index DECIMAL(10,2),
    school_closures INTEGER,
    public_transport_usage DECIMAL(10,2),
    mask_usage_rate DECIMAL(10,2),
    lockdown_status INTEGER,
    industrial_activity DECIMAL(10,2),
    vehicle_count INTEGER,
    construction_activity DECIMAL(10,2),
    respiratory_admissions INTEGER,
    population_density DECIMAL(10,2),
    green_cover_percentage DECIMAL(10,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create index for raw sensor data queries
CREATE INDEX IF NOT EXISTS idx_raw_sensor_data_timestamp
ON raw_sensor_data(timestamp);

CREATE INDEX IF NOT EXISTS idx_raw_sensor_data_region_timestamp
ON raw_sensor_data(region, timestamp);

-- Create table for storing processed/aggregated data by hour
CREATE TABLE IF NOT EXISTS hourly_aggregated_data (
    id SERIAL PRIMARY KEY,
    region VARCHAR(50) NOT NULL,
    hour_start TIMESTAMP WITH TIME ZONE NOT NULL,
    hour_end TIMESTAMP WITH TIME ZONE NOT NULL,
    avg_aqi DECIMAL(10,2) NOT NULL,
    avg_pm25 DECIMAL(10,2) NOT NULL,
    avg_pm10 DECIMAL(10,2) NOT NULL,
    avg_temperature DECIMAL(10,2) NOT NULL,
    avg_humidity DECIMAL(10,2) NOT NULL,
    avg_no2 DECIMAL(10,2) NOT NULL,
    avg_so2 DECIMAL(10,2) NOT NULL,
    avg_co DECIMAL(10,2) NOT NULL,
    avg_o3 DECIMAL(10,2) NOT NULL,
    min_aqi DECIMAL(10,2),
    max_aqi DECIMAL(10,2),
    record_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for hourly aggregated data
CREATE INDEX IF NOT EXISTS idx_hourly_data_region_time
ON hourly_aggregated_data(region, hour_start, hour_end);

-- Create table for storing daily summaries
CREATE TABLE IF NOT EXISTS daily_summaries (
    id SERIAL PRIMARY KEY,
    region VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    avg_aqi DECIMAL(10,2) NOT NULL,
    avg_pm25 DECIMAL(10,2) NOT NULL,
    avg_pm10 DECIMAL(10,2) NOT NULL,
    avg_temperature DECIMAL(10,2) NOT NULL,
    avg_humidity DECIMAL(10,2) NOT NULL,
    min_aqi DECIMAL(10,2),
    max_aqi DECIMAL(10,2),
    aqi_category VARCHAR(20),
    health_risk_level VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for daily summaries
CREATE INDEX IF NOT EXISTS idx_daily_summaries_region_date
ON daily_summaries(region, date);

-- Create table for storing health impact correlations
CREATE TABLE IF NOT EXISTS health_correlations (
    id SERIAL PRIMARY KEY,
    region VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    aqi_level DECIMAL(10,2) NOT NULL,
    hospital_visits INTEGER,
    emergency_visits INTEGER,
    respiratory_admissions INTEGER,
    correlation_strength DECIMAL(5,4),
    confidence_interval DECIMAL(5,4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for health correlations
CREATE INDEX IF NOT EXISTS idx_health_correlations_region_date
ON health_correlations(region, date);

-- Grant permissions to aqi_user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO aqi_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO aqi_user;

-- Create a view for current real-time data (last 24 hours)
CREATE OR REPLACE VIEW current_realtime_data AS
SELECT
    region,
    window_start,
    window_end,
    avg_aqi,
    avg_pm25,
    avg_pm10,
    avg_temperature,
    avg_humidity,
    CASE
        WHEN avg_aqi <= 50 THEN 'Good'
        WHEN avg_aqi <= 100 THEN 'Moderate'
        WHEN avg_aqi <= 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN avg_aqi <= 200 THEN 'Unhealthy'
        WHEN avg_aqi <= 300 THEN 'Very Unhealthy'
        ELSE 'Hazardous'
    END as aqi_category,
    CASE
        WHEN avg_aqi <= 50 THEN 'Low'
        WHEN avg_aqi <= 100 THEN 'Moderate'
        WHEN avg_aqi <= 150 THEN 'High'
        ELSE 'Very High'
    END as health_risk_level
FROM realtime_metrics_enhanced
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY region, window_start DESC;

COMMENT ON VIEW current_realtime_data IS 'Current real-time AQI data with health categorizations';
