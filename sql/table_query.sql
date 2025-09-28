-- =====================================================
-- Global Weather Data Pipeline - Database Schema
-- =====================================================
-- This script creates the complete database schema for 
-- the weather data pipeline project
-- =====================================================

-- Create Weather schema
CREATE SCHEMA IF NOT EXISTS Weather;

-- =====================================================
-- Main Weather Data Table
-- =====================================================
CREATE TABLE IF NOT EXISTS Weather.weather_snapshots_now_and_prev (
    index SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    timezone VARCHAR(50),
    localtime TIMESTAMP,
    last_updated TIMESTAMP,
    temp_c DECIMAL(5,2),
    condition VARCHAR(200),
    condition_icon VARCHAR(500),
    wind_kph DECIMAL(5,2),
    wind_dir VARCHAR(10),
    cloud INTEGER CHECK (cloud >= 0 AND cloud <= 100),
    humidity INTEGER CHECK (humidity >= 0 AND humidity <= 100),
    pressure_mb DECIMAL(7,2),
    day_1_temp DECIMAL(5,2),
    day_1_condition VARCHAR(200),
    day_2_temp DECIMAL(5,2),
    day_2_condition VARCHAR(200),
    day_3_temp DECIMAL(5,2),
    day_3_condition VARCHAR(200),
    time_data_gotten TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error VARCHAR(500)
);

-- =====================================================
-- Cities Reference Data Table
-- =====================================================
CREATE TABLE IF NOT EXISTS Weather.cities_data (
    city_id_database SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    city_ascii VARCHAR(100),
    lat DECIMAL(10,7),
    lng DECIMAL(10,7),
    country VARCHAR(100) NOT NULL,
    iso2 CHAR(2),
    iso3 CHAR(3),
    admin_name VARCHAR(100),
    capital VARCHAR(50),
    population INTEGER,
    id INTEGER
);

-- =====================================================
-- Timezone Offset Reference Table
-- =====================================================
CREATE TABLE IF NOT EXISTS Weather.cities_offset (
    id SERIAL PRIMARY KEY,
    offset INTEGER,
    minutes_diff_UTC INTEGER,
    title VARCHAR(100),
    hours_diff_utc DECIMAL(4,2)
);

-- =====================================================
-- Countries Reference Table
-- =====================================================
CREATE TABLE IF NOT EXISTS Weather.countries_data (
    country_id SERIAL PRIMARY KEY,
    country VARCHAR(100) NOT NULL UNIQUE,
    iso3 CHAR(3) UNIQUE
);

-- =====================================================
-- Create Indexes for Performance
-- =====================================================

-- Indexes for weather_snapshots_now_and_prev
CREATE INDEX IF NOT EXISTS idx_weather_city_country 
    ON Weather.weather_snapshots_now_and_prev(city, country);

CREATE INDEX IF NOT EXISTS idx_weather_time_data_gotten 
    ON Weather.weather_snapshots_now_and_prev(time_data_gotten DESC);

CREATE INDEX IF NOT EXISTS idx_weather_timezone 
    ON Weather.weather_snapshots_now_and_prev(timezone);

CREATE INDEX IF NOT EXISTS idx_weather_temp 
    ON Weather.weather_snapshots_now_and_prev(temp_c);

-- Indexes for cities_data
CREATE INDEX IF NOT EXISTS idx_cities_country 
    ON Weather.cities_data(country);

CREATE INDEX IF NOT EXISTS idx_cities_population 
    ON Weather.cities_data(population DESC);

CREATE INDEX IF NOT EXISTS idx_cities_coordinates 
    ON Weather.cities_data(lat, lng);

-- Indexes for countries_data
CREATE INDEX IF NOT EXISTS idx_countries_iso3 
    ON Weather.countries_data(iso3);


--note that you will need to import the data manually through the data I provided later after creating the Scheme