"""
File: weather_schema.sql
Path: ./sql/migrations/weather_schema.sql
Description: Database schema for weather forecast data tables
"""

-- Create coordinates table
CREATE TABLE IF NOT EXISTS coordinates (
    id SERIAL PRIMARY KEY,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    location_name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create forecasts table with all weather data fields
CREATE TABLE IF NOT EXISTS forecasts (
    id SERIAL PRIMARY KEY,
    coordinate_id INTEGER NOT NULL REFERENCES coordinates(id),
    date DATE NOT NULL,
    hour INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
    
    -- Basic weather parameters
    tc FLOAT,                    -- Temperature (°C)
    rh FLOAT,                    -- Relative humidity (%)
    slp FLOAT,                   -- Sea-level pressure (hPa)
    rain FLOAT,                  -- Rainfall (mm)
    
    -- Wind speed at different pressure levels
    ws_10m FLOAT,                -- Wind speed at 10m
    ws_925 FLOAT,                -- Wind speed at 925 hPa
    ws_850 FLOAT,                -- Wind speed at 850 hPa
    ws_700 FLOAT,                -- Wind speed at 700 hPa
    ws_500 FLOAT,                -- Wind speed at 500 hPa
    ws_200 FLOAT,                -- Wind speed at 200 hPa
    
    -- Wind direction at different pressure levels
    wd_10m FLOAT,                -- Wind direction at 10m
    wd_925 FLOAT,                -- Wind direction at 925 hPa
    wd_850 FLOAT,                -- Wind direction at 850 hPa
    wd_700 FLOAT,                -- Wind direction at 700 hPa
    wd_500 FLOAT,                -- Wind direction at 500 hPa
    wd_200 FLOAT,                -- Wind direction at 200 hPa
    
    -- Cloud cover
    cloudlow FLOAT,              -- Low cloud cover (%)
    cloudmed FLOAT,              -- Medium cloud cover (%)
    cloudhigh FLOAT,             -- High cloud cover (%)
    
    -- Weather condition
    cond INTEGER,                -- Weather condition code
    wdfn VARCHAR(50),            -- Wind direction full name
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicates
    UNIQUE(coordinate_id, date, hour)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_forecasts_coord_date_hour 
ON forecasts(coordinate_id, date, hour);

CREATE INDEX IF NOT EXISTS idx_forecasts_date 
ON forecasts(date);

CREATE INDEX IF NOT EXISTS idx_forecasts_created_at 
ON forecasts(created_at);

CREATE INDEX IF NOT EXISTS idx_coordinates_active 
ON coordinates(is_active) WHERE is_active = TRUE;

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to both tables
DROP TRIGGER IF EXISTS update_coordinates_updated_at ON coordinates;
CREATE TRIGGER update_coordinates_updated_at 
    BEFORE UPDATE ON coordinates 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_forecasts_updated_at ON forecasts;
CREATE TRIGGER update_forecasts_updated_at 
    BEFORE UPDATE ON forecasts 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert sample coordinates for testing
INSERT INTO coordinates (latitude, longitude, location_name) VALUES
(21.0285, 105.8542, 'Hanoi, Vietnam'),
(10.8231, 106.6297, 'Ho Chi Minh City, Vietnam'),
(16.4637, 107.5909, 'Hue, Vietnam'),
(15.8801, 108.338, 'Da Nang, Vietnam'),
(12.2388, 109.1967, 'Nha Trang, Vietnam')
ON CONFLICT DO NOTHING;

-- Create view for latest forecasts
CREATE OR REPLACE VIEW latest_forecasts AS
SELECT 
    f.*,
    c.location_name,
    c.latitude,
    c.longitude
FROM forecasts f
JOIN coordinates c ON f.coordinate_id = c.id
WHERE f.date >= CURRENT_DATE
AND c.is_active = TRUE
ORDER BY f.coordinate_id, f.date, f.hour;

-- Create view for data quality monitoring
CREATE OR REPLACE VIEW forecast_data_quality AS
SELECT 
    DATE(created_at) as ingestion_date,
    COUNT(*) as total_records,
    COUNT(tc) as records_with_temperature,
    COUNT(rh) as records_with_humidity,
    COUNT(slp) as records_with_pressure,
    ROUND(COUNT(tc)::numeric / COUNT(*)::numeric * 100, 2) as temperature_completeness_pct,
    ROUND(COUNT(rh)::numeric / COUNT(*)::numeric * 100, 2) as humidity_completeness_pct,
    ROUND(COUNT(slp)::numeric / COUNT(*)::numeric * 100, 2) as pressure_completeness_pct
FROM forecasts
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(created_at)
ORDER BY ingestion_date DESC;