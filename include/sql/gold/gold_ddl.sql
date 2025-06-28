-- Create the gold schema if it doesn't already exist
CREATE SCHEMA IF NOT EXISTS gold;

------------------------------------------------------------------------------------
-- Step 1: Create the Materialized Views
-- This only needs to be done once. These are the objects Power BI will connect to.
------------------------------------------------------------------------------------


-- Materialized View 1: Daily Trip Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.dm_daily_trip_summary AS
SELECT
    EXTRACT(HOUR FROM ft.start_time) AS start_hour,
    TO_DATE(d.date_id::TEXT, 'YYYYMMDD') AS date,
    d.year,
    d.month_name,
    d.day_name,
    d.is_weekend,
    COUNT(ft.trip_id) AS total_trips,
    SUM(ft.duration_min) AS total_duration_min,
    AVG(ft.duration_min) AS avg_duration_min,
    COUNT(DISTINCT ft.bike_id) AS unique_bikes_used
FROM silver.fact_trips ft
JOIN silver.dim_date d ON ft.start_date_id = d.date_id
GROUP BY
    EXTRACT(HOUR FROM ft.start_time), d.date_id, d.year, d.month_name, d.day_name, d.is_weekend;

-- Materialized View 2: Station Popularity
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.dm_station_popularity AS
WITH starts AS (
    SELECT start_station_name AS station_name, COUNT(trip_id) AS total_starts
    FROM silver.fact_trips
    GROUP BY start_station_name
),
ends AS (
    SELECT end_station_name AS station_name, COUNT(trip_id) AS total_ends
    FROM silver.fact_trips
    GROUP BY end_station_name
)
SELECT
    COALESCE(s.station_name, e.station_name) AS station_name,
    (ARRAY_AGG(loc.city ORDER BY ft.start_time DESC LIMIT 1))[1] AS city,
    (ARRAY_AGG(loc.state ORDER BY ft.start_time DESC LIMIT 1))[1] AS state,
    (ARRAY_AGG(loc.latitude ORDER BY ft.start_time DESC LIMIT 1))[1] AS latitude,
    (ARRAY_AGG(loc.longitude ORDER BY ft.start_time DESC LIMIT 1))[1] AS longitude,
    COALESCE(s.total_starts, 0) AS total_trips_started,
    COALESCE(e.total_ends, 0) AS total_trips_ended,
    (COALESCE(s.total_starts, 0) - COALESCE(e.total_ends, 0)) AS net_flow,
    (COALESCE(s.total_starts, 0) + COALESCE(e.total_ends, 0)) AS total_trips
FROM starts s
FULL OUTER JOIN ends e ON s.station_name = e.station_name
LEFT JOIN silver.fact_trips ft ON COALESCE(s.station_name, e.station_name) = ft.start_station_name
LEFT JOIN silver.dim_locations loc ON ft.start_location_id = loc.location_id
WHERE COALESCE(s.station_name, e.station_name) IS NOT NULL
GROUP BY COALESCE(s.station_name, e.station_name), s.total_starts, e.total_ends;

-- Materialized View 3: Popular Routes
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.dm_popular_routes AS
SELECT
    ft.start_station_name || ' -> ' || ft.end_station_name AS route_id,
    ft.start_station_name,
    ft.end_station_name,
    COUNT(ft.trip_id) AS trip_count,
    AVG(ft.duration_min) AS avg_duration_min
FROM silver.fact_trips AS ft
JOIN 
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY
    start_station_name,
    end_station_name;

-- Materialized View 4: User Behavior Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.dm_user_behavior_summary AS
SELECT
    ut.user_type,
    ut.member_gender,
    ut.bike_share_for_all_trip,
    (dt.year - ut.member_birth_year) AS age
    COUNT(ft.trip_id) AS total_trips,
    SUM(ft.duration_min) AS total_duration_min,
    AVG(ft.duration_min) AS avg_duration_min
FROM silver.fact_trips ft
JOIN silver.dim_user_types ut ON ft.user_type_id = ut.user_type_id
JOIN silver.dim_date dt ON ft.date_id = dt.date_id 
GROUP BY
    ut.user_type,
    ut.member_gender,
    ut.bike_share_for_all_trip;

-- normal View 1: gold.dim_locations_view
CREATE OR REPLACE VIEW gold.dim_locations_view AS
SELECT 
    location_id AS id,
    latitude AS lat,
    longitude AS lng,
    highway AS highway_name,
    road AS road_name,
    neighbourhood AS neighborhood,
    suburb AS suburb_name,
    city AS city_name,
    state AS state_name,
    postcode AS postal_code,
    country AS country_name,
    display_name AS full_display_name
FROM silver.dim_locations;


--normal View 2: gold.dim_user_types_view

CREATE OR REPLACE VIEW gold.dim_user_types_view AS
SELECT 
    user_type_id AS id,
    user_type AS user_category,
    member_birth_year AS birth_year,
    member_gender AS gender,
    bike_share_for_all_trip AS bike_share_option
FROM silver.dim_user_types;

-- noraml View 3: gold.dim_date_view

CREATE OR REPLACE VIEW gold.dim_date_view AS
SELECT 
    date_id AS id,
    year AS year_number,
    month AS month_number,
    month_name AS month_name_text,
    day AS day_number,
    quarter AS quarter_number,
    day_of_week AS weekday_number,
    day_name AS weekday_name,
    is_weekend AS weekend_flag
FROM silver.dim_date;

-- normal View 4: gold.fact_trips_view

CREATE OR REPLACE VIEW gold.fact_trips_view AS
SELECT 
    trip_id AS id,
    duration_min AS duration_minutes,
    start_location_id AS start_location,
    start_date_id AS start_date,
    start_time AS start_time_of_day,
    start_station_name AS start_station,
    end_location_id AS end_location,
    end_date_id AS end_date,
    end_time AS end_time_of_day,
    end_station_name AS end_station,
    bike_id AS bike_identifier,
    user_type_id AS user_type
FROM silver.fact_trips;


------------------------------------------------------------------------------------
-- Step 2: Create a Procedure to Refresh the Data
-- This procedure should be run on a schedule (e.g., nightly).
------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE gold.refresh_dm_daily_trip_summary()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Refreshing gold.dm_daily_trip_summary...';
    REFRESH MATERIALIZED VIEW gold.dm_daily_trip_summary;
    RAISE NOTICE 'Done.';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error refreshing gold.dm_daily_trip_summary: %', SQLERRM;
END;
$$;
CREATE OR REPLACE PROCEDURE gold.refresh_dm_station_popularity()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Refreshing gold.dm_station_popularity...';
    REFRESH MATERIALIZED VIEW gold.dm_station_popularity;
    RAISE NOTICE 'Done.';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error refreshing gold.dm_station_popularity: %', SQLERRM;
END;
$$;
CREATE OR REPLACE PROCEDURE gold.refresh_dm_popular_routes()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Refreshing gold.dm_popular_routes...';
    REFRESH MATERIALIZED VIEW gold.dm_popular_routes;
    RAISE NOTICE 'Done.';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error refreshing gold.dm_popular_routes: %', SQLERRM;
END;
$$;
CREATE OR REPLACE PROCEDURE gold.refresh_dm_user_behavior_summary()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Refreshing gold.dm_user_behavior_summary...';
    REFRESH MATERIALIZED VIEW gold.dm_user_behavior_summary;
    RAISE NOTICE 'Done.';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error refreshing gold.dm_user_behavior_summary: %', SQLERRM;
END;
$$;