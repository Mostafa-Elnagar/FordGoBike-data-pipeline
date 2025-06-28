-- Create the gold schema if it doesn't already exist
CREATE SCHEMA IF NOT EXISTS gold;

------------------------------------------------------------------------------------
-- Step 1: Create the Materialized Views
-- This only needs to be done once. These are the objects Power BI will connect to.
------------------------------------------------------------------------------------

-- Materialized View 1: Daily Trip Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.dm_daily_trip_summary AS
SELECT
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
    d.date_id, d.year, d.month_name, d.day_name, d.is_weekend;

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
    MAX(loc.city) AS city,
    MAX(loc.state) AS state,
    MAX(loc.latitude) AS latitude,
    MAX(loc.longitude) AS longitude,
    COALESCE(s.total_starts, 0) AS total_trips_started,
    COALESCE(e.total_ends, 0) AS total_trips_ended,
    (COALESCE(s.total_starts, 0) - COALESCE(e.total_ends, 0)) AS net_flow
FROM starts s
FULL OUTER JOIN ends e ON s.station_name = e.station_name
LEFT JOIN silver.fact_trips ft ON COALESCE(s.station_name, e.station_name) = ft.start_station_name
LEFT JOIN silver.dim_locations loc ON ft.start_location_id = loc.location_id
WHERE COALESCE(s.station_name, e.station_name) IS NOT NULL
GROUP BY COALESCE(s.station_name, e.station_name), s.total_starts, e.total_ends;

-- Materialized View 3: Popular Routes
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.dm_popular_routes AS
SELECT
    start_station_name || ' -> ' || end_station_name AS route_id,
    start_station_name,
    end_station_name,
    COUNT(trip_id) AS trip_count,
    AVG(duration_min) AS avg_duration_min
FROM silver.fact_trips
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
    COUNT(ft.trip_id) AS total_trips,
    SUM(ft.duration_min) AS total_duration_min,
    AVG(ft.duration_min) AS avg_duration_min
FROM silver.fact_trips ft
JOIN silver.dim_user_types ut ON ft.user_type_id = ut.user_type_id
GROUP BY
    ut.user_type,
    ut.member_gender,
    ut.bike_share_for_all_trip;

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