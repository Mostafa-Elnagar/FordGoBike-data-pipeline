-- Create the gold schema if it doesn't already exist
CREATE SCHEMA IF NOT EXISTS gold;

------------------------------------------------------------------------------------
-- Step 1: Create the Materialized Views
-- This only needs to be done once. These are the objects Power BI will connect to.
------------------------------------------------------------------------------------


-- Materialized View 1:  Trip Summary
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

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.dm_station_popularity AS

-- CTE to count all trips starting from each distinct station.
-- This assumes a standard star schema where fact tables join to dimension tables on IDs.
WITH starts AS (
    SELECT
        dl.station_name,
        COUNT(ft.trip_id) AS total_starts
    FROM silver.fact_trips AS ft
    -- Join with the locations dimension on the starting location ID to get the station name.
    JOIN silver.dim_locations AS dl ON ft.start_location_id = dl.location_id
    GROUP BY dl.station_name
),

-- CTE to count all trips ending at each distinct station.
ends AS (
    SELECT
        dl.station_name,
        COUNT(ft.trip_id) AS total_ends
    FROM silver.fact_trips AS ft
    -- Join with the locations dimension on the ending location ID to get the station name.
    JOIN silver.dim_locations AS dl ON ft.end_location_id = dl.location_id
    GROUP BY dl.station_name
),

-- CTE to get unique station details (city, coordinates, etc.).
-- This is more efficient than joining back to the main fact table later.
station_details AS (
    SELECT
        station_name,
        -- Use an aggregate function (like MAX) to ensure one row per station,
        -- handling potential duplicate entries in the dimension table gracefully.
        MAX(city) AS city,
        MAX(latitude) AS latitude,
        MAX(longitude) AS longitude,
        MAX(display_name) AS display_name
    FROM silver.dim_locations
    WHERE station_name IS NOT NULL
    GROUP BY station_name
)

-- Final SELECT statement to combine the aggregated data into the final view.
SELECT
    -- Use COALESCE to get the station name from either the starts or ends CTE.
    -- This handles stations that are only used for starts or only for ends.
    COALESCE(s.station_name, e.station_name) AS station_name,
    sd.city,
    sd.latitude,
    sd.longitude,
    sd.display_name,
    -- If a station has no starts or no ends, COALESCE ensures the count is 0 instead of NULL.
    COALESCE(s.total_starts, 0) AS total_trips_started,
    COALESCE(e.total_ends, 0) AS total_trips_ended,
    -- Calculate the net flow of bikes (positive means more bikes left than arrived).
    (COALESCE(s.total_starts, 0) - COALESCE(e.total_ends, 0)) AS net_flow,
    -- Calculate the total number of trips (starts + ends) associated with the station.
    (COALESCE(s.total_starts, 0) + COALESCE(e.total_ends, 0)) AS total_trips
FROM starts s
-- A FULL OUTER JOIN ensures that we don't miss any stations that only have starts or only have ends.
FULL OUTER JOIN ends e ON s.station_name = e.station_name
-- Cleanly join the station details using the definitive station name.
LEFT JOIN station_details sd ON sd.station_name = COALESCE(s.station_name, e.station_name)
-- Filter out any potential NULL station names that might result from data quality issues.
WHERE COALESCE(s.station_name, e.station_name) IS NOT NULL;

-- Materialized View 3: Popular Routes
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.dm_popular_routes AS
SELECT
    sl.station_name || ' -> ' || el.station_name AS route_id,
    sl.station_name AS start_station_name,
    el.station_name AS end_station_name,
    ft.start_location_id,
    ft.end_location_id,
    COUNT(ft.trip_id) AS trip_count,
    AVG(ft.duration_min) AS avg_duration_min
FROM silver.fact_trips AS ft
LEFT JOIN silver.dim_locations AS sl
    ON ft.start_location_id = sl.location_id
LEFT JOIN silver.dim_locations AS el
    ON ft.end_location_id = el.location_id
WHERE sl.station_name IS NOT NULL AND el.station_name IS NOT NULL
GROUP BY
    sl.station_name,
    el.station_name,
    ft.start_location_id,
    ft.end_location_id;

-- Materialized View 4: User Behavior Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.dm_user_behavior_summary AS
SELECT
    ut.user_type,
    ut.member_gender,
    ut.bike_share_for_all_trip,
    (dt.year - ut.member_birth_year) AS age,
    COUNT(ft.trip_id) AS total_trips,
    SUM(ft.duration_min) AS total_duration_min,
    AVG(ft.duration_min) AS avg_duration_min
FROM silver.fact_trips ft
JOIN silver.dim_user_types ut ON ft.user_type_id = ut.user_type_id
JOIN silver.dim_date dt ON ft.start_date_id = dt.date_id 
GROUP BY
    ut.user_type,
    ut.member_gender,
    ut.bike_share_for_all_trip,
    (dt.year - ut.member_birth_year);

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
    display_name AS full_address,
    station_name
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
    end_location_id AS end_location,
    end_date_id AS end_date,
    end_time AS end_time_of_day,
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