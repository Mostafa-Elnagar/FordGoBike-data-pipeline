INSERT INTO silver.dim_locations(
	location_id,
	latitude,
	longitude,
	highway,
	road,
	neighbourhood,
	suburb,
	city,
	state,
	postcode,
	country,
	display_name
)
SELECT DISTINCT ON (location_id) *
FROM bronze.locations
WHERE location_id NOT IN (SELECT location_id FROM silver.dim_locations);

-- Data Transformations on Trips Table
CREATE TEMP TABLE temp_updated_bike_trips 
ON COMMIT PRESERVE ROWS
AS 
SELECT
    trip_id,
    ROUND(duration_sec / 60.0, 0)::INTEGER AS duration_min,
    DATE_TRUNC('day', start_time)::date AS start_date,
    DATE_TRUNC('day', end_time)::date AS end_date,
    start_time::TIME AS start_time,
    end_time::TIME AS end_time,
    start_station_name,
    start_station_latitude::FLOAT,
    start_station_longitude::FLOAT,
    end_station_name,
    end_station_latitude::FLOAT,
    end_station_longitude::FLOAT,
    bike_id,
    ('x' || substr(md5(
            COALESCE(user_type, '') || '|' ||
            COALESCE(member_birth_year::TEXT, '') || '|' ||
            COALESCE(member_gender, '') || '|' ||
            (
				CASE 
					WHEN bike_share_for_all_trip = 'Yes' THEN True
					ELSE False
				END
			)::TEXT
        ), 1, 16))::bit(64)::bigint AS user_type_id
FROM bronze.bike_trips
WHERE NOT loaded_to_silver;


-- LOAD USER TYPES DIMENSION
INSERT INTO silver.dim_user_types (
    user_type_id,
    user_type,
    member_birth_year,
    member_gender,
    bike_share_for_all_trip
)
WITH bounds AS (
    SELECT
        MIN(member_birth_year)::INT AS min_birth,
        MAX(member_birth_year)::INT AS max_birth
    FROM bronze.bike_trips
    WHERE NOT loaded_to_silver
),
birth_years AS (
    SELECT generate_series(min_birth, max_birth) AS birth_year
    FROM bounds
    UNION ALL
    SELECT NULL
),
all_combinations AS (
    SELECT 
        u.user_type,
        b.birth_year,
        g.gender,
        bs.bike_share
    FROM 
        (SELECT unnest(ARRAY['Customer', 'Subscriber']) AS user_type) AS u,
        birth_years AS b,
        (SELECT unnest(ARRAY['Male', 'Female', 'Other', NULL]) AS gender) AS g,
        (SELECT unnest(ARRAY[TRUE, FALSE]) AS bike_share) AS bs
),
final_data AS (
    SELECT 
        user_type,
        birth_year,
        gender,
        bike_share,

        ('x' || substr(md5(
            COALESCE(user_type, '') || '|' ||
            COALESCE(birth_year::TEXT, '') || '|' ||
            COALESCE(gender, '') || '|' ||
            COALESCE(bike_share::TEXT, 'false')
        ), 1, 16))::bit(64)::bigint AS user_type_id
		FROM all_combinations
)
SELECT
    user_type_id,
    user_type,
    birth_year,
    gender,
    bike_share
FROM final_data
ON CONFLICT DO NOTHING;


-- LOAD DATE DIMENSION  
DELETE FROM silver.dim_date;

INSERT INTO silver.dim_date (
    date,
    year,
    month,
    month_name,
    day,
    quarter,
    day_of_week,
    day_name,
    is_weekend
)
WITH min_max_date AS (
	SELECT 
		MIN(LEAST(start_time, end_time))::DATE AS min_date,
		MAX(GREATEST(start_time, end_time))::DATE AS max_date
	FROM bronze.bike_trips
),
dates AS (
	SELECT 
		generate_series(min_date, max_date, interval '1 day')::DATE AS date
	FROM min_max_date
)
SELECT 
    date,
    EXTRACT(YEAR FROM date)::INT AS year,
    EXTRACT(MONTH FROM date)::INT AS month,
    TRIM(TO_CHAR(date, 'Month')) AS month_name,
    EXTRACT(DAY FROM date)::INT AS day,
    EXTRACT(QUARTER FROM date)::INT AS quarter,
    EXTRACT(DOW FROM date)::INT AS day_of_week,
    TRIM(TO_CHAR(date, 'Day')) AS day_name,
    CASE 
        WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM dates;

-- LOAD FACT TABLE
INSERT INTO silver.fact_trips
(
	trip_id,
	duration_min,
	start_date,
	end_date,
	start_time,
	end_time,
	start_station_name,
	end_station_name,
	bike_id,
	user_type_id,
	start_location_id,
	end_location_id
)
SELECT
	t.trip_id,
	t.duration_min,
	t.start_date,
	t.end_date,
	t.start_time,
	t.end_time,
	t.start_station_name,
	t.end_station_name,
	t.bike_id,
	t.user_type_id,
	sl.location_id AS start_location_id,
	el.location_id AS end_location_id
FROM temp_updated_bike_trips AS t
LEFT JOIN bronze.locations as sl
	ON t.start_station_latitude = sl.latitude
		AND t.start_station_longitude = sl.longitude
LEFT JOIN bronze.locations as el
	ON t.end_station_latitude = el.latitude
		AND t.end_station_longitude = el.longitude;

-- DROP TEMP TABLE
DROP TABLE IF EXISTS temp_updated_bike_trips;