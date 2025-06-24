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

-- INSERT INTO silver.dim_user_types (
-- 	user_type_id,
-- 	user_type,
-- 	member_birth_year,
-- 	member_gender,
-- 	bike_share_for_all_trip
-- )
-- SELECT 
-- 	DISTINCT
-- 	user_type,
-- 	member_birth_year,
-- 	member_gender,
-- 	bike_share_for_all_trip
-- FROM bronze.bike_trips
-- WHERE 

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
        (SELECT unnest(ARRAY['Male', 'Female', 'Unknown']) AS gender) AS g,
        (SELECT unnest(ARRAY[TRUE, FALSE]) AS bike_share) AS bs
),
final_data AS (
    SELECT 
        user_type,
        birth_year,
        gender,
        bike_share,
        -- Generate a consistent surrogate key by hashing the values
        ('x' || substr(md5(
            COALESCE(user_type, '') || '|' ||
            COALESCE(birth_year::TEXT, '') || '|' ||
            COALESCE(gender, '') || '|' ||
            COALESCE(bike_share::TEXT, '')
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

TRUNCATE TABLE silver.dim_date;
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
WITH min_max_date AS(
	SELECT
		DATE_TRUNC('year', start_time) as Date
	FROM bronze.bike_trips
	UNION
	SELECT
		DATE_TRUNC('year', end_time) as Date
	FROM bronze.bike_trips
),
dates AS (
	SELECT 
		generate_series(MIN(date)::DATE, MAX(date)::DATE, '1 day'::interval)::date AS date
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

SELECT * FROM silver.dim_date;