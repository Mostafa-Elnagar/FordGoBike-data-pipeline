CREATE OR REPLACE PROCEDURE silver.run_full_etl()
LANGUAGE plpgsql
AS $$
BEGIN
    -- ================================================
    -- 🚀 Start ETL Process
    -- ================================================
    RAISE NOTICE 'Starting ETL Process for Silver Layer...';

    -- ================================================
    -- 1. 📍 Load new locations into silver.dim_locations
    -- ================================================
    RAISE NOTICE 'Step 1: Loading new locations into silver.dim_locations...';
    BEGIN
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
        RAISE NOTICE 'Step 1 completed successfully.';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error in Step 1: %', SQLERRM;
            RETURN;
    END;

    -- ================================================
    -- 2. 👥 Load user type dimension into silver.dim_user_types
    -- ================================================
    RAISE NOTICE 'Step 2: Loading user types into silver.dim_user_types...';
    BEGIN
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
                (SELECT unnest(ARRAY['Male', 'Female', 'Other', 'Unknown']) AS gender) AS g,
                (SELECT unnest(ARRAY['No', 'Yes']) AS bike_share) AS bs
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

        RAISE NOTICE 'Step 2 completed successfully.';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error in Step 2: %', SQLERRM;
            RETURN;
    END;

    -- ================================================
    -- 3. 📅 Load silver.dim_date with missing dates
    -- ================================================
    RAISE NOTICE 'Step 3: Loading dates into silver.dim_date...';
    BEGIN
        INSERT INTO silver.dim_date (
            date_id, 
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
            (EXTRACT(YEAR FROM date)::INT * 10000) +
			(EXTRACT(MONTH FROM date)::INT * 100) +
			EXTRACT(DAY FROM date)::INT AS date_id,
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
        FROM dates
        ON CONFLICT (date_id) DO NOTHING;

        RAISE NOTICE 'Step 3 completed successfully.';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error in Step 3: %', SQLERRM;
            RETURN;
    END;

    -- ================================================
    -- 4. 🚴 Load transformed bike trips into silver.fact_trips
    -- ================================================
    RAISE NOTICE 'Step 4: Loading bike trips into silver.fact_trips...';
    BEGIN
        WITH temp_bike_trips AS (
            SELECT
                trip_id,
                ROUND(duration_sec / 60.0)::INT AS duration_min,
                start_time::date AS start_date_trip,
                start_time::TIME AS start_time,
                NULLIF(start_station_name, 'NULL') AS start_station_name,
                end_time::date AS end_date_trip,
                end_time::TIME AS end_time,
                NULLIF(end_station_name, 'NULL') AS end_station_name,
                start_station_latitude,
                start_station_longitude,
                end_station_latitude,
                end_station_longitude,
                bike_id AS bike_id,
                user_type,
                (
                    CASE 
                        WHEN (EXTRACT(YEAR FROM start_time)::INT - member_birth_year) < 18 THEN NULL
                        WHEN (EXTRACT(YEAR FROM start_time)::INT - member_birth_year) > 100 THEN NULL
                        ELSE  member_birth_year
                    END
                ) AS member_birth_year,
                COALESCE(member_gender, 'Unknown') AS member_gender,
                COALESCE(bike_share_for_all_trip, 'No') AS bike_share_for_all_trip
            FROM bronze.bike_trips AS bt
            WHERE loaded_to_silver = FALSE
        )
        INSERT INTO silver.fact_trips
        (
            trip_id,
            duration_min,
            start_location_id,
            start_date_id,
            start_time,
            start_station_name,
            end_location_id,
            end_date_id,
            end_time,
            end_station_name,
            bike_id,
            user_type_id
        )
        SELECT
            bt.trip_id,
            bt.duration_min,
            sl.location_id AS start_location_id,
			(EXTRACT(YEAR FROM bt.start_date_trip)::INT * 10000) +
			(EXTRACT(MONTH FROM bt.start_date_trip)::INT * 100) +
			EXTRACT(DAY FROM bt.start_date_trip)::INT AS start_date_id,
            bt.start_time,
            bt.start_station_name,
            el.location_id AS end_location_id,
			(EXTRACT(YEAR FROM bt.end_date_trip)::INT * 10000) +
			(EXTRACT(MONTH FROM bt.end_date_trip)::INT * 100) +
			EXTRACT(DAY FROM bt.end_date_trip)::INT AS end_date_id,
            bt.end_time,
            bt.end_station_name,
            bt.bike_id,
            ('x' || substr(md5(
                    COALESCE(bt.user_type, '') || '|' ||
                    COALESCE(bt.member_birth_year::TEXT, '') || '|' ||
                    COALESCE(bt.member_gender, '') || '|' ||
                    COALESCE(bt.bike_share_for_all_trip::TEXT, '')
                ), 1, 16))::bit(64)::BIGINT AS user_type_id
        FROM temp_bike_trips bt
        LEFT JOIN bronze.locations sl
            ON bt.start_station_latitude = sl.latitude
            AND bt.start_station_longitude = sl.longitude
        LEFT JOIN bronze.locations el
            ON bt.end_station_latitude = el.latitude
            AND bt.end_station_longitude = el.longitude;

        RAISE NOTICE 'Step 4 completed successfully.';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error in Step 4: %', SQLERRM;
            RETURN;
    END;

    -- ================================================
    -- 5. ✅ Update bronze.bike_trips to mark as loaded
    -- ================================================
    RAISE NOTICE 'Step 5: Updating bronze.bike_trips to mark as loaded...';
    BEGIN
        UPDATE bronze.bike_trips
        SET loaded_to_silver = TRUE
        WHERE loaded_to_silver = FALSE;

        RAISE NOTICE 'Step 5 completed successfully.';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error in Step 5: %', SQLERRM;
            RETURN;
    END;

    -- ================================================
    -- 🎉 ETL Process Completed Successfully
    -- ================================================
    RAISE NOTICE 'ETL Process completed successfully!';

END;
$$;
CALL silver.run_full_etl();