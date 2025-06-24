-- -- WITH locations AS (
-- -- 	SELECT 
-- -- 		start_station_id AS station_id,
-- -- 		start_station_latitude AS latitude,
-- -- 		start_station_longitude AS longitude
-- -- 	FROM bronze.bike_trips
-- -- 	UNION
-- -- 	SELECT 
-- -- 		end_station_id AS station_id,
-- -- 		end_station_latitude AS latitude,
-- -- 		end_station_longitude AS longitude
-- -- 	FROM bronze.bike_trips
-- -- )

-- -- SELECT 
-- -- 	l.*,
-- -- 	AVG(l.latitude) OVER(PARTITION BY l.station_id) AS avg_latitude,
-- -- 	AVG(l.longitude) OVER(PARTITION BY l.station_id) AS avg_longitude
-- -- FROM locations AS l
-- -- ORDER BY station_id;
-- WITH pre_l AS (
-- 	SELECT 
-- 		start_station_id AS station_id,
-- 		MAX(start_time) AS trip_time,
-- 		start_station_latitude AS latitude,
-- 		start_station_longitude AS longitude
-- 	FROM bronze.bike_trips
-- 	GROUP BY
-- 		start_station_id, 
-- 		start_station_latitude, 
-- 		start_station_longitude
-- 	UNION
-- 	SELECT 
-- 		end_station_id AS station_id,
-- 		MAX(end_time) AS trip_time,
-- 		end_station_latitude AS latitude,
-- 		end_station_longitude AS longitude
-- 	FROM bronze.bike_trips
-- 	GROUP BY 
-- 		end_station_id, 
-- 		end_station_latitude, 
-- 		end_station_longitude
-- ), 
-- locations AS (
-- 	SELECT 
-- 	SELECT 
-- 		pl.station_id,
-- 		RANK() OVER(PARTITION BY pl.station_id ORDER BY pl.trip_time DESC) AS ranking
-- 	FROM pre_l AS pl
-- 	GROUP BY pl.station_id
-- ),
-- latest_locs AS (
-- 	SELECT
-- 		l.station_id, 
-- 		l.latitude,
-- 		l.longitude,
-- 		MAX(l.trip_time)
-- 	FROM locations AS l
-- 	GROUP BY l.station_id, l.latitude, l.longitude
-- )

-- SELECT *
-- FROM latest_locs
WITH locations AS (	
	SELECT 
		start_station_id AS station_id,
		start_time AS trip_time,
		start_station_latitude AS latitude,
		start_station_longitude AS longitude
	FROM bronze.bike_trips
	UNION 
	SELECT 
		end_station_id AS station_id,
		end_time AS trip_time,
		end_station_latitude AS latitude,
		end_station_longitude AS longitude
	FROM bronze.bike_trips
),
U_loc AS (
	SELECT
		*,
		DENSE_RANK() OVER(PARTITION BY station_id ORDER BY trip_time DESC) AS RANk_U
	FROM locations
)
SELECT 
	DISTINCT
	ul.station_id,
	l.latitude AS latitude,
	l.longitude AS longitude,
	ul.latitude AS latest_latitude,
	ul.longitude AS latest_longitude
FROM U_loc AS ul
LEFT JOIN locations AS l
	ON l.station_id = ul.station_id
WHERE ul.RANK_U = 1 AND ul.station_id IS NOT NULL;