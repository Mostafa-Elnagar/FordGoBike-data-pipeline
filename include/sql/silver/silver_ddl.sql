
CREATE SCHEMA IF NOT EXISTS silver;

-- DROP TABLE IF EXISTS silver.fact_trips;
-- DROP TABLE IF EXISTS silver.dim_locations;
-- DROP TABLE IF EXISTS silver.dim_user_types;
-- DROP TABLE IF EXISTS silver.dim_date;


CREATE TABLE IF NOT EXISTS silver.dim_locations (
	location_id VARCHAR(50) PRIMARY KEY,
	latitude FLOAT,
	longitude FLOAT,
	highway VARCHAR(256),
	road VARCHAR(256),
	neighbourhood VARCHAR(256),
	suburb VARCHAR(256),
	city VARCHAR(256),
	state VARCHAR(256),
	postcode VARCHAR(50),
	country VARCHAR(256),
	display_name VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS silver.dim_user_types (
	user_type_id BIGINT PRIMARY KEY,
	user_type VARCHAR(50),
	member_birth_year INTEGER,
	member_gender VARCHAR(20),
	bike_share_for_all_trip BOOLEAN
);	

CREATE TABLE IF NOT EXISTS silver.dim_date (
    date DATE PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    month_name TEXT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.Fact_Trips(
	trip_id INTEGER PRIMARY KEY,
	duration_min INTEGER,
	start_time TIME,
	end_time TIME,
	start_date DATE,
	end_date DATE,
	start_station_name VARCHAR(256),
	end_station_name VARCHAR(256),
	bike_id VARCHAR(50),
	user_type_id BIGINT,
	start_location_id VARCHAR(50),
	end_location_id VARCHAR(50),
	FOREIGN KEY (user_type_id)
	REFERENCES silver.dim_user_types(user_type_id)
	ON UPDATE CASCADE,
	FOREIGN KEY (start_location_id)
	REFERENCES silver.dim_locations(location_id)
	ON UPDATE CASCADE,
	FOREIGN KEY (end_location_id)
	REFERENCES silver.dim_locations(location_id)
	ON UPDATE CASCADE,
	FOREIGN KEY (start_date)
	REFERENCES silver.dim_date(date),
	FOREIGN KEY (end_date)
	REFERENCES silver.dim_date(date)
);