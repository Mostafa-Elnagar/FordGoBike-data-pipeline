CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE PROCEDURE silver.create_silver_schema_and_tables()
LANGUAGE plpgsql
AS $$
BEGIN
  -- dim_locations
  EXECUTE '
    CREATE TABLE IF NOT EXISTS silver.dim_locations (
      location_id BIGINT PRIMARY KEY,
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
      display_name VARCHAR(256),
      station_name VARCHAR(256)
    )';

  -- dim_user_types
  EXECUTE '
    CREATE TABLE IF NOT EXISTS silver.dim_user_types (
      user_type_id BIGINT PRIMARY KEY,
      user_type VARCHAR(50),
      member_birth_year INTEGER,
      member_gender VARCHAR(20),
      bike_share_for_all_trip VARCHAR(20)
    )';

  -- dim_date
  EXECUTE '
    CREATE TABLE IF NOT EXISTS silver.dim_date (
      date_id INT PRIMARY KEY,
      year INT NOT NULL,
      month INT NOT NULL,
      month_name TEXT NOT NULL,
      day INT NOT NULL,
      quarter INT NOT NULL,
      day_of_week INT NOT NULL,
      day_name TEXT NOT NULL,
      is_weekend BOOLEAN NOT NULL
    )';

  -- fact_trips
  EXECUTE '
    CREATE TABLE IF NOT EXISTS silver.fact_trips (
      trip_id INTEGER PRIMARY KEY,
      duration_min INT,
      start_location_id BIGINT,
      start_date_id INT,
      start_time TIME,
      end_location_id BIGINT,
      end_date_id INT,
      end_time TIME,
      bike_id VARCHAR(50),
      user_type_id BIGINT,
      CONSTRAINT fk_start_date FOREIGN KEY (start_date_id) REFERENCES silver.dim_date(date_id),
      CONSTRAINT fk_end_date FOREIGN KEY (end_date_id) REFERENCES silver.dim_date(date_id),
      CONSTRAINT fk_start_location FOREIGN KEY (start_location_id) REFERENCES silver.dim_locations(location_id),
      CONSTRAINT fk_end_location FOREIGN KEY (end_location_id) REFERENCES silver.dim_locations(location_id),
      CONSTRAINT fk_user_type FOREIGN KEY (user_type_id) REFERENCES silver.dim_user_types(user_type_id)
    )';

  -- indexes
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fact_trips_start_location ON silver.fact_trips(start_location_id)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fact_trips_end_location ON silver.fact_trips(end_location_id)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fact_trips_user_type ON silver.fact_trips(user_type_id)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fact_trips_start_date ON silver.fact_trips(start_date_id)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fact_trips_end_date ON silver.fact_trips(end_date_id)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fact_trips_bike_id ON silver.fact_trips(bike_id)';

EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Error creating silver schema or tables: %', SQLERRM;
END;
$$;

CALL silver.create_silver_schema_and_tables();
