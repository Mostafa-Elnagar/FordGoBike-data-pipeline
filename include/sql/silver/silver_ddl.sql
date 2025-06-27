CREATE OR REPLACE PROCEDURE silver.create_silver_schema_and_tables()
LANGUAGE plpgsql
AS $$
BEGIN
  -- 1. Create schema dynamically
  CREATE SCHEMA IF NOT EXISTS silver;

  -- 2. Create tables dynamically
  EXECUTE $ddl$
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
  $ddl$;

  EXECUTE $ddl$
    CREATE TABLE IF NOT EXISTS silver.dim_user_types (
      user_type_id BIGINT PRIMARY KEY,
      user_type VARCHAR(50),
      member_birth_year INTEGER,
      member_gender VARCHAR(20),
      bike_share_for_all_trip VARCHAR(20)
    );
  $ddl$;

  EXECUTE $ddl$
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
  $ddl$;

  EXECUTE $ddl$
    CREATE TABLE IF NOT EXISTS silver.fact_trips (
      trip_id INTEGER PRIMARY KEY,
      duration_min INT,
      start_location_id VARCHAR(50),
      start_date_trip DATE,
      start_time TIME,
      start_station_name VARCHAR(256),
      end_location_id VARCHAR(50),
      end_date_trip DATE,
      end_time TIME,
      end_station_name VARCHAR(256),
      bike_id VARCHAR(50),
      user_type_id BIGINT,
      CONSTRAINT fk_start_date FOREIGN KEY (start_date_trip) REFERENCES silver.dim_date(date),
      CONSTRAINT fk_end_date FOREIGN KEY (end_date_trip) REFERENCES silver.dim_date(date),
      CONSTRAINT fk_start_location FOREIGN KEY (start_location_id) REFERENCES silver.dim_locations(location_id),
      CONSTRAINT fk_end_location FOREIGN KEY (end_location_id) REFERENCES silver.dim_locations(location_id),
      CONSTRAINT fk_user_type FOREIGN KEY (user_type_id) REFERENCES silver.dim_user_types(user_type_id)
    );
  $ddl$;

  -- 3. Create indexes via EXECUTE (optional)
  CREATE INDEX IF NOT EXISTS idx_fact_trips_start_location ON silver.fact_trips(start_location_id);
  CREATE INDEX IF NOT EXISTS idx_fact_trips_end_location ON silver.fact_trips(end_location_id);
  CREATE INDEX IF NOT EXISTS idx_fact_trips_user_type ON silver.fact_trips(user_type_id);
  CREATE INDEX IF NOT EXISTS idx_fact_trips_start_date ON silver.fact_trips(start_date_trip);
  CREATE INDEX IF NOT EXISTS idx_fact_trips_end_date ON silver.fact_trips(end_date_trip);
  CREATE INDEX IF NOT EXISTS idx_fact_trips_bike_id ON silver.fact_trips(bike_id);

EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Error creating silver schema or tables: %', SQLERRM;
END;
$$;

CALL silver.create_silver_schema_and_tables();
