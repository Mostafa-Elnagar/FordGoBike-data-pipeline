import psycopg2
import os
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_connection():
    """Create and return a database connection"""
    try:
        connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database="fordgobike",
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres')
        )
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def create_database():
    """Create the database if it doesn't exist"""
    try:
        # Connect to default postgres database to create our database
        connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database='postgres',
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres')
        )
        connection.autocommit = True
        cursor = connection.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'fordgobike'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier('fordgobike')))
            print("Database 'fordgobike' created successfully")
        else:
            print("Database 'fordgobike' already exists")
            
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"Error creating database: {e}")

def create_schema():
    """Create the database schema for Ford GoBike data"""
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Create the main trips tale
        create_table_query = """
        CREATE SCHEMA IF NOT EXISTS bronze;
        
        CREATE TABLE IF NOT EXISTS bronze.bike_trips (
            duration_sec INTEGER,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            start_station_id VARCHAR(50),
            start_station_name VARCHAR(255),
            start_station_latitude FLOAT,
            start_station_longitude FLOAT,
            end_station_id VARCHAR(50),
            end_station_name VARCHAR(255),
            end_station_latitude FLOAT,
            end_station_longitude FLOAT,
            bike_id VARCHAR(50),
            user_type VARCHAR(50),
            member_birth_year INTEGER,
            member_gender VARCHAR(20),
            period VARCHAR(50),
            bike_share_for_all_trip VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS bronze.locations (
            location_id VARCHAR(50),
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
            UNIQUE (latitude, longitude)
        );

        """
        
        cursor.execute(create_table_query)
        
        connection.commit()
        print("Schema created successfully")
        
        # Show table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'bike_trips' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print("\nTable structure:")
        print("-" * 50)
        for col in columns:
            print(f"{col[0]:<25} {col[1]:<15} {col[2]}")
        
        cursor.close()
        connection.close()
        return True
        
    except Exception as e:
        print(f"Error creating schema: {e}")
        if connection:
            connection.rollback()
            connection.close()
        return False

def main():
    """Main function to initialize the database"""
    print("Initializing Ford GoBike database...")
    
    # Create database
    create_database()
    
    # Create schema
    if create_schema():
        print("\nDatabase initialization completed successfully!")
    else:
        print("\nDatabase initialization failed!")

if __name__ == "__main__":
    main() 