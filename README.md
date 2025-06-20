# Ford GoBike Data Pipeline

This project sets up a PostgreSQL database for Ford GoBike trip data with Docker containers for PostgreSQL, pgAdmin, and Apache Airflow.

## Prerequisites

- Docker and Docker Compose
- Python 3.8+

## Quick Start

### 1. Setup Environment

```bash
# Run the setup script to create .env file and check prerequisites
python setup.py
```

### 2. Start Database Services

```bash
docker-compose up -d postgres pgadmin
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the Complete Pipeline

```bash
python main.py
```

This will automatically:
- Download Ford GoBike data from S3
- Initialize the database schema
- Load data into the bronze layer
- Enrich location data
- Provide detailed logging and progress updates

## Manual Setup Instructions

### 1. Environment Configuration

Create a `.env` file in the root directory with the following content:

```env
# PostgreSQL Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=fordgobike
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres:5432/airflow
AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW_UID=50000
AIRFLOW_GID=0
```

### 2. Start the Database Services

```bash
docker-compose up -d postgres pgadmin
```

This will start:
- PostgreSQL on port 5432
- pgAdmin on port 5050 (http://localhost:5050)

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 4. Initialize the Database Schema

```bash
python sql/bronze/init_db.py
```

This will:
- Create the `fordgobike` database
- Create the `bike_trips` table with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| duration_sec | INTEGER | Trip duration in seconds |
| start_time | TIMESTAMP | Trip start time |
| end_time | TIMESTAMP | Trip end time |
| start_station_id | FLOAT | Starting station ID |
| start_station_name | VARCHAR(255) | Starting station name |
| start_station_latitude | FLOAT | Starting station latitude |
| start_station_longitude | FLOAT | Starting station longitude |
| end_station_id | FLOAT | Ending station ID |
| end_station_name | VARCHAR(255) | Ending station name |
| end_station_latitude | FLOAT | Ending station latitude |
| end_station_longitude | FLOAT | Ending station longitude |
| bike_id | INTEGER | Bike identifier |
| user_type | VARCHAR(50) | Type of user (Subscriber/Customer) |
| member_birth_year | FLOAT | Member birth year |
| member_gender | VARCHAR(20) | Member gender |
| period | VARCHAR(50) | Time period |
| bike_share_for_all_trip | VARCHAR(10) | Bike share for all trip indicator |
| created_at | TIMESTAMP | Record creation timestamp |

### 5. Access pgAdmin

1. Open http://localhost:5050
2. Login with:
   - Email: admin@admin.com
   - Password: admin
3. Add a new server connection:
   - Host: postgres
   - Port: 5432
   - Database: fordgobike
   - Username: postgres
   - Password: postgres

## Database Schema

The `bike_trips` table is designed to store Ford GoBike trip data with appropriate data types and indexes for optimal performance. The schema includes:

- **Primary Key**: Auto-incrementing ID
- **Indexes**: Created on frequently queried columns (start_time, end_time, station IDs, bike_id, user_type, period)
- **Data Types**: Optimized for the provided data structure
- **Constraints**: Appropriate field lengths and data types

## Usage

After initialization, you can:

1. **Connect to the database** using any PostgreSQL client
2. **Query the data** using SQL
3. **Use with data analysis tools** like pandas, Power BI, or Tableau
4. **Integrate with Airflow** for automated data processing

## Troubleshooting

- If you get connection errors, ensure Docker containers are running
- Check that the `.env` file exists and has correct credentials
- Verify ports 5432 and 5050 are not in use by other services
- Check the log file `fordgobike_pipeline.log` for detailed error information

