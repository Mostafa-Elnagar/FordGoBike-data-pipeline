# Ford GoBike Data Pipeline

A robust, modular data pipeline for ingesting, processing, and analyzing Ford GoBike trip data using Apache Airflow, PostgreSQL, and Docker. The pipeline supports multi-layer ETL (Bronze, Silver, Gold), automated email notifications, and is designed for extensibility and reproducibility.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Features](#features)
- [Directory Structure](#directory-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup](#detailed-setup)
- [Airflow DAGs](#airflow-dags)
- [Modules](#modules)
- [Email Notification Service](#email-notification-service)
- [Database Schema](#database-schema)
- [Data Exploration](#data-exploration)
- [Dashboards & Visualizations](#dashboards-and-visualizations)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## Project Overview

This project automates the end-to-end workflow for Ford GoBike trip data, from raw data ingestion to advanced analytics-ready tables. It leverages:

- **Apache Airflow** for orchestration
- **PostgreSQL** for storage and analytics
- **Docker** for reproducibility and easy deployment
- **Modular Python and Node.js components** for extensibility

---

## Architecture

```mermaid
graph TD
    S3[Ford GoBike S3 Data] -->|Download| Bronze[Bronze Layer: Raw Data]
    Bronze -->|ETL| Silver[Silver Layer: Cleaned/Enriched Data]
    Silver -->|Transform| Gold[Gold Layer: Analytics Data Marts]
    Airflow[Airflow DAGs] --> Bronze
    Airflow --> Silver
    Airflow --> Gold
    Airflow -->|Failure Alerts| Email[Email Sender Service]
    User[User/Analyst] -->|Query| PostgreSQL
    User -->|Explore| Notebook[Jupyter Notebook]
```

---

### Airflow Pipeline Diagram

![Airflow Pipeline Diagram](assets/images/airflow_diagram.jpg)

---

## Features

- **Multi-layer ETL**: Bronze (raw), Silver (cleaned/enriched), Gold (analytics)
- **Automated orchestration** with Airflow DAGs
- **Email notifications** on pipeline failures
- **Dockerized** for easy local or cloud deployment
- **Modular codebase** for easy extension
- **Data exploration notebook** for analysis and validation

---

## Directory Structure

```
FordGoBike-data-pipeline/
│
├── dags/                # Airflow DAGs for ETL orchestration
├── include/
│   ├── data/            # Raw, extracted, and archived data
│   ├── modules/         # Modular Python and Node.js components
│   │   ├── get_data.py          # S3 data downloader
│   │   ├── get_locations.py     # Location enrichment
│   │   └── email_sender/        # Email notification microservice
│   └── sql/             # SQL scripts for each ETL layer
│       ├── bronze/
│       ├── silver/
│       └── gold/
├── notebooks/           # Jupyter notebooks for data exploration
├── tests/               # (Reserved for tests)
├── Dockerfile           # Main pipeline Docker image
├── docker-compose.override.yml  # Compose for services
├── requirements.txt     # Python dependencies
├── airflow_settings.yaml# Airflow connections/variables
└── README.md
```

---

## Prerequisites

- Docker & Docker Compose
- Python 3.8+
- (Optional) Node.js 20+ (for email sender development)

---

## Quick Start

1. **Clone the repository**
2. **Configure environment variables** (see below)
   - **Main pipeline**: Create a `.env` file in the project root (see example in Detailed Setup)
   - **Email sender module**: Create a `.env` file in `include/modules/email_sender/` (see example below)
3. **Build the email_sender image**  
   From the project root, run:
   ```bash
   docker build -f include/modules/email_sender/Dockerfile -t email_sender:latest include/modules/email_sender
   ```
4. **Install Astronomer CLI** (if not already installed)
   ```bash
   curl -sSL https://install.astronomer.io | sudo bash
   ```
5. **Start all services (Airflow, Postgres, pgAdmin, email sender, etc.)**
   ```bash
   astro dev start
   ```
6. **(Optional) Install Python dependencies locally for development**
   ```bash
   pip install -r requirements.txt
   ```
7. **Trigger the pipeline** (via Airflow UI or CLI)

---

## Detailed Setup

### 1. Environment Configuration

Create a `.env` file in the root and in `include/modules/email_sender/` as needed.

**Example for project root:**

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=fordgobike
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
S3_BUCKET_URL=https://s3.amazonaws.com/fordgobike-data/
REVERSE_GEOCODE_API_URL=...
GEOCODE_API_HOST=...
GEOCODE_API_KEY1=...
GEOCODE_KEY_COUNT=1
```

**Example for `include/modules/email_sender/.env`:**

```env
PORT=5000
EMAIL_USER=your_email@gmail.com
EMAIL_PASS=your_gmail_app_password
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5050
```

- `EMAIL_USER`: The Gmail address used to send emails
- `EMAIL_PASS`: The Gmail App Password (not your regular password)
- `ALLOWED_ORIGINS`: Comma-separated list of allowed origins for CORS
- `PORT`: Port for the email sender service (default: 5000)

### 2. Build and Start Services

```bash
astro dev start
```
- **PostgreSQL**: port 5432
- **pgAdmin**: port 5050 ([http://localhost:5050](http://localhost:5050))
- **Email Sender**: port 5000
- **Airflow**: port 8080


---

## Airflow DAGs

- **ddl_dag**: Initializes database and schemas (bronze, silver, gold)
- **bronze_dag**: Downloads data from S3, loads into bronze layer
- **silver_dag**: Cleans/enriches data, loads into silver layer
- **gold_dag**: Builds analytics-ready data marts in gold layer

Each DAG is modular and triggers the next stage. Failure in any task triggers an email alert.

---

## Modules

- **get_data.py**: Downloads and extracts trip data from S3.
- **get_locations.py**: Enriches trip data with reverse geocoded location info.
- **email_sender/**: Node.js microservice for sending email alerts (used by Airflow for failure notifications).

---

## Email Notification Service

- **Location**: `include/modules/email_sender/`
- **Tech**: Node.js, Express, Nodemailer
- **API**: `POST /send` with JSON body:
  ```json
  {
    "name": "Airflow",
    "email": "sender@example.com",
    "subject": "Task Failure",
    "message": "Details about the failure...",
    "receiver_email": "admin@example.com"
  }
  ```
- **Dockerized**: Runs as a service in `docker-compose.override.yml`
- **Environment**: Configure Gmail App Password for secure sending

This module is originally based on [KareemEhab/email-sender](https://github.com/KareemEhab/email-sender) with some modifications.  

---

## Database Schema

The main `bike_trips` table (bronze layer) includes:

| Column                   | Type         | Description                        |
|--------------------------|--------------|------------------------------------|
| id                       | SERIAL       | Primary key                        |
| duration_sec             | INTEGER      | Trip duration in seconds           |
| start_time               | TIMESTAMP    | Trip start time                    |
| end_time                 | TIMESTAMP    | Trip end time                      |
| start_station_id         | FLOAT        | Starting station ID                |
| ...                      | ...          | ...                                |
| bike_share_for_all_trip  | VARCHAR(10)  | Bike share for all trip indicator  |
| created_at               | TIMESTAMP    | Record creation timestamp          |

See `include/sql/bronze/init_db.py` and `include/sql/silver/`, `include/sql/gold/` for full schema and transformations.

---

### Silver Layer Schema

The Silver layer is a star schema designed for analytics, consisting of dimension and fact tables:

#### `silver.dim_locations`
| Column         | Type         | Description                |
|---------------|--------------|----------------------------|
| location_id    | BIGINT       | Primary key                |
| latitude       | FLOAT        | Latitude of the location   |
| longitude      | FLOAT        | Longitude of the location  |
| highway        | VARCHAR(256) | Highway name (if any)      |
| road           | VARCHAR(256) | Road name (if any)         |
| neighbourhood  | VARCHAR(256) | Neighbourhood name         |
| suburb         | VARCHAR(256) | Suburb name                |
| city           | VARCHAR(256) | City name                  |
| state          | VARCHAR(256) | State name                 |
| postcode       | VARCHAR(50)  | Postal code                |
| country        | VARCHAR(256) | Country                    |
| display_name   | VARCHAR(256) | Full display name          |
| station_name   | VARCHAR(256) | Station name               |

#### `silver.dim_user_types`
| Column                | Type         | Description                        |
|-----------------------|--------------|------------------------------------|
| user_type_id          | BIGINT       | Primary key                        |
| user_type             | VARCHAR(50)  | User type (e.g., Subscriber)       |
| member_birth_year     | INTEGER      | Birth year of the member           |
| member_gender         | VARCHAR(20)  | Gender of the member               |
| bike_share_for_all_trip | VARCHAR(20)| Bike share for all trip indicator  |

#### `silver.dim_date`
| Column       | Type    | Description                |
|--------------|---------|----------------------------|
| date_id      | INT     | Primary key                |
| year         | INT     | Year                       |
| month        | INT     | Month (number)             |
| month_name   | TEXT    | Month (name)               |
| day          | INT     | Day of month               |
| quarter      | INT     | Quarter of year            |
| day_of_week  | INT     | Day of week (number)       |
| day_name     | TEXT    | Day of week (name)         |
| is_weekend   | BOOLEAN | Weekend indicator          |

#### `silver.fact_trips`
| Column            | Type      | Description                                 |
|-------------------|-----------|---------------------------------------------|
| trip_id           | INTEGER   | Primary key                                 |
| duration_min      | INT       | Trip duration in minutes                    |
| start_location_id | BIGINT    | FK to dim_locations (start)                 |
| start_date_id     | INT       | FK to dim_date (start)                      |
| start_time        | TIME      | Trip start time                             |
| end_location_id   | BIGINT    | FK to dim_locations (end)                   |
| end_date_id       | INT       | FK to dim_date (end)                        |
| end_time          | TIME      | Trip end time                               |
| bike_id           | VARCHAR(50) | Bike identifier                           |
| user_type_id      | BIGINT    | FK to dim_user_types                        |

**Foreign Keys:**
- `start_date_id`, `end_date_id` → `silver.dim_date(date_id)`
- `start_location_id`, `end_location_id` → `silver.dim_locations(location_id)`
- `user_type_id` → `silver.dim_user_types(user_type_id)`

**Indexes:**
- On all foreign keys and `bike_id` for efficient analytics queries.

---

### Database Diagram

![Database Schema Diagram](assets/images/Database_digram.png)

*Entity-relationship diagram of the main tables and relationships in the Ford GoBike database (Silver Layer).* 

---

## Data Exploration

- **Notebook**: `notebooks/data_exploration.ipynb`
- **Purpose**: Explore, validate, and analyze the processed data using pandas and numpy.
- **How to use**: Open in JupyterLab or VSCode and run cells for summary statistics, missing value analysis, and station mapping.

---

## Dashboards & Visualizations

This project includes a set of interactive Power BI dashboards to help you explore and analyze Ford GoBike data. Below are the main dashboard pages and their purposes, with animated previews:

### Overview Page
Provides a high-level summary of bike usage, key metrics, and trends across the system.

![Overview Dashboard](assets/images/dashboard/overview_page.gif)

### Client Type Page
Breaks down trip statistics by user type (e.g., Subscriber vs. Customer), helping to understand user demographics and behaviors.

![Client Type Dashboard](assets/images/dashboard/client_type_page.gif)

### Locations Page
Visualizes the geographical distribution of stations and trip flows, highlighting popular start/end locations and spatial patterns.

![Locations Dashboard](assets/images/dashboard/locations_page.gif)

### Time Analysis Page
Analyzes temporal trends in bike usage, such as hourly, daily, and monthly patterns, to uncover peak usage times and seasonality.

![Time Analysis Dashboard](assets/images/dashboard/time_analysis_page.gif)

---

## Troubleshooting

- Ensure all Docker containers are running (`docker ps`)
- Check `.env` files for correct credentials and API keys
- Verify ports 5432, 5050, 8080, and 5000 are available
- Check Airflow logs and `fordgobike_pipeline.log` for errors
- Email issues: check email sender logs and Gmail App Password setup

---

## Contributing

Pull requests and issues are welcome! Please open an issue to discuss your ideas or report bugs.

---

## License

This project is licensed under the MIT License.

