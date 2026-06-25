# FlightOps Multi-DB Airflow Pipeline

A production-style data engineering project demonstrating:
- **Multi-database ETL orchestration** with Apache Airflow 2.8
- **Incremental load pattern** using watermark-based ID tracking
- **Three-database architecture**: PostgreSQL + MongoDB + SQLite (Oracle equivalent)
- **Exception handling** with dedicated exception logging table
- **Fully containerized** local development via Docker Compose

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Apache Airflow 2.8                          │
│                                                                 │
│  ┌──────────────────────┐   ┌──────────────────────────────┐   │
│  │  flightops_           │   │  flightops_                  │   │
│  │  initial_load         │   │  incremental_batch           │   │
│  │  (Manual / One-time)  │   │  (Every 5 minutes)           │   │
│  └──────────┬───────────┘   └──────────────┬───────────────┘   │
└─────────────┼──────────────────────────────┼───────────────────┘
              │                              │
     ┌────────▼──────────────────────────────▼──────────┐
     │              Pipeline Flow                        │
     │                                                   │
     │  1. Health Check (all 3 DBs)                      │
     │  2. Start Run Log → PostgreSQL                    │
     │  3. Fetch Records → MongoDB  (source of truth)    │
     │       ↓ watermark: only new records (batch DAG)   │
     │  4. Lookup IDs   → SQLite    (on-prem / Oracle)   │
     │  5. Write Mappings → PostgreSQL (key mapping)     │
     │  6. Log Exceptions → PostgreSQL (exception table) │
     │  7. Update Watermarks → PostgreSQL  (batch only)  │
     │  8. Complete Run Log → PostgreSQL                 │
     └───────────────────────────────────────────────────┘

Databases:
  PostgreSQL  →  Process tracking, audit log, key mappings, watermarks
  MongoDB     →  Operational data (AWS DocumentDB equivalent)
  SQLite      →  On-prem legacy system  (Oracle on-prem equivalent)
```

## Collections Processed

| MongoDB Collection | On-Prem Table     | BusinessKey Format                    |
|--------------------|-------------------|---------------------------------------|
| FlightSchedules    | FLIGHT_SCHEDULE   | `FLIGHT_NUMBER::ORIGIN::DEPARTURE_DTM`|
| CrewAssignments    | CREW_ASSIGNMENT   | `EMPLOYEE_ID::FLIGHT_NUMBER::DATE`    |
| RouteOperations    | ROUTE_OPERATION   | `ROUTE_CODE::BID_PERIOD`              |
| AircraftStatus     | AIRCRAFT_STATUS   | `TAIL_NUMBER::STATUS_DATE`            |
| PassengerBookings  | PASSENGER_BOOKING | `PNR::FLIGHT_NUMBER::FLIGHT_DATE`     |

---

## Quick Start

### Prerequisites
- Docker Desktop
- Docker Compose
- Python 3.9+

### 1. Clone and start

```bash
git clone https://github.com/your-username/flightops-airflow-pipeline
cd flightops-airflow-pipeline

# Start all services
cd docker
docker-compose up -d

# Wait ~60 seconds for services to initialize
```

### 2. Seed the on-prem SQLite database

```bash
# Run from project root
python scripts/init_sqlite_onprem.py
```

### 3. Open Airflow UI

```
URL:      http://localhost:8080
Username: admin
Password: admin
```

### 4. Run the pipelines

1. Enable and trigger **`flightops_initial_load`** (manual, run once)
2. Enable **`flightops_incremental_batch`** (auto-runs every 5 minutes)

---

## Project Structure

```
flightops-airflow-pipeline/
├── dags/
│   ├── dag_initial_load.py         # One-time full load DAG
│   └── dag_incremental_batch.py    # Watermark-based incremental DAG
├── scripts/
│   ├── db_connections.py           # Centralized DB connection utils
│   ├── init_postgres.sql           # PostgreSQL schema + seed data
│   ├── init_mongo.js               # MongoDB collections + seed data
│   └── init_sqlite_onprem.py       # SQLite on-prem DB seed script
├── docker/
│   └── docker-compose.yml          # Full local stack
├── data/                           # SQLite DB file (git-ignored)
└── README.md
```

---

## Key Engineering Concepts Demonstrated

### 1. Incremental Load with Watermark Pattern
The batch DAG tracks `last_processed_id` per collection in PostgreSQL.
Each run fetches only records with `id > last_watermark`, then updates
the watermark on success. This is a standard CDC (Change Data Capture)
alternative for systems without native CDC support.

```sql
-- Watermark table
SELECT last_processed_id FROM flightops.incremental_watermark
WHERE collection_name = 'FlightSchedules';

-- MongoDB query (incremental)
collection.find({ flightScheduleId: { $gt: last_id } })
```

### 2. BusinessKey-based Cross-System ID Resolution
Records are matched across MongoDB and on-prem SQLite using a composite
BusinessKey (e.g., `UA100::ORD::2025-06-01 08:00:00`). Matched pairs
are stored as `(aws_doc_id, onprem_id)` mappings in PostgreSQL.

### 3. Exception Handling & Audit Trail
Unmatched BusinessKeys (format errors, not found in on-prem) are logged
to `flightops.exception_log` with reason codes. Every pipeline run is
tracked in `flightops.pipeline_run_log` with start/end timestamps,
records processed, and records failed.

### 4. Idempotent Inserts
All insert operations check for existing records before inserting,
making the pipeline safe to re-run without creating duplicates.

---

## Environment Variables

| Variable               | Default        | Description                     |
|------------------------|----------------|---------------------------------|
| `APP_POSTGRES_HOST`    | localhost      | PostgreSQL host                 |
| `APP_POSTGRES_PORT`    | 5433           | PostgreSQL port                 |
| `APP_POSTGRES_DB`      | flightops      | PostgreSQL database name        |
| `APP_POSTGRES_USER`    | flightops_user | PostgreSQL username             |
| `APP_POSTGRES_PASSWORD`| flightops_pass | PostgreSQL password             |
| `MONGO_HOST`           | localhost      | MongoDB host                    |
| `MONGO_PORT`           | 27017          | MongoDB port                    |
| `MONGO_USER`           | mongo_user     | MongoDB username                |
| `MONGO_PASSWORD`       | mongo_pass     | MongoDB password                |
| `MONGO_DB`             | flight_data    | MongoDB database name           |
| `SQLITE_DB_PATH`       | data/onprem... | SQLite file path                |

---

## Production Equivalents

| This Project      | Production Equivalent              |
|-------------------|------------------------------------|
| MongoDB           | AWS DocumentDB                     |
| SQLite            | Oracle on-prem DB (oracledb driver)|
| Env variables     | AWS Secrets Manager                |
| Docker PostgreSQL | AWS RDS PostgreSQL                 |
| Local Airflow     | AWS MWAA (Managed Airflow)         |

---

## Tech Stack

- **Orchestration:** Apache Airflow 2.8
- **Databases:** PostgreSQL 15, MongoDB 6.0, SQLite 3
- **Languages:** Python 3.11, SQL, JavaScript (MongoDB init)
- **Infrastructure:** Docker Compose
- **Libraries:** psycopg2, pymongo, sqlite3

---

## License

MIT
