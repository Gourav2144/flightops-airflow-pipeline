import sys
import os
sys.path.insert(0, '/opt/airflow/scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from db_connections import get_postgres_conn, get_mongo_db, get_sqlite_conn, check_all_connections

# ── 1. GLOBAL CONFIGURATION ───────────────────────────────────────────────────
COLLECTION_CONFIG = [
    {
        "mongo_collection": "FlightSchedules",
        "mongo_id_field":   "flightScheduleId",
        "onprem_table":     "FLIGHT_SCHEDULE",
        "onprem_id_col":    "FLIGHT_SCHEDULE_ID",
        "bk_parts":         3,
        "record_type":      "FlightSchedule",
        "xcom_key":         "flight_schedules",
    },
    {
        "mongo_collection": "CrewAssignments",
        "mongo_id_field":   "crewAssignmentId",
        "onprem_table":     "CREW_ASSIGNMENT",
        "onprem_id_col":    "CREW_ASSIGNMENT_ID",
        "bk_parts":         3,
        "record_type":      "CrewAssignment",
        "xcom_key":         "crew_assignments",
    },
    {
        "mongo_collection": "RouteOperations",
        "mongo_id_field":   "routeOperationId",
        "onprem_table":     "ROUTE_OPERATION",
        "onprem_id_col":    "ROUTE_OPERATION_ID",
        "bk_parts":         2,
        "record_type":      "RouteOperation",
        "xcom_key":         "route_operations",
    },
    {
        "mongo_collection": "AircraftStatus",
        "mongo_id_field":   "aircraftStatusId",
        "onprem_table":     "AIRCRAFT_STATUS",
        "onprem_id_col":    "AIRCRAFT_STATUS_ID",
        "bk_parts":         2,
        "record_type":      "AircraftStatus",
        "xcom_key":         "aircraft_status",
    },
    {
        "mongo_collection": "PassengerBookings",
        "mongo_id_field":   "passengerBookingId",
        "onprem_table":     "PASSENGER_BOOKING",
        "onprem_id_col":    "BOOKING_ID",
        "bk_parts":         3,
        "record_type":      "PassengerBooking",
        "xcom_key":         "passenger_bookings",
    },
]

ONPREM_QUERIES = {
    "FlightSchedules": "SELECT FLIGHT_SCHEDULE_ID FROM FLIGHT_SCHEDULE WHERE FLIGHT_NUMBER = ? AND ORIGIN = ? AND DEPARTURE_DTM = ?",
    "CrewAssignments": "SELECT CREW_ASSIGNMENT_ID FROM CREW_ASSIGNMENT WHERE EMPLOYEE_ID = ? AND FLIGHT_NUMBER = ? AND ASSIGNMENT_DATE = ?",
    "RouteOperations": "SELECT ROUTE_OPERATION_ID FROM ROUTE_OPERATION WHERE ROUTE_CODE = ? AND BID_PERIOD = ?",
    "AircraftStatus":  "SELECT AIRCRAFT_STATUS_ID FROM AIRCRAFT_STATUS WHERE TAIL_NUMBER = ? AND STATUS_DATE = ?",
    "PassengerBookings": "SELECT BOOKING_ID FROM PASSENGER_BOOKING WHERE PNR = ? AND FLIGHT_NUMBER = ? AND FLIGHT_DATE = ?",
}

# ── 2. SHARED PIPELINE TASKS (8 STEPS) ────────────────────────────────────────

# STEP 1: Health Check & Structural Sync
def health_check():
    results = check_all_connections()
    failed = [k for k, v in results.items() if v != "OK"]
    if failed:
        raise Exception(f"DB health check failed: {failed}")
    
    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS flightops;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS flightops.pipeline_run_log (
                    run_id SERIAL PRIMARY KEY, pipeline_name VARCHAR(100),
                    start_dtm TIMESTAMP, end_dtm TIMESTAMP, run_status VARCHAR(50)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS flightops.incremental_watermark (
                    collection_name VARCHAR(100) PRIMARY KEY, last_processed_id INT NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS flightops.key_mappings (
                    mapping_id SERIAL PRIMARY KEY, run_id INT, business_key VARCHAR(255),
                    aws_id VARCHAR(255), onprem_id VARCHAR(255), record_type VARCHAR(100), load_dtm TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS flightops.pipeline_exceptions (
                    exception_id SERIAL PRIMARY KEY, run_id INT, business_key VARCHAR(255),
                    aws_id VARCHAR(255), exception_reason TEXT, log_dtm TIMESTAMP
                );
            """)
            conn.commit()
    print("Database structure successfully verified.")

# STEP 2: Start Run Log
def start_run_log(pipeline_name, **kwargs):
    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO flightops.pipeline_run_log (pipeline_name, start_dtm, run_status)
                VALUES (%s, %s, %s) RETURNING run_id;
            """, (pipeline_name, datetime.utcnow(), "Running"))
            run_id = cur.fetchone()[0]
            conn.commit()
    print(f"Run logging initialized. run_id={run_id}")
    kwargs['ti'].xcom_push(key='run_id', value=run_id)

# STEP 3: Load High Watermarks (Only needed for incremental)
def load_watermarks(**kwargs):
    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            for cfg in COLLECTION_CONFIG:
                cur.execute("""
                    INSERT INTO flightops.incremental_watermark (collection_name, last_processed_id)
                    VALUES (%s, 0) ON CONFLICT (collection_name) DO NOTHING;
                """, (cfg["mongo_collection"],))
            conn.commit()
            
            cur.execute("SELECT collection_name, last_processed_id FROM flightops.incremental_watermark;")
            watermarks = {row[0]: int(row[1]) for row in cur.fetchall()}
    kwargs['ti'].xcom_push(key='watermarks', value=watermarks)
    print(f"Watermarks active context: {watermarks}")

# STEP 4: Fetch MongoDB Records (Handles both Full and Delta depending on DAG context)
def fetch_mongo_records(is_incremental, **kwargs):
    db = get_mongo_db()
    new_max_ids = {}
    
    watermarks = {}
    if is_incremental:
        watermarks = kwargs['ti'].xcom_pull(key='watermarks', task_ids='load_watermarks') or {}

    for cfg in COLLECTION_CONFIG:
        coll = db[cfg["mongo_collection"]]
        last_id = int(watermarks.get(cfg["mongo_collection"], 0)) if is_incremental else -1
        
        records = list(coll.find(
            {cfg["mongo_id_field"]: {"$gt": last_id}},
            {"BusinessKey": 1, cfg["mongo_id_field"]: 1, "_id": 0}
        ).sort(cfg["mongo_id_field"], 1))

        business_keys = [
            {"BusinessKey": r["BusinessKey"], "aws_id": int(r[cfg["mongo_id_field"]])}
            for r in records if "BusinessKey" in r and cfg["mongo_id_field"] in r
        ]

        if business_keys:
            new_max_ids[cfg["mongo_collection"]] = max(item["aws_id"] for item in business_keys)
        else:
            new_max_ids[cfg["mongo_collection"]] = last_id if is_incremental else 0

        print(f"{cfg['mongo_collection']}: Collected {len(business_keys)} entries.")
        kwargs['ti'].xcom_push(key=f"{cfg['xcom_key']}_bkeys", value=business_keys)

    kwargs['ti'].xcom_push(key='new_max_ids', value=new_max_ids)

# STEP 5: Query On-Prem SQLite Matrix
def query_onprem_ids(parent_task_id, **kwargs):
    sqlite_conn = get_sqlite_conn()
    cur = sqlite_conn.cursor()

    for cfg in COLLECTION_CONFIG:
        business_keys = kwargs['ti'].xcom_pull(key=f"{cfg['xcom_key']}_bkeys", task_ids=parent_task_id) or []
        processed = []
        exceptions = []
        query = ONPREM_QUERIES[cfg["mongo_collection"]]

        for item in business_keys:
            bk = item["BusinessKey"]
            parts = bk.split("::")

            if len(parts) != cfg["bk_parts"]:
                exceptions.append({"BusinessKey": bk, "reason": "invalid_bk_format", "aws_id": str(item["aws_id"])})
                continue

            params = tuple(p.replace("T", " ") for p in parts)
            try:
                cur.execute(query, params)
                result = cur.fetchone()
            except Exception as e:
                exceptions.append({"BusinessKey": bk, "reason": f"query_error: {e}", "aws_id": str(item["aws_id"])})
                continue

            if result:
                processed.append({"BusinessKey": bk, "aws_id": str(item["aws_id"]), "onprem_id": str(result[0])})
            else:
                exceptions.append({"BusinessKey": bk, "reason": "not_found_in_onprem", "aws_id": str(item["aws_id"])})

        print(f"{cfg['mongo_collection']}: Matched={len(processed)}, Exceptions={len(exceptions)}")
        kwargs['ti'].xcom_push(key=f"{cfg['xcom_key']}_matched", value=processed)
        kwargs['ti'].xcom_push(key=f"{cfg['xcom_key']}_exceptions", value=exceptions)

    cur.close()
    sqlite_conn.close()

# STEP 6: Insert Key Mappings to Target
def insert_key_mappings(parent_start_task, **kwargs):
    run_id = kwargs['ti'].xcom_pull(key='run_id', task_ids=parent_start_task)
    total_inserted = 0

    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            for cfg in COLLECTION_CONFIG:
                matched_items = kwargs['ti'].xcom_pull(key=f"{cfg['xcom_key']}_matched", task_ids='query_onprem_ids') or []
                for item in matched_items:
                    cur.execute("""
                        INSERT INTO flightops.key_mappings (run_id, business_key, aws_id, onprem_id, record_type, load_dtm)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (run_id, item["BusinessKey"], item["aws_id"], item["onprem_id"], cfg["record_type"], datetime.utcnow()))
                    total_inserted += 1
            conn.commit()
    print(f"Flushed {total_inserted} records securely to PostgreSQL.")

 # STEP 7: Log System Processing Exceptions
def log_exceptions(parent_start_task, **kwargs):
    run_id = kwargs['ti'].xcom_pull(
        key='run_id',
        task_ids=parent_start_task
    )

    total_exceptions = 0

    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            for cfg in COLLECTION_CONFIG:

                exceptions = kwargs['ti'].xcom_pull(
                    key=f"{cfg['xcom_key']}_exceptions",
                    task_ids='query_onprem_ids'
                ) or []

                for exc in exceptions:
                    cur.execute("""
                        INSERT INTO flightops.pipeline_exceptions
                        (
                            run_id,
                            business_key,
                            aws_id,
                            exception_reason,
                            log_dtm
                        )
                        VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        exc["BusinessKey"],
                        exc["aws_id"],
                        exc["reason"],
                        datetime.utcnow()
                    ))

                    total_exceptions += 1

            conn.commit()

    print(f"Logged {total_exceptions} format/lookup exceptions.")


# STEP 8: Finalize Watermarks & Close Session
def complete_run_log(parent_start_task, update_watermarks, **kwargs):

    run_id = kwargs['ti'].xcom_pull(
        key='run_id',
        task_ids=parent_start_task
    )

    with get_postgres_conn() as conn:
        with conn.cursor() as cur:

            if update_watermarks:

                new_max_ids = kwargs['ti'].xcom_pull(
                    key='new_max_ids',
                    task_ids='fetch_mongo_records'
                ) or {}

                for collection_name, max_id in new_max_ids.items():
                    cur.execute("""
                        UPDATE flightops.incremental_watermark
                        SET last_processed_id = %s
                        WHERE collection_name = %s
                    """, (max_id, collection_name))

            cur.execute("""
                UPDATE flightops.pipeline_run_log
                SET run_status = 'Complete',
                    end_dtm = %s
                WHERE run_id = %s
            """, (datetime.utcnow(), run_id))

            conn.commit()

    print(f"Session closed successfully for run_id={run_id}")


# ── 3. AIRFLOW DAG DEFINITIONS ────────────────────────────────────────────────

default_args = {
    'owner': 'flightops',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG 2: Incremental Batch
with DAG(
    'flightops_incremental_batch',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=['flightops', 'incremental_delta']
) as dag_incremental:

    inc_t1 = PythonOperator(
        task_id='health_check',
        python_callable=health_check
    )

    inc_t2 = PythonOperator(
        task_id='start_run_log',
        python_callable=start_run_log,
        op_kwargs={
            'pipeline_name': 'flightops_incremental_batch'
        }
    )

    inc_t3 = PythonOperator(
        task_id='load_watermarks',
        python_callable=load_watermarks
    )

    inc_t4 = PythonOperator(
        task_id='fetch_mongo_records',
        python_callable=fetch_mongo_records,
        op_kwargs={
            'is_incremental': True
        }
    )

    inc_t5 = PythonOperator(
        task_id='query_onprem_ids',
        python_callable=query_onprem_ids,
        op_kwargs={
            'parent_task_id': 'fetch_mongo_records'
        }
    )

    inc_t6 = PythonOperator(
        task_id='insert_key_mappings',
        python_callable=insert_key_mappings,
        op_kwargs={
            'parent_start_task': 'start_run_log'
        }
    )

    inc_t7 = PythonOperator(
        task_id='log_exceptions',
        python_callable=log_exceptions,
        op_kwargs={
            'parent_start_task': 'start_run_log'
        }
    )

    inc_t8 = PythonOperator(
        task_id='complete_run_log',
        python_callable=complete_run_log,
        op_kwargs={
            'parent_start_task': 'start_run_log',
            'update_watermarks': True
        }
    )

    inc_t1 >> inc_t2 >> inc_t3 >> inc_t4 >> inc_t5 >> [inc_t6, inc_t7] >> inc_t8