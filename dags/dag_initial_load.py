import sys
import os
sys.path.insert(0, '/opt/airflow/scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import psycopg2
from db_connections import get_postgres_conn, get_mongo_db, get_sqlite_conn, check_all_connections

# ── Collection config ─────────────────────────────────────────────────────────
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
    "AircraftStatus": "SELECT AIRCRAFT_STATUS_ID FROM AIRCRAFT_STATUS WHERE TAIL_NUMBER = ? AND STATUS_DATE = ?",
    "PassengerBookings": "SELECT BOOKING_ID FROM PASSENGER_BOOKING WHERE PNR = ? AND FLIGHT_NUMBER = ? AND FLIGHT_DATE = ?",
}

# ── Task 1: Health Check ──────────────────────────────────────────────────────
def health_check():
    results = check_all_connections()
    failed = [k for k, v in results.items() if v != "OK"]
    if failed:
        raise Exception(f"DB connection check failed for: {failed}. Results: {results}")
    print(f"All DB connections healthy: {results}")
# ── Task 2: Start Run Log (With Auto Table Creation) ─────────────────────────
def start_run_log(**kwargs):
    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            # 1. पहले स्कीमा और टेबल बनाएं अगर वे मौजूद नहीं हैं
            cur.execute("CREATE SCHEMA IF NOT EXISTS flightops;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS flightops.pipeline_run_log (
                    run_id SERIAL PRIMARY KEY,
                    pipeline_name VARCHAR(100),
                    start_dtm TIMESTAMP,
                    end_dtm TIMESTAMP,
                    run_status VARCHAR(50)
                );
            """)
            conn.commit()

            # 2. अब रन रिकॉर्ड इन्सर्ट करें
            cur.execute("""
                INSERT INTO flightops.pipeline_run_log (pipeline_name, start_dtm, run_status)
                VALUES (%s, %s, %s) RETURNING run_id
            """, ("flightops_initial_load", datetime.utcnow(), "Running"))
            run_id = cur.fetchone()[0] # [0] जोड़ा गया है ताकि केवल ID टुपल से बाहर आए
            conn.commit()
            
    print(f"Pipeline run started. run_id={run_id}")
    kwargs['ti'].xcom_push(key='run_id', value=run_id)


# ── Task 3: Fetch MongoDB Records ─────────────────────────────────────────────
def fetch_mongo_records(**kwargs):
    db = get_mongo_db()
    for cfg in COLLECTION_CONFIG:
        coll = db[cfg["mongo_collection"]]
        records = list(coll.find({}, {"BusinessKey": 1, cfg["mongo_id_field"]: 1, "_id": 0}))
        business_keys = [
            {"BusinessKey": r["BusinessKey"], "aws_id": str(r[cfg["mongo_id_field"]])}
            for r in records if "BusinessKey" in r and cfg["mongo_id_field"] in r
        ]
        print(f"{cfg['mongo_collection']}: fetched {len(business_keys)} records")
        kwargs['ti'].xcom_push(key=f"{cfg['xcom_key']}_bkeys", value=business_keys)

# ── Task 4: Query On-Prem IDs ─────────────────────────────────────────────────
def query_onprem_ids(**kwargs):
    sqlite_conn = get_sqlite_conn()
    cur = sqlite_conn.cursor()
    for cfg in COLLECTION_CONFIG:
        business_keys = kwargs['ti'].xcom_pull(key=f"{cfg['xcom_key']}_bkeys", task_ids='fetch_mongo_records') or []
        processed = []
        exceptions = []
        query = ONPREM_QUERIES[cfg["mongo_collection"]]
        
        for item in business_keys:
            bk = item["BusinessKey"]
            parts = bk.split("::")
            if len(parts) != cfg["bk_parts"]:
                exceptions.append({"BusinessKey": bk, "reason": "invalid_format", "aws_id": item["aws_id"]})
                continue
            params = tuple(p.replace("T", " ") for p in parts)
            try:
                cur.execute(query, params)
                result = cur.fetchone()
            except Exception as e:
                exceptions.append({"BusinessKey": bk, "reason": str(e), "aws_id": item["aws_id"]})
                continue
            if result:
                processed.append({"BusinessKey": bk, "aws_id": item["aws_id"], "onprem_id": str(result[0])})
            else:
                exceptions.append({"BusinessKey": bk, "reason": "not_found_in_onprem", "aws_id": item["aws_id"]})
        
        print(f"{cfg['mongo_collection']}: matched={len(processed)}, exceptions={len(exceptions)}")
        kwargs['ti'].xcom_push(key=f"{cfg['xcom_key']}_matched", value=processed)
        kwargs['ti'].xcom_push(key=f"{cfg['xcom_key']}_exceptions", value=exceptions)
    cur.close()
    sqlite_conn.close()

# ── Task 5: Insert Key Mappings (With Auto Table Creation) ────────────────────
def insert_key_mappings(**kwargs):
    run_id = kwargs['ti'].xcom_pull(key='run_id', task_ids='start_run_log')
    # अगर run_id एक टुपल (tuple) के रूप में आ रहा है, तो उसका पहला एलिमेंट निकालें
    if isinstance(run_id, (tuple, list)):
        run_id = run_id[0]
        
    total_inserted = 0

    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            # टेबल मौजूद नहीं है तो पहले उसे बनाएं
            cur.execute("""
                CREATE TABLE IF NOT EXISTS flightops.key_mappings (
                    mapping_id SERIAL PRIMARY KEY,
                    run_id INT,
                    business_key VARCHAR(255),
                    aws_id VARCHAR(255),
                    onprem_id VARCHAR(255),
                    record_type VARCHAR(100),
                    load_dtm TIMESTAMP
                );
            """)
            conn.commit()

            for cfg in COLLECTION_CONFIG:
                matched_items = kwargs['ti'].xcom_pull(key=f"{cfg['xcom_key']}_matched", task_ids='query_onprem_ids') or []
                for item in matched_items:
                    cur.execute("""
                        INSERT INTO flightops.key_mappings (run_id, business_key, aws_id, onprem_id, record_type, load_dtm)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (run_id, item["BusinessKey"], item["aws_id"], item["onprem_id"], cfg["record_type"], datetime.utcnow()))
                    total_inserted += 1
            conn.commit()
    print(f"Successfully inserted {total_inserted} key mappings.")

# ── Task 6: Log Exceptions (With Auto Table Creation) ─────────────────────────
def log_exceptions(**kwargs):
    run_id = kwargs['ti'].xcom_pull(key='run_id', task_ids='start_run_log')
    # अगर run_id एक टुपल (tuple) के रूप में आ रहा है, तो उसका पहला एलिमेंट निकालें
    if isinstance(run_id, (tuple, list)):
        run_id = run_id[0]
        
    total_exceptions = 0

    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            # टेबल मौजूद नहीं है तो पहले उसे बनाएं
            cur.execute("""
                CREATE TABLE IF NOT EXISTS flightops.pipeline_exceptions (
                    exception_id SERIAL PRIMARY KEY,
                    run_id INT,
                    business_key VARCHAR(255),
                    aws_id VARCHAR(255),
                    exception_reason TEXT,
                    log_dtm TIMESTAMP
                );
            """)
            conn.commit()

            for cfg in COLLECTION_CONFIG:
                exceptions = kwargs['ti'].xcom_pull(key=f"{cfg['xcom_key']}_exceptions", task_ids='query_onprem_ids') or []
                for exc in exceptions:
                    cur.execute("""
                        INSERT INTO flightops.pipeline_exceptions (run_id, business_key, aws_id, exception_reason, log_dtm)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (run_id, exc["BusinessKey"], exc["aws_id"], exc["reason"], datetime.utcnow()))
                    total_exceptions += 1
            conn.commit()
    print(f"Logged {total_exceptions} exceptions to PostgreSQL.")

# ── Task 7: Complete Run Log ──────────────────────────────────────────────────
def complete_run_log(**kwargs):
    run_id = kwargs['ti'].xcom_pull(key='run_id', task_ids='start_run_log')
    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE flightops.pipeline_run_log
                SET run_status = 'Complete', end_dtm = %s
                WHERE run_id = %s
            """, (datetime.utcnow(), run_id))
            conn.commit()
    print(f"Pipeline run_id={run_id} marked as Complete.")

# ── DAG Definition ────────────────────────────────────────────────────────────
default_args = {
    'owner': 'flightops',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    'flightops_initial_load',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['flightops', 'initial_load']
) as dag:

    t1 = PythonOperator(task_id='health_check', python_callable=health_check)
    t2 = PythonOperator(task_id='start_run_log', python_callable=start_run_log)
    t3 = PythonOperator(task_id='fetch_mongo_records', python_callable=fetch_mongo_records)
    t4 = PythonOperator(task_id='query_onprem_ids', python_callable=query_onprem_ids)
    t5 = PythonOperator(task_id='insert_key_mappings', python_callable=insert_key_mappings)
    t6 = PythonOperator(task_id='log_exceptions', python_callable=log_exceptions)
    t7 = PythonOperator(task_id='complete_run_log', python_callable=complete_run_log)

    # Pipeline Flow
    t1 >> t2 >> t3 >> t4 >> [t5, t6] >> t7