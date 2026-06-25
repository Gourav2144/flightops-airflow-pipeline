"""
init_sqlite_onprem.py
=====================
Seeds the local SQLite DB that simulates an on-prem Oracle database.
Run once before starting the pipeline:
    python scripts/init_sqlite_onprem.py

Mirrors production pattern:
- 5 tables = 5 entity types with on-prem integer primary keys
- BusinessKey format matches MongoDB records exactly
- Some records intentionally missing to demonstrate exception handling
"""

import sqlite3
import os

DB_PATH = os.environ.get("SQLITE_DB_PATH", "data/onprem_oracle_sim.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# ── FlightSchedules ──────────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS FLIGHT_SCHEDULE (
    FLIGHT_SCHEDULE_ID INTEGER PRIMARY KEY,
    FLIGHT_NUMBER      TEXT NOT NULL,
    ORIGIN             TEXT NOT NULL,
    DESTINATION        TEXT NOT NULL,
    DEPARTURE_DTM      TEXT NOT NULL,
    AIRCRAFT_TYPE      TEXT,
    STATUS             TEXT DEFAULT 'Scheduled'
)
""")
cur.executemany(
    "INSERT OR REPLACE INTO FLIGHT_SCHEDULE VALUES (?,?,?,?,?,?,?)",
    [
        (10001, "UA100", "ORD", "LAX", "2025-06-01 08:00:00", "B737", "Scheduled"),
        (10002, "UA101", "LAX", "JFK", "2025-06-01 10:30:00", "B757", "Scheduled"),
        (10003, "UA200", "JFK", "ORD", "2025-06-02 06:00:00", "A320", "Scheduled"),
        (10004, "UA201", "ORD", "DEN", "2025-06-02 14:15:00", "B737", "Scheduled"),
        (10005, "UA300", "DEN", "SFO", "2025-06-03 09:45:00", "B757", "Scheduled"),
        # UA301 intentionally missing → will appear in exception log
    ]
)

# ── CrewAssignments ──────────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS CREW_ASSIGNMENT (
    CREW_ASSIGNMENT_ID INTEGER PRIMARY KEY,
    EMPLOYEE_ID        TEXT NOT NULL,
    FLIGHT_NUMBER      TEXT NOT NULL,
    ASSIGNMENT_DATE    TEXT NOT NULL,
    ROLE               TEXT NOT NULL,
    STATUS             TEXT DEFAULT 'Confirmed'
)
""")
cur.executemany(
    "INSERT OR REPLACE INTO CREW_ASSIGNMENT VALUES (?,?,?,?,?,?)",
    [
        (20001, "EMP001", "UA100", "2025-06-01", "Captain",      "Confirmed"),
        (20002, "EMP002", "UA100", "2025-06-01", "FirstOfficer", "Confirmed"),
        (20003, "EMP003", "UA101", "2025-06-01", "Captain",      "Confirmed"),
        (20004, "EMP004", "UA200", "2025-06-02", "Captain",      "Confirmed"),
        (20005, "EMP005", "UA201", "2025-06-02", "FirstOfficer", "Confirmed"),
        # EMP006 intentionally missing → exception
    ]
)

# ── RouteOperations ──────────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS ROUTE_OPERATION (
    ROUTE_OPERATION_ID INTEGER PRIMARY KEY,
    ROUTE_CODE         TEXT NOT NULL,
    BID_PERIOD         INTEGER NOT NULL,
    DISTANCE_MILES     INTEGER,
    AVG_FLIGHT_MINS    INTEGER,
    OPERATIONAL_STATUS TEXT DEFAULT 'Active'
)
""")
cur.executemany(
    "INSERT OR REPLACE INTO ROUTE_OPERATION VALUES (?,?,?,?,?,?)",
    [
        (30001, "ORD-LAX", 202506, 1745, 210, "Active"),
        (30002, "LAX-JFK", 202506, 2475, 300, "Active"),
        (30003, "JFK-ORD", 202506, 1190, 165, "Active"),
        (30004, "ORD-DEN", 202506, 920,  150, "Active"),
        # DEN-SFO intentionally missing → exception
    ]
)

# ── AircraftStatus ───────────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS AIRCRAFT_STATUS (
    AIRCRAFT_STATUS_ID INTEGER PRIMARY KEY,
    TAIL_NUMBER        TEXT NOT NULL,
    STATUS_DATE        TEXT NOT NULL,
    AIRCRAFT_TYPE      TEXT,
    STATUS_CODE        TEXT DEFAULT 'ACTIVE',
    TOTAL_FLIGHT_HOURS INTEGER
)
""")
cur.executemany(
    "INSERT OR REPLACE INTO AIRCRAFT_STATUS VALUES (?,?,?,?,?,?)",
    [
        (40001, "N12345", "2025-06-01 00:00:00", "B737", "ACTIVE",       12500),
        (40002, "N23456", "2025-06-01 00:00:00", "B757", "ACTIVE",       18200),
        (40003, "N34567", "2025-06-01 00:00:00", "A320", "MAINTENANCE",  9800),
        (40004, "N45678", "2025-06-02 00:00:00", "B737", "ACTIVE",       7200),
        (40005, "N56789", "2025-06-02 00:00:00", "B757", "ACTIVE",       22100),
    ]
)

# ── PassengerBookings ────────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS PASSENGER_BOOKING (
    BOOKING_ID      INTEGER PRIMARY KEY,
    PNR             TEXT NOT NULL,
    FLIGHT_NUMBER   TEXT NOT NULL,
    FLIGHT_DATE     TEXT NOT NULL,
    PASSENGER_COUNT INTEGER,
    BOOKING_CLASS   TEXT,
    FARE_AMOUNT     REAL,
    STATUS          TEXT DEFAULT 'Confirmed'
)
""")
cur.executemany(
    "INSERT OR REPLACE INTO PASSENGER_BOOKING VALUES (?,?,?,?,?,?,?,?)",
    [
        (50001, "PNR001", "UA100", "2025-06-01", 142, "Y", 28400.00, "Confirmed"),
        (50002, "PNR002", "UA101", "2025-06-01", 198, "Y", 51480.00, "Confirmed"),
        (50003, "PNR003", "UA200", "2025-06-02", 156, "B", 46800.00, "Confirmed"),
        (50004, "PNR004", "UA201", "2025-06-02", 89,  "Y", 17800.00, "Waitlisted"),
        (50005, "PNR005", "UA300", "2025-06-03", 203, "F", 91350.00, "Confirmed"),
    ]
)

conn.commit()
conn.close()
print(f"SQLite on-prem DB initialized at: {DB_PATH}")
print("Tables created: FLIGHT_SCHEDULE, CREW_ASSIGNMENT, ROUTE_OPERATION, AIRCRAFT_STATUS, PASSENGER_BOOKING")
print("Note: Some records intentionally missing to demonstrate exception handling.")
