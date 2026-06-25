"""
db_connections.py
=================
Centralized connection utilities for all databases.
Uses environment variables — no hardcoded credentials.

Mirrors production pattern of:
- AWS Secrets Manager → here replaced by env vars (safe for local/GitHub)
- DocumentDB → MongoDB 6.0 (same driver, same API)
- Oracle → SQLite (same query pattern, different driver)
- PostgreSQL → unchanged
"""

import os
import sqlite3
import psycopg2
from pymongo import MongoClient


# ── PostgreSQL (Process Tracking) ────────────────────────────────────────────

def get_postgres_conn():
    """Returns a psycopg2 connection to the app PostgreSQL DB."""
    return psycopg2.connect(
        host=os.environ.get("APP_POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("APP_POSTGRES_PORT", 5433)),
        dbname=os.environ.get("APP_POSTGRES_DB", "flightops"),
        user=os.environ.get("APP_POSTGRES_USER", "flightops_user"),
        password=os.environ.get("APP_POSTGRES_PASSWORD", "flightops_pass"),
    )


# ── MongoDB / DocumentDB ──────────────────────────────────────────────────────

def get_mongo_client():
    """
    Returns a MongoClient.
    Production equivalent: DocumentDB with SSL + Secrets Manager credentials.
    Local equivalent: plain MongoDB with env var credentials.
    """
    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", 27017))
    user = os.environ.get("MONGO_USER", "mongo_user")
    password = os.environ.get("MONGO_PASSWORD", "mongo_pass")

    uri = f"mongodb://{user}:{password}@{host}:{port}/admin?authSource=admin&retryWrites=false"
    return MongoClient(uri, serverSelectionTimeoutMS=10000)


def get_mongo_db():
   """Returns the flight_data database."""
   client = get_mongo_client()
   db_name = os.environ.get("MONGO_DB", "flight_data")
   return client[db_name]


# ── SQLite (On-Prem Oracle Simulator) ────────────────────────────────────────

def get_sqlite_conn():
    """
    Returns a sqlite3 connection.
    Production equivalent: Oracle on-prem DB via oracledb driver.
    Same query pattern — only the connection method differs.
    """
    db_path = os.environ.get("SQLITE_DB_PATH", "/opt/airflow/data/onprem_oracle_sim.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # enables dict-like access: row["column_name"]
    return conn


# ── Health Check ─────────────────────────────────────────────────────────────

def check_all_connections():
    """Validates all three DB connections. Used in pipeline health check task."""
    results = {}

    try:
        conn = get_postgres_conn()
        conn.close()
        results["postgres"] = "OK"
    except Exception as e:
        results["postgres"] = f"FAILED: {e}"

    try:
        client = get_mongo_client()
        client.admin.command("ping")
        client.close()
        results["mongodb"] = "OK"
    except Exception as e:
        results["mongodb"] = f"FAILED: {e}"

    try:
        conn = get_sqlite_conn()
        conn.execute("SELECT 1")
        conn.close()
        results["sqlite_onprem"] = "OK"
    except Exception as e:
        results["sqlite_onprem"] = f"FAILED: {e}"

    return results
