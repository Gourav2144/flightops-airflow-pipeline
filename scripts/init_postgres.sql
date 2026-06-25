-- ============================================================
-- FlightOps PostgreSQL Schema
-- Simulates: process tracking + config (mirrors production pattern)
-- ============================================================

-- Process tracking table (mirrors OnPremIdSyncConfig in production)
CREATE TABLE IF NOT EXISTS flightops.pipeline_run_log (
    run_id        SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    start_dtm     TIMESTAMP    NOT NULL DEFAULT NOW(),
    end_dtm       TIMESTAMP,
    run_status    VARCHAR(20)  NOT NULL DEFAULT 'Running',
    records_processed INT      DEFAULT 0,
    records_failed    INT      DEFAULT 0,
    created_by    VARCHAR(50)  DEFAULT 'Airflow'
);

-- Grant
CREATE SCHEMA IF NOT EXISTS flightops;

-- Re-create in correct schema
DROP TABLE IF EXISTS flightops.pipeline_run_log;
CREATE TABLE flightops.pipeline_run_log (
    run_id        SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    start_dtm     TIMESTAMP    NOT NULL DEFAULT NOW(),
    end_dtm       TIMESTAMP,
    run_status    VARCHAR(20)  NOT NULL DEFAULT 'Running',
    records_processed INT      DEFAULT 0,
    records_failed    INT      DEFAULT 0,
    created_by    VARCHAR(50)  DEFAULT 'Airflow'
);

-- Watermark table - tracks last processed ID per collection (incremental load)
CREATE TABLE flightops.incremental_watermark (
    collection_name VARCHAR(100) PRIMARY KEY,
    last_processed_id BIGINT      NOT NULL DEFAULT 0,
    last_run_dtm      TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_by        VARCHAR(50) DEFAULT 'Airflow'
);

-- Seed watermarks for all collections
INSERT INTO flightops.incremental_watermark (collection_name, last_processed_id)
VALUES
    ('FlightSchedules', 0),
    ('CrewAssignments', 0),
    ('RouteOperations', 0),
    ('AircraftStatus', 0),
    ('PassengerBookings', 0)
ON CONFLICT DO NOTHING;

-- Pipeline config table
CREATE TABLE flightops.pipeline_config (
    config_key   VARCHAR(100) PRIMARY KEY,
    config_value VARCHAR(500),
    description  VARCHAR(200),
    updated_dtm  TIMESTAMP DEFAULT NOW()
);

INSERT INTO flightops.pipeline_config (config_key, config_value, description)
VALUES
    ('batch_size',      '1000',    'Number of records per batch'),
    ('retry_attempts',  '3',       'Max retry attempts on failure'),
    ('alert_threshold', '100',     'Alert if exceptions exceed this count')
ON CONFLICT DO NOTHING;

-- Key mapping table (mirrors OnPremNonFlyingKey in production)
-- Stores: AWS MongoDB ID <-> On-Prem SQLite ID mapping
CREATE TABLE flightops.id_key_mapping (
    mapping_id      SERIAL PRIMARY KEY,
    aws_doc_id      VARCHAR(100) NOT NULL,
    onprem_id       BIGINT       NOT NULL,
    collection_name VARCHAR(100) NOT NULL,
    record_type     VARCHAR(50)  NOT NULL,
    run_id          INT          REFERENCES flightops.pipeline_run_log(run_id),
    created_dtm     TIMESTAMP    DEFAULT NOW(),
    created_by      VARCHAR(50)  DEFAULT 'Airflow'
);

-- Exception tracking table
CREATE TABLE flightops.exception_log (
    exception_id    SERIAL PRIMARY KEY,
    business_key    VARCHAR(500) NOT NULL,
    collection_name VARCHAR(100) NOT NULL,
    error_reason    VARCHAR(500),
    run_id          INT          REFERENCES flightops.pipeline_run_log(run_id),
    created_dtm     TIMESTAMP    DEFAULT NOW()
);

-- Index for fast lookups
CREATE INDEX idx_key_mapping_aws_doc ON flightops.id_key_mapping(aws_doc_id);
CREATE INDEX idx_key_mapping_collection ON flightops.id_key_mapping(collection_name);
CREATE INDEX idx_exception_collection ON flightops.exception_log(collection_name);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA flightops TO flightops_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA flightops TO flightops_user;
