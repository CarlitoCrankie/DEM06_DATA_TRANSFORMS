-- PostgreSQL Analytics Initialization Script
-- Medallion Architecture: Bronze, Silver, Gold

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

-- Grant Permissions
GRANT ALL ON SCHEMA bronze TO analytics_user;
GRANT ALL ON SCHEMA silver TO analytics_user;
GRANT ALL ON SCHEMA gold TO analytics_user;
GRANT ALL ON SCHEMA audit TO analytics_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT ALL ON TABLES TO analytics_user;

-- Audit Tables
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(255),
    task_id VARCHAR(100),
    status VARCHAR(50),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    rows_processed INT,
    rows_failed INT,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit.data_quality_results (
    id SERIAL PRIMARY KEY,
    check_name VARCHAR(100),
    check_type VARCHAR(50),
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    passed BOOLEAN,
    expected_value TEXT,
    actual_value TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bronze Layer
CREATE TABLE IF NOT EXISTS bronze.validated_flights (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100),
    source_code VARCHAR(10),
    source_name VARCHAR(255),
    destination_code VARCHAR(10),
    destination_name VARCHAR(255),
    departure_datetime TIMESTAMP,
    arrival_datetime TIMESTAMP,
    duration_hrs DECIMAL(6, 2),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    travel_class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare_bdt DECIMAL(12, 2),
    tax_surcharge_bdt DECIMAL(12, 2),
    total_fare_bdt DECIMAL(12, 2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    is_valid BOOLEAN,
    validation_errors TEXT,
    mysql_raw_id INT,
    mysql_validated_id INT,
    mysql_loaded_at TIMESTAMP,
    bronze_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_bronze_airline ON bronze.validated_flights(airline);
CREATE INDEX IF NOT EXISTS idx_bronze_route ON bronze.validated_flights(source_code, destination_code);
CREATE INDEX IF NOT EXISTS idx_bronze_valid ON bronze.validated_flights(is_valid);
CREATE INDEX IF NOT EXISTS idx_bronze_seasonality ON bronze.validated_flights(seasonality);
CREATE INDEX IF NOT EXISTS idx_audit_dag_run ON audit.pipeline_runs(dag_id, dag_run_id);
