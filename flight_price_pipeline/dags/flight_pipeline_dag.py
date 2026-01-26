"""
Flight Price Analysis Pipeline
==============================

This DAG orchestrates the ETL pipeline for Bangladesh flight price data.

Pipeline Flow:
    1. Load CSV → MySQL (raw_flight_data)
    2. Validate data in MySQL (validated_flight_data)
    3. Transfer to PostgreSQL (bronze.validated_flights)
    4. Run DBT transformations (silver and gold layers)

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

# Import our logging utilities
from utils.logging_utils import log_pipeline_event, get_task_context


# ============================================
# DAG Configuration
# ============================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Column mapping: CSV column names → Database column names
COLUMN_MAPPING = {
    'Airline': 'airline',
    'Source': 'source_code',
    'Source Name': 'source_name',
    'Destination': 'destination_code',
    'Destination Name': 'destination_name',
    'Departure Date & Time': 'departure_datetime',
    'Arrival Date & Time': 'arrival_datetime',
    'Duration (hrs)': 'duration_hrs',
    'Stopovers': 'stopovers',
    'Aircraft Type': 'aircraft_type',
    'Class': 'travel_class',
    'Booking Source': 'booking_source',
    'Base Fare (BDT)': 'base_fare_bdt',
    'Tax & Surcharge (BDT)': 'tax_surcharge_bdt',
    'Total Fare (BDT)': 'total_fare_bdt',
    'Seasonality': 'seasonality',
    'Days Before Departure': 'days_before_departure'
}

CSV_FILE_PATH = '/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv'


# ============================================
# Task Functions
# ============================================

def load_csv_to_mysql(**context) -> dict:
    """
    Task 1: Load CSV file into MySQL raw_flight_data table.
    """
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    # Log task start
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        # Step 1: Read CSV file
        print(f"Reading CSV from {CSV_FILE_PATH}")
        df = pd.read_csv(CSV_FILE_PATH)
        total_rows = len(df)
        print(f"Read {total_rows} rows from CSV")
        
        # Step 2: Rename columns
        print("Renaming columns to match database schema")
        df = df.rename(columns=COLUMN_MAPPING)
        print(f"Columns after rename: {list(df.columns)}")
        
        # Step 3: Convert data types
        print("Converting data types")
        
        # DateTime columns
        df['departure_datetime'] = pd.to_datetime(df['departure_datetime'], errors='coerce')
        df['arrival_datetime'] = pd.to_datetime(df['arrival_datetime'], errors='coerce')
        
        # Numeric columns
        numeric_columns = [
            'duration_hrs', 
            'base_fare_bdt', 
            'tax_surcharge_bdt', 
            'total_fare_bdt',
            'days_before_departure'
        ]
        
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Step 4: Add metadata columns
        df['source_file'] = 'Flight_Price_Dataset_of_Bangladesh.csv'
        
        # Step 5: Check for conversion issues
        rows_with_issues = df.isna().any(axis=1).sum()
        if rows_with_issues > 0:
            print(f"Warning: {rows_with_issues} rows have NULL values after conversion")
        
        # Step 6: Insert into MySQL
        print("Connecting to MySQL")
        mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
        engine = mysql_hook.get_sqlalchemy_engine()
        
        # Clear existing data (for idempotency)
        print("Clearing existing data from MySQL tables")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("TRUNCATE TABLE validated_flight_data")
        cursor.execute("TRUNCATE TABLE raw_flight_data")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
        cursor.close()
        conn.close()
        
        # Insert new data
        print(f"Inserting {total_rows} rows into raw_flight_data")
        
        columns_to_insert = [
            'airline', 'source_code', 'source_name', 'destination_code',
            'destination_name', 'departure_datetime', 'arrival_datetime',
            'duration_hrs', 'stopovers', 'aircraft_type', 'travel_class',
            'booking_source', 'base_fare_bdt', 'tax_surcharge_bdt',
            'total_fare_bdt', 'seasonality', 'days_before_departure',
            'source_file'
        ]
        
        df[columns_to_insert].to_sql(
            name='raw_flight_data',
            con=engine,
            if_exists='append',
            index=False,
            chunksize=5000
        )
        
        print(f"Successfully loaded {total_rows} rows into MySQL")
        
        # Step 7: Log success
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            rows_processed=total_rows,
            rows_failed=0,
            metadata={
                'source_file': CSV_FILE_PATH,
                'rows_with_null_values': int(rows_with_issues)
            }
        )
        
        return {
            'rows_loaded': total_rows,
            'rows_with_issues': int(rows_with_issues)
        }
        
    except Exception as e:
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


def validate_mysql_data(**context) -> dict:
    """
    Task 2: Validate data in MySQL and populate validated_flight_data table.
    
    Validation Strategy:
    - HARD RULES (reject if failed): Nulls, negative values, data type issues
    - SOFT RULES (warn but accept): Unexpected categorical values
    """
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
        
        # Step 1: Get raw data
        print("Fetching raw data for validation")
        
        raw_data_query = """
            SELECT 
                id,
                airline,
                source_code,
                source_name,
                destination_code,
                destination_name,
                departure_datetime,
                arrival_datetime,
                duration_hrs,
                stopovers,
                aircraft_type,
                travel_class,
                booking_source,
                base_fare_bdt,
                tax_surcharge_bdt,
                total_fare_bdt,
                seasonality,
                days_before_departure
            FROM raw_flight_data
        """
        
        df = mysql_hook.get_pandas_df(raw_data_query)
        total_rows = len(df)
        print(f"Fetched {total_rows} rows for validation")
        
        # Step 2: Initialize validation columns
        print("Applying validation rules")
        df['is_valid'] = True
        df['validation_errors'] = ''
        df['validation_warnings'] = ''
        
        # =====================================================
        # HARD RULES - These cause rejection (is_valid = FALSE)
        # =====================================================
        
        # Rule 1: Required fields not null
        required_fields = ['airline', 'source_code', 'destination_code']
        for field in required_fields:
            mask = df[field].isna() | (df[field].astype(str).str.strip() == '')
            df.loc[mask, 'is_valid'] = False
            df.loc[mask, 'validation_errors'] += f'{field} is required; '
        
        # Rule 2: Fare values must be positive
        fare_fields = ['base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt']
        for field in fare_fields:
            mask = (df[field].isna()) | (df[field] <= 0)
            df.loc[mask, 'is_valid'] = False
            df.loc[mask, 'validation_errors'] += f'{field} must be positive; '
        
        # Rule 3: IATA codes should be 3 characters
        for field in ['source_code', 'destination_code']:
            mask = df[field].astype(str).str.len() != 3
            df.loc[mask, 'is_valid'] = False
            df.loc[mask, 'validation_errors'] += f'{field} must be 3 characters; '
        
        # Rule 4: Duration must be positive
        mask = (df['duration_hrs'].isna()) | (df['duration_hrs'] <= 0)
        df.loc[mask, 'is_valid'] = False
        df.loc[mask, 'validation_errors'] += 'duration_hrs must be positive; '
        
        # Rule 5: Days before departure should be positive
        mask = (df['days_before_departure'].isna()) | (df['days_before_departure'] < 1)
        df.loc[mask, 'is_valid'] = False
        df.loc[mask, 'validation_errors'] += 'days_before_departure must be at least 1; '
        
        # =====================================================
        # SOFT RULES - These cause warnings but NOT rejection
        # =====================================================
        
        # Known/expected values (for reporting purposes)
        known_seasons = ['Regular', 'Eid', 'Hajj', 'Winter', 'Winter Holidays']
        known_classes = ['Economy', 'Business', 'First', 'First Class', 'Premium Economy']
        known_stopovers = ['Direct', '1 Stop', '2 Stops']
        known_booking_sources = ['Direct Booking', 'Travel Agency', 'Online Website']
        
        # Warn on unexpected seasonality (but don't reject)
        mask = ~df['seasonality'].isin(known_seasons) & df['seasonality'].notna()
        unexpected_seasons = df.loc[mask, 'seasonality'].unique()
        if len(unexpected_seasons) > 0:
            print(f"WARNING: Unexpected seasonality values found: {unexpected_seasons}")
            df.loc[mask, 'validation_warnings'] += f'Unexpected seasonality: {df.loc[mask, "seasonality"]}; '
        
        # Warn on unexpected travel class (but don't reject)
        mask = ~df['travel_class'].isin(known_classes) & df['travel_class'].notna()
        unexpected_classes = df.loc[mask, 'travel_class'].unique()
        if len(unexpected_classes) > 0:
            print(f"WARNING: Unexpected travel_class values found: {unexpected_classes}")
            df.loc[mask, 'validation_warnings'] += f'Unexpected travel_class: {df.loc[mask, "travel_class"]}; '
        
        # Warn on unexpected stopovers (but don't reject)
        mask = ~df['stopovers'].isin(known_stopovers) & df['stopovers'].notna()
        unexpected_stopovers = df.loc[mask, 'stopovers'].unique()
        if len(unexpected_stopovers) > 0:
            print(f"WARNING: Unexpected stopovers values found: {unexpected_stopovers}")
            df.loc[mask, 'validation_warnings'] += f'Unexpected stopovers: {df.loc[mask, "stopovers"]}; '
        
        # Warn on unexpected booking source (but don't reject)
        mask = ~df['booking_source'].isin(known_booking_sources) & df['booking_source'].notna()
        unexpected_sources = df.loc[mask, 'booking_source'].unique()
        if len(unexpected_sources) > 0:
            print(f"WARNING: Unexpected booking_source values found: {unexpected_sources}")
            df.loc[mask, 'validation_warnings'] += f'Unexpected booking_source: {df.loc[mask, "booking_source"]}; '
        
        # Warn if days_before_departure > 365 (unusual but not invalid)
        mask = df['days_before_departure'] > 365
        if mask.sum() > 0:
            print(f"WARNING: {mask.sum()} rows have days_before_departure > 365")
            df.loc[mask, 'validation_warnings'] += 'days_before_departure > 365 (unusual); '
        
        # =====================================================
        # Clean up and summarize
        # =====================================================
        
        # Clean up error/warning strings
        df['validation_errors'] = df['validation_errors'].str.rstrip('; ')
        df.loc[df['validation_errors'] == '', 'validation_errors'] = None
        
        df['validation_warnings'] = df['validation_warnings'].str.rstrip('; ')
        df.loc[df['validation_warnings'] == '', 'validation_warnings'] = None
        
        # Count results
        valid_count = df['is_valid'].sum()
        invalid_count = total_rows - valid_count
        warning_count = df['validation_warnings'].notna().sum()
        
        print(f"Validation complete:")
        print(f"  - Valid records: {valid_count}")
        print(f"  - Invalid records: {invalid_count}")
        print(f"  - Records with warnings: {warning_count}")
        
        # Step 3: Insert into validated_flight_data
        print("Inserting validated data into MySQL")
        
        # Rename id to raw_id for foreign key
        df = df.rename(columns={'id': 'raw_id'})
        
        # Get SQLAlchemy engine
        engine = mysql_hook.get_sqlalchemy_engine()
        
        # Columns to insert (note: validation_warnings won't be stored in MySQL 
        # since the table doesn't have that column, but we log it)
        columns_to_insert = [
            'airline', 'source_code', 'source_name', 'destination_code',
            'destination_name', 'departure_datetime', 'arrival_datetime',
            'duration_hrs', 'stopovers', 'aircraft_type', 'travel_class',
            'booking_source', 'base_fare_bdt', 'tax_surcharge_bdt',
            'total_fare_bdt', 'seasonality', 'days_before_departure',
            'is_valid', 'validation_errors', 'raw_id'
        ]
        
        df[columns_to_insert].to_sql(
            name='validated_flight_data',
            con=engine,
            if_exists='append',
            index=False,
            chunksize=5000
        )
        
        print(f"Inserted {total_rows} rows into validated_flight_data")
        
        # Step 4: Log success with detailed metrics
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            rows_processed=int(valid_count),
            rows_failed=int(invalid_count),
            metadata={
                'total_rows': total_rows,
                'valid_rows': int(valid_count),
                'invalid_rows': int(invalid_count),
                'rows_with_warnings': int(warning_count),
                'validation_rate': f"{(valid_count/total_rows)*100:.2f}%",
                'unexpected_seasons': list(unexpected_seasons) if len(unexpected_seasons) > 0 else None,
                'unexpected_classes': list(unexpected_classes) if len(unexpected_classes) > 0 else None
            }
        )
        
        return {
            'total_rows': total_rows,
            'valid_rows': int(valid_count),
            'invalid_rows': int(invalid_count),
            'rows_with_warnings': int(warning_count)
        }
        
    except Exception as e:
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


def transfer_to_postgres_bronze(**context) -> dict:
    """
    Task 3: Transfer validated data from MySQL to PostgreSQL bronze layer.
    """
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        # Step 1: Extract from MySQL
        print("Extracting validated data from MySQL")
        mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
        
        extract_query = """
            SELECT 
                v.airline,
                v.source_code,
                v.source_name,
                v.destination_code,
                v.destination_name,
                v.departure_datetime,
                v.arrival_datetime,
                v.duration_hrs,
                v.stopovers,
                v.aircraft_type,
                v.travel_class,
                v.booking_source,
                v.base_fare_bdt,
                v.tax_surcharge_bdt,
                v.total_fare_bdt,
                v.seasonality,
                v.days_before_departure,
                v.is_valid,
                v.validation_errors,
                v.raw_id as mysql_raw_id,
                v.id as mysql_validated_id,
                r.loaded_at as mysql_loaded_at
            FROM validated_flight_data v
            JOIN raw_flight_data r ON v.raw_id = r.id
        """
        
        df = mysql_hook.get_pandas_df(extract_query)
        total_rows = len(df)
        print(f"Extracted {total_rows} rows from MySQL")
        
        # Step 2: Convert data types for PostgreSQL compatibility
        # MySQL stores boolean as TINYINT (0/1), PostgreSQL needs actual boolean
        print("Converting data types for PostgreSQL")
        df['is_valid'] = df['is_valid'].astype(bool)
        
        # Step 3: Load into PostgreSQL
        print("Loading data into PostgreSQL bronze layer")
        postgres_hook = PostgresHook(postgres_conn_id='postgres_analytics')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        # Clear existing bronze data (for idempotency)
        print("Clearing existing bronze data")
        postgres_hook.run("TRUNCATE TABLE bronze.validated_flights RESTART IDENTITY")
        
        # Insert data
        print(f"Inserting {total_rows} rows into bronze.validated_flights")
        
        df.to_sql(
            name='validated_flights',
            schema='bronze',
            con=engine,
            if_exists='append',
            index=False,
            chunksize=5000
        )
        
        print(f"Successfully transferred {total_rows} rows to PostgreSQL bronze")
        
        # Step 4: Verify transfer
        verify_query = "SELECT COUNT(*) FROM bronze.validated_flights"
        result = postgres_hook.get_first(verify_query)
        postgres_count = result[0]
        
        if postgres_count != total_rows:
            raise ValueError(
                f"Row count mismatch: MySQL={total_rows}, PostgreSQL={postgres_count}"
            )
        
        print(f"Verified: {postgres_count} rows in PostgreSQL bronze layer")
        
        # Step 5: Log success
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            rows_processed=total_rows,
            metadata={
                'source': 'mysql.validated_flight_data',
                'destination': 'postgres.bronze.validated_flights',
                'rows_transferred': total_rows
            }
        )
        
        return {
            'rows_transferred': total_rows
        }
        
    except Exception as e:
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id='flight_price_pipeline',
    default_args=default_args,
    description='ETL pipeline for Bangladesh flight price analysis',
    schedule_interval=None,
    catchup=False,
    tags=['flight', 'etl', 'bangladesh']
) as dag:
    
    # Task 1: Load CSV to MySQL
    task_load_csv = PythonOperator(
        task_id='load_csv_to_mysql',
        python_callable=load_csv_to_mysql,
        provide_context=True
    )
    
    # Task 2: Validate data in MySQL
    task_validate = PythonOperator(
        task_id='validate_mysql_data',
        python_callable=validate_mysql_data,
        provide_context=True
    )
    
    # Task 3: Transfer to PostgreSQL Bronze
    task_transfer = PythonOperator(
        task_id='transfer_to_postgres_bronze',
        python_callable=transfer_to_postgres_bronze,
        provide_context=True
    )
    
    # Task 4: Run DBT transformations and snapshots
    task_dbt = BashOperator(
        task_id='run_dbt_transformations',
        bash_command='''
            cd /opt/airflow/dbt_project && \
            dbt run --profiles-dir . && \
            dbt snapshot --profiles-dir . && \
            dbt test --profiles-dir .
        ''',
    )

    
    # Define task dependencies
    task_load_csv >> task_validate >> task_transfer >> task_dbt
