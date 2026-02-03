"""
Flight Price Analysis Pipeline
==============================

Pipeline Flow:
    0. Extract from Kaggle (download, track metadata, evolve schema)
    1. Load CSV → MySQL (raw_flight_data)
    2. Validate data in MySQL (validated_flight_data)
    3. Transfer to PostgreSQL (bronze.validated_flights)
    4. Run DBT transformations (silver and gold layers)
"""

from datetime import datetime, timedelta, time
import subprocess
import logging
import hashlib
import json
import os
import zipfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql

from utils.logging_utils import log_pipeline_event, get_task_context

logger = logging.getLogger("airflow.task")


# ============================================
# Configuration
# ============================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'sla': timedelta(minutes=10),
}

# Kaggle dataset configuration
KAGGLE_DATASET = 'mahatiratusher/flight-price-dataset-of-bangladesh'
KAGGLE_FILENAME = 'Flight_Price_Dataset_of_Bangladesh.csv'
DATA_DIR = '/opt/airflow/data'
CSV_FILE_PATH = f'{DATA_DIR}/{KAGGLE_FILENAME}'
DBT_PROJECT_DIR = '/opt/airflow/dbt_project'
BATCH_SIZE = 5000

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

# Python type to MySQL type mapping
PYTHON_TO_MYSQL_TYPE = {
    'int64': 'BIGINT',
    'float64': 'DECIMAL(12, 2)',
    'object': 'VARCHAR(255)',
    'datetime64[ns]': 'DATETIME',
    'bool': 'BOOLEAN',
}


# ============================================
# Helper Functions
# ============================================

def get_mysql_connection_params():
    conn = BaseHook.get_connection('mysql_staging')
    return {
        'host': conn.host,
        'port': conn.port or 3306,
        'user': conn.login,
        'password': conn.password,
        'database': conn.schema
    }


def get_mysql_engine():
    params = get_mysql_connection_params()
    connection_string = (
        f"mysql+pymysql://{params['user']}:{params['password']}@"
        f"{params['host']}:{params['port']}/{params['database']}"
    )
    return create_engine(connection_string)


def get_mysql_connection():
    params = get_mysql_connection_params()
    return pymysql.connect(
        host=params['host'],
        port=params['port'],
        user=params['user'],
        password=params['password'],
        database=params['database']
    )


def execute_mysql_query(query):
    engine = get_mysql_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    finally:
        engine.dispose()


def get_schema_hash(columns: list) -> str:
    """Generate a hash of column names for change detection."""
    columns_str = ','.join(sorted(columns))
    return hashlib.sha256(columns_str.encode()).hexdigest()


def get_mysql_table_columns(table_name: str) -> list:
    """Get existing columns from MySQL table."""
    query = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'flight_staging' 
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """
    df = execute_mysql_query(query)
    return df['COLUMN_NAME'].tolist()


def infer_mysql_type(dtype: str, sample_values: pd.Series) -> str:
    """Infer MySQL column type from pandas dtype."""
    dtype_str = str(dtype)
    
    if dtype_str in PYTHON_TO_MYSQL_TYPE:
        return PYTHON_TO_MYSQL_TYPE[dtype_str]
    
    if 'int' in dtype_str:
        return 'BIGINT'
    elif 'float' in dtype_str:
        return 'DECIMAL(12, 2)'
    elif 'datetime' in dtype_str:
        return 'DATETIME'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    else:
        max_len = sample_values.astype(str).str.len().max()
        if pd.isna(max_len) or max_len < 255:
            return 'VARCHAR(255)'
        else:
            return 'TEXT'


# ============================================
# Task Functions
# ============================================

def download_from_kaggle_with_retry(dataset_id: str, output_dir: str, max_retries: int = 3) -> dict:
    """
    Download dataset from Kaggle with retry logic and exponential backoff.
    
    Returns:
        dict with download status and details
    """
    delay = 5  # Initial delay in seconds
    last_error = None
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Kaggle download attempt {attempt + 1}/{max_retries}")
            
            result = subprocess.run(
                ['kaggle', 'datasets', 'download', '-d', dataset_id, '-p', output_dir, '--unzip'],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                logger.info("Kaggle download successful")
                return {'success': True, 'attempts': attempt + 1}
            
            last_error = result.stderr
            logger.warning(f"Kaggle download failed: {result.stderr}")
            
        except subprocess.TimeoutExpired:
            last_error = "Download timed out after 5 minutes"
            logger.warning(last_error)
        except Exception as e:
            last_error = str(e)
            logger.warning(f"Kaggle download error: {e}")
        
        if attempt < max_retries - 1:
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2  # Exponential backoff
    
    return {'success': False, 'attempts': max_retries, 'error': last_error}


def extract_from_kaggle(**context) -> dict:
    """
    Task 0: Download dataset from Kaggle and track metadata.
    
    Features:
    - Loads credentials from .env, Airflow Variables, or kaggle.json
    - Retries with exponential backoff on failure
    - Falls back to existing file if download fails
    - Tracks metadata and schema changes
    """
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        # =====================================================
        # Step 1: Setup Kaggle Credentials
        # =====================================================
        
        # Priority: Environment (.env) > Airflow Variables > kaggle.json
        kaggle_username = os.environ.get('KAGGLE_USERNAME')
        kaggle_key = os.environ.get('KAGGLE_KEY')
        credential_source = None
        
        if kaggle_username and kaggle_key:
            credential_source = 'environment variables (.env)'
        else:
            # Try Airflow Variables
            try:
                kaggle_username = Variable.get('KAGGLE_USERNAME', default_var=None)
                kaggle_key = Variable.get('KAGGLE_KEY', default_var=None)
                if kaggle_username and kaggle_key:
                    os.environ['KAGGLE_USERNAME'] = kaggle_username
                    os.environ['KAGGLE_KEY'] = kaggle_key
                    credential_source = 'Airflow Variables'
            except:
                pass
        
        if not credential_source:
            # Check for kaggle.json
            kaggle_json_path = '/home/airflow/.kaggle/kaggle.json'
            if os.path.exists(kaggle_json_path):
                credential_source = 'kaggle.json'
            else:
                raise ValueError(
                    "Kaggle credentials not found. Provide them via:\n"
                    "1. .env file (KAGGLE_USERNAME, KAGGLE_KEY)\n"
                    "2. Airflow Variables\n"
                    "3. ~/.kaggle/kaggle.json"
                )
        
        logger.info(f"Kaggle credentials loaded from {credential_source}")
        
        # =====================================================
        # Step 2: Download Dataset with Retries
        # =====================================================
        
        os.makedirs(DATA_DIR, exist_ok=True)
        
        logger.info(f"Downloading dataset: {KAGGLE_DATASET}")
        download_result = download_from_kaggle_with_retry(KAGGLE_DATASET, DATA_DIR, max_retries=3)
        
        if not download_result['success']:
            # Check if file already exists as fallback
            if os.path.exists(CSV_FILE_PATH):
                logger.warning(
                    f"Kaggle download failed after {download_result['attempts']} attempts, "
                    f"but existing file found: {CSV_FILE_PATH}"
                )
                logger.warning(f"Last error: {download_result.get('error')}")
            else:
                raise Exception(
                    f"Kaggle download failed after {download_result['attempts']} attempts: "
                    f"{download_result.get('error')}"
                )
        
        # =====================================================
        # Step 3: Read and Analyze CSV
        # =====================================================
        
        logger.info(f"Reading CSV: {CSV_FILE_PATH}")
        df = pd.read_csv(CSV_FILE_PATH)
        
        csv_columns = list(df.columns)
        row_count = len(df)
        file_size = os.path.getsize(CSV_FILE_PATH)
        schema_hash = get_schema_hash(csv_columns)
        
        logger.info(f"CSV has {row_count} rows and {len(csv_columns)} columns")
        
        # Map CSV columns to database columns
        db_columns = [
            COLUMN_MAPPING.get(col, col.lower().replace(' ', '_').replace('(', '').replace(')', '')) 
            for col in csv_columns
        ]
        
        # =====================================================
        # Step 4: Check for Schema Changes
        # =====================================================
        
        existing_columns = get_mysql_table_columns('raw_flight_data')
        system_columns = ['id', 'loaded_at', 'source_file', 'metadata_id']
        data_columns = [col for col in existing_columns if col not in system_columns]
        
        new_columns = [col for col in db_columns if col not in existing_columns]
        schema_changed = len(new_columns) > 0
        
        if schema_changed:
            logger.warning(f"Schema change detected! New columns: {new_columns}")
            
            # Alter tables to add new columns
            conn = get_mysql_connection()
            try:
                cursor = conn.cursor()
                
                for new_col in new_columns:
                    csv_col_idx = db_columns.index(new_col)
                    csv_col_name = csv_columns[csv_col_idx]
                    mysql_type = infer_mysql_type(df[csv_col_name].dtype, df[csv_col_name])
                    
                    for table in ['raw_flight_data', 'validated_flight_data']:
                        alter_sql = f"ALTER TABLE {table} ADD COLUMN `{new_col}` {mysql_type} NULL"
                        logger.info(f"Executing: {alter_sql}")
                        cursor.execute(alter_sql)
                
                conn.commit()
                logger.info(f"Added {len(new_columns)} new columns to MySQL tables")
                
            finally:
                cursor.close()
                conn.close()
        else:
            logger.info("No schema changes detected")
        
        # =====================================================
        # Step 5: Record Metadata
        # =====================================================
        
        # Get previous metadata (handle case where table is empty)
        try:
            previous_metadata = execute_mysql_query(
                "SELECT id, schema_hash FROM dataset_metadata ORDER BY id DESC LIMIT 1"
            )
            previous_metadata_id = int(previous_metadata.iloc[0]['id']) if len(previous_metadata) > 0 else None
        except:
            previous_metadata_id = None
        
        columns_info = {col: str(df[col].dtype) for col in csv_columns}
        
        conn = get_mysql_connection()
        try:
            cursor = conn.cursor()
            
            insert_sql = """
                INSERT INTO dataset_metadata 
                (dataset_name, kaggle_dataset_id, file_name, file_size_bytes, 
                 row_count, column_count, columns_json, schema_hash, 
                 schema_changed, new_columns_added, previous_metadata_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (
                'Flight Price Dataset of Bangladesh',
                KAGGLE_DATASET,
                KAGGLE_FILENAME,
                file_size,
                row_count,
                len(csv_columns),
                json.dumps(columns_info),
                schema_hash,
                schema_changed,
                json.dumps(new_columns) if new_columns else None,
                previous_metadata_id
            ))
            
            metadata_id = cursor.lastrowid
            conn.commit()
            
            logger.info(f"Recorded metadata with ID: {metadata_id}")
            
        finally:
            cursor.close()
            conn.close()
        
        # Store in XCom for downstream tasks
        context['ti'].xcom_push(key='metadata_id', value=metadata_id)
        context['ti'].xcom_push(key='schema_changed', value=schema_changed)
        context['ti'].xcom_push(key='new_columns', value=new_columns)
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            rows_processed=row_count,
            metadata={
                'kaggle_dataset': KAGGLE_DATASET,
                'credential_source': credential_source,
                'download_attempts': download_result.get('attempts', 1),
                'file_size_bytes': file_size,
                'row_count': row_count,
                'column_count': len(csv_columns),
                'schema_changed': schema_changed,
                'new_columns': new_columns,
                'metadata_id': metadata_id
            }
        )
        
        return {
            'metadata_id': metadata_id,
            'row_count': row_count,
            'column_count': len(csv_columns),
            'schema_changed': schema_changed,
            'new_columns': new_columns
        }
        
    except Exception as e:
        logger.exception(f"Kaggle extraction failed: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


def load_csv_to_mysql(**context) -> dict:
    """Task 1: Load CSV file into MySQL raw_flight_data table."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    # Get metadata_id from previous task
    metadata_id = context['ti'].xcom_pull(task_ids='extract_from_kaggle', key='metadata_id')
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        logger.info(f"Reading CSV from {CSV_FILE_PATH}")
        
        if not os.path.exists(CSV_FILE_PATH):
            raise FileNotFoundError(f"CSV file not found: {CSV_FILE_PATH}")

        if os.path.getsize(CSV_FILE_PATH) == 0:
            raise ValueError(f"CSV file is empty: {CSV_FILE_PATH}")

        df = pd.read_csv(CSV_FILE_PATH)
        total_rows = len(df)
        logger.info(f"Read {total_rows} rows from CSV")
        
        # Rename columns
        logger.info("Renaming columns to match database schema")
        df = df.rename(columns=COLUMN_MAPPING)
        
        # For any columns not in mapping, normalize the name
        for col in df.columns:
            if col not in COLUMN_MAPPING.values():
                new_col = col.lower().replace(' ', '_').replace('(', '').replace(')', '')
                if new_col != col:
                    df = df.rename(columns={col: new_col})
        
        logger.info(f"Columns after rename: {list(df.columns)}")
        
        # Convert data types
        logger.info("Converting data types")
        
        if 'departure_datetime' in df.columns:
            df['departure_datetime'] = pd.to_datetime(df['departure_datetime'], errors='coerce')
        if 'arrival_datetime' in df.columns:
            df['arrival_datetime'] = pd.to_datetime(df['arrival_datetime'], errors='coerce')
        
        numeric_columns = [
            'duration_hrs', 'base_fare_bdt', 'tax_surcharge_bdt', 
            'total_fare_bdt', 'days_before_departure'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Add metadata columns
        df['source_file'] = KAGGLE_FILENAME
        df['metadata_id'] = metadata_id
        
        rows_with_issues = df.isna().any(axis=1).sum()
        if rows_with_issues > 0:
            logger.warning(f"{rows_with_issues} rows have NULL values after conversion")
        
        # Clear existing data
        logger.info("Clearing existing data from MySQL tables")
        conn = get_mysql_connection()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute("TRUNCATE TABLE validated_flight_data")
                cursor.execute("TRUNCATE TABLE raw_flight_data")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
                conn.commit()
            finally:
                cursor.close()
        finally:
            conn.close()

        # Insert data
        logger.info(f"Inserting {total_rows} rows into raw_flight_data")
        
        engine = get_mysql_engine()
        try:
            df.to_sql(
                name='raw_flight_data',
                con=engine,
                if_exists='append',
                index=False,
                chunksize=BATCH_SIZE
            )
        finally:
            engine.dispose()
        
        logger.info(f"Successfully loaded {total_rows} rows into MySQL")
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            rows_processed=total_rows,
            rows_failed=0,
            metadata={
                'source_file': CSV_FILE_PATH,
                'rows_with_null_values': int(rows_with_issues),
                'metadata_id': metadata_id
            }
        )
        
        return {
            'rows_loaded': total_rows,
            'rows_with_issues': int(rows_with_issues)
        }
        
    except Exception as e:
        logger.exception(f"Failed to load CSV: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


def validate_mysql_data(**context) -> dict:
    """Task 2: Validate data in MySQL."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        logger.info("Fetching raw data for validation")
        
        # Get all columns dynamically
        columns = get_mysql_table_columns('raw_flight_data')
        columns_str = ', '.join([f'`{col}`' for col in columns])
        
        raw_data_query = f"SELECT {columns_str} FROM raw_flight_data"
        
        df = execute_mysql_query(raw_data_query)
        total_rows = len(df)
        logger.info(f"Fetched {total_rows} rows for validation")
        
        logger.info("Applying validation rules")
        df['is_valid'] = True
        df['validation_errors'] = ''
        
        # HARD RULES - only validate columns that exist
        required_fields = ['airline', 'source_code', 'destination_code']
        for field in required_fields:
            if field in df.columns:
                mask = df[field].isna() | (df[field].astype(str).str.strip() == '')
                df.loc[mask, 'is_valid'] = False
                df.loc[mask, 'validation_errors'] += f'{field} is required; '
        
        fare_fields = ['base_fare_bdt', 'total_fare_bdt']
        for field in fare_fields:
            if field in df.columns:
                mask = (df[field].isna()) | (df[field] <= 0)
                df.loc[mask, 'is_valid'] = False
                df.loc[mask, 'validation_errors'] += f'{field} must be positive; '

        if 'tax_surcharge_bdt' in df.columns:
            mask = (df['tax_surcharge_bdt'].isna()) | (df['tax_surcharge_bdt'] < 0)
            df.loc[mask, 'is_valid'] = False
            df.loc[mask, 'validation_errors'] += 'tax_surcharge_bdt cannot be negative; '

        for field in ['source_code', 'destination_code']:
            if field in df.columns:
                mask = df[field].astype(str).str.len() != 3
                df.loc[mask, 'is_valid'] = False
                df.loc[mask, 'validation_errors'] += f'{field} must be 3 characters; '
        
        if 'duration_hrs' in df.columns:
            mask = (df['duration_hrs'].isna()) | (df['duration_hrs'] <= 0)
            df.loc[mask, 'is_valid'] = False
            df.loc[mask, 'validation_errors'] += 'duration_hrs must be positive; '
        
        if 'days_before_departure' in df.columns:
            mask = (df['days_before_departure'].isna()) | (df['days_before_departure'] < 1)
            df.loc[mask, 'is_valid'] = False
            df.loc[mask, 'validation_errors'] += 'days_before_departure must be at least 1; '
        
        # Clean up
        df['validation_errors'] = df['validation_errors'].str.rstrip('; ')
        df.loc[df['validation_errors'] == '', 'validation_errors'] = None
        
        valid_count = df['is_valid'].sum()
        invalid_count = total_rows - valid_count
        
        logger.info(f"Validation complete - Valid: {valid_count}, Invalid: {invalid_count}")
        
        # Prepare for insert
        df = df.rename(columns={'id': 'raw_id'})
        
        # Get validated_flight_data columns
        validated_columns = get_mysql_table_columns('validated_flight_data')
        
        # Only include columns that exist in both dataframe and table
        columns_to_insert = [col for col in df.columns if col in validated_columns and col != 'id']
        
        logger.info(f"Inserting columns: {columns_to_insert}")
        
        engine = get_mysql_engine()
        try:
            df[columns_to_insert].to_sql(
                name='validated_flight_data',
                con=engine,
                if_exists='append',
                index=False,
                chunksize=BATCH_SIZE
            )
        finally:
            engine.dispose()
        
        logger.info(f"Inserted {total_rows} rows into validated_flight_data")
        
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
                'validation_rate': f"{(valid_count/total_rows)*100:.2f}%"
            }
        )
        
        return {
            'total_rows': total_rows,
            'valid_rows': int(valid_count),
            'invalid_rows': int(invalid_count)
        }
        
    except Exception as e:
        logger.exception(f"Validation failed: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise

def transfer_to_postgres_bronze(**context) -> dict:
    """Task 3: Transfer validated data from MySQL to PostgreSQL bronze layer."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        logger.info("Extracting validated data from MySQL")
        
        # Get columns dynamically
        validated_columns = get_mysql_table_columns('validated_flight_data')
        raw_columns = get_mysql_table_columns('raw_flight_data')
        
        # Build dynamic SELECT query
        v_cols = [f'v.`{col}`' for col in validated_columns if col not in ['id', 'raw_id', 'validated_at']]
        
        extract_query = f"""
            SELECT 
                {', '.join(v_cols)},
                v.raw_id as mysql_raw_id,
                v.id as mysql_validated_id,
                r.loaded_at as mysql_loaded_at
            FROM validated_flight_data v
            JOIN raw_flight_data r ON v.raw_id = r.id
            WHERE v.is_valid = 1
        """
        
        df = execute_mysql_query(extract_query)
        total_rows = len(df)
        logger.info(f"Extracted {total_rows} rows from MySQL")
        
        if total_rows == 0:
            logger.warning("No valid rows to transfer")
            log_pipeline_event(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_id,
                status='completed',
                rows_processed=0,
                metadata={'message': 'No valid rows to transfer'}
            )
            return {'rows_transferred': 0}
        
        logger.info("Converting data types for PostgreSQL")
        if 'is_valid' in df.columns:
            df['is_valid'] = df['is_valid'].astype(bool)
        df['bronze_loaded_at'] = datetime.now()
        
        logger.info("Loading data into PostgreSQL bronze layer")
        postgres_hook = PostgresHook(postgres_conn_id='postgres_analytics')
        
        # Check if we need to add new columns to PostgreSQL
        schema_changed = context['ti'].xcom_pull(task_ids='extract_from_kaggle', key='schema_changed')
        new_columns = context['ti'].xcom_pull(task_ids='extract_from_kaggle', key='new_columns') or []
        
        if schema_changed and new_columns:
            logger.info(f"Adding new columns to PostgreSQL: {new_columns}")
            for new_col in new_columns:
                try:
                    # Infer PostgreSQL type from dataframe
                    if new_col in df.columns:
                        dtype = str(df[new_col].dtype)
                        if 'int' in dtype:
                            pg_type = 'BIGINT'
                        elif 'float' in dtype:
                            pg_type = 'NUMERIC(12, 2)'
                        elif 'datetime' in dtype:
                            pg_type = 'TIMESTAMP'
                        elif 'bool' in dtype:
                            pg_type = 'BOOLEAN'
                        else:
                            pg_type = 'VARCHAR(255)'
                        
                        alter_sql = f'ALTER TABLE bronze.validated_flights ADD COLUMN IF NOT EXISTS "{new_col}" {pg_type}'
                        logger.info(f"Executing: {alter_sql}")
                        postgres_hook.run(alter_sql)
                except Exception as e:
                    logger.warning(f"Could not add column {new_col}: {e}")
        
        logger.info("Clearing existing bronze data")
        postgres_hook.run("TRUNCATE TABLE bronze.validated_flights RESTART IDENTITY")
        
        logger.info(f"Inserting {total_rows} rows into bronze.validated_flights")
        
        engine = postgres_hook.get_sqlalchemy_engine()
        
        df.to_sql(
            name='validated_flights',
            schema='bronze',
            con=engine,
            if_exists='append',
            index=False,
            chunksize=BATCH_SIZE
        )
        
        logger.info(f"Successfully transferred {total_rows} rows to PostgreSQL bronze")
        
        # Verify transfer
        verify_query = "SELECT COUNT(*) FROM bronze.validated_flights"
        result = postgres_hook.get_first(verify_query)
        postgres_count = result[0]
        
        if postgres_count != total_rows:
            raise ValueError(
                f"Row count mismatch: MySQL={total_rows}, PostgreSQL={postgres_count}"
            )
        
        logger.info(f"Verified: {postgres_count} rows in PostgreSQL bronze layer")
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            rows_processed=total_rows,
            metadata={
                'source': 'mysql.validated_flight_data',
                'destination': 'postgres.bronze.validated_flights',
                'rows_transferred': total_rows,
                'schema_changed': schema_changed,
                'new_columns_added': new_columns
            }
        )
        
        return {'rows_transferred': total_rows}
        
    except Exception as e:
        logger.exception(f"Transfer failed: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


def run_dbt_transformations(**context) -> dict:
    """Task 4: Run DBT transformations."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    results = {
        'dbt_snapshot': None,
        'dbt_run': None,
        'dbt_test': None,
    }
    
    commands = [
        ('dbt_snapshot', ['dbt', 'snapshot', '--profiles-dir', '.']),
        ('dbt_run', ['dbt', 'run', '--profiles-dir', '.']),
        ('dbt_test', ['dbt', 'test', '--profiles-dir', '.']),
    ]
    
    try:
        for step_name, cmd in commands:
            logger.info(f"Starting {step_name}")
            
            result = subprocess.run(
                cmd,
                cwd=DBT_PROJECT_DIR,
                capture_output=True,
                text=True
            )
            
            if result.stdout:
                logger.info(result.stdout)
            
            if result.returncode != 0:
                logger.error(f"{step_name} failed with return code {result.returncode}")
                if result.stderr:
                    logger.error(result.stderr)
                
                results[step_name] = 'failed'
                
                log_pipeline_event(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    task_id=task_id,
                    status='failed',
                    error_message=f"{step_name} failed: {result.stderr[:500] if result.stderr else 'Unknown error'}",
                    metadata={'failed_step': step_name, 'results': results}
                )
                
                raise Exception(f"{step_name} failed with return code {result.returncode}")
            
            results[step_name] = 'success'
            logger.info(f"{step_name} completed successfully")
        
        logger.info("All DBT steps completed successfully")
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            metadata=results
        )
        
        return results
        
    except Exception as e:
        logger.exception(f"DBT transformation failed: {e}")
        raise


# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id='flight_price_pipeline',
    default_args=default_args,
    description='ETL pipeline for Bangladesh flight price analysis with Kaggle extraction',
    schedule_interval=None,
    catchup=False,
    tags=['flight', 'etl', 'bangladesh', 'kaggle'],
) as dag:
    
    task_extract = PythonOperator(
        task_id='extract_from_kaggle',
        python_callable=extract_from_kaggle,
    )
    
    task_load_csv = PythonOperator(
        task_id='load_csv_to_mysql',
        python_callable=load_csv_to_mysql,
    )
    
    task_validate = PythonOperator(
        task_id='validate_mysql_data',
        python_callable=validate_mysql_data,
    )
    
    task_transfer = PythonOperator(
        task_id='transfer_to_postgres_bronze',
        python_callable=transfer_to_postgres_bronze,
    )
    
    task_dbt = PythonOperator(
        task_id='run_dbt_transformations',
        python_callable=run_dbt_transformations,
    )
    
    task_extract >> task_load_csv >> task_validate >> task_transfer >> task_dbt
