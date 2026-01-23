"""
Pipeline logging utilities.
Tracks task execution in the audit.pipeline_runs table.
"""

from datetime import datetime
import json
from typing import Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook


def log_pipeline_event(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    status: str,
    rows_processed: Optional[int] = None,
    rows_failed: Optional[int] = None,
    error_message: Optional[str] = None,
    metadata: Optional[dict] = None,
    postgres_conn_id: str = 'postgres_analytics'
) -> None:
    """
    Log a pipeline event to the audit table.
    
    Args:
        dag_id: Name of the DAG
        dag_run_id: Unique identifier for this DAG run
        task_id: Name of the task
        status: One of 'started', 'completed', 'failed'
        rows_processed: Number of rows successfully processed
        rows_failed: Number of rows that failed
        error_message: Error details if status is 'failed'
        metadata: Additional information as dictionary
        postgres_conn_id: Airflow connection ID for PostgreSQL
    
    Example:
        log_pipeline_event(
            dag_id='flight_pipeline',
            dag_run_id='manual__2024-01-15',
            task_id='load_csv',
            status='completed',
            rows_processed=57000
        )
    """
    
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    now = datetime.now()
    
    insert_sql = """
        INSERT INTO audit.pipeline_runs 
        (dag_id, dag_run_id, task_id, status, started_at, completed_at, 
         rows_processed, rows_failed, error_message, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Set timestamps based on status
    started_at = now if status == 'started' else None
    completed_at = now if status in ('completed', 'failed') else None
    
    hook.run(
        insert_sql,
        parameters=(
            dag_id,
            dag_run_id,
            task_id,
            status,
            started_at,
            completed_at,
            rows_processed,
            rows_failed,
            error_message,
            json.dumps(metadata) if metadata else None
        )
    )


def get_task_context(context: dict) -> tuple:
    """
    Extract common identifiers from Airflow task context.
    
    Args:
        context: The **context passed to a PythonOperator callable
    
    Returns:
        Tuple of (dag_id, dag_run_id, task_id)
    
    Example:
        def my_task(**context):
            dag_id, dag_run_id, task_id = get_task_context(context)
    """
    
    dag_id = context['dag'].dag_id
    dag_run_id = context['run_id']
    task_id = context['task'].task_id
    
    return dag_id, dag_run_id, task_id
