"""
Test - Extraction + Validation + Transformation + Fraud Analysis
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from include.customer_risk_platform.extractors import extract_sales_data
from include.customer_risk_platform.validators import validate_data_quality
from include.customer_risk_platform.transformers import transform_clean_data
from include.customer_risk_platform.fraud_analyzers import enrich_fraud_indicators

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}

dag = DAG(
    'test_pipeline_4_modules',
    default_args=DEFAULT_ARGS,
    description='Customer risk platform - full ELT pipeline',
    schedule_interval=None,
    tags=['testing'],
    max_active_runs=1,
)

start = DummyOperator(task_id='start', dag=dag)

extract_data = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_sales_data,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_clean_data',
    python_callable=transform_clean_data,
    dag=dag,
)

validate_quality = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

dbt_staging  = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt && dbt run --select staging',
    dag=dag,
)

analyze_fraud = PythonOperator(
    task_id='enrich_fraud_indicators',
    python_callable=enrich_fraud_indicators,
    dag=dag,
)

dbt_marts = BashOperator(
    task_id='dbt_marts',
    bash_command='cd /opt/airflow/dbt && dbt run --select marts',
    dag=dag,
)

dbt_test_staging = BashOperator(
    task_id='dbt_test_staging',
    bash_command='cd /opt/airflow/dbt && dbt test --select staging',
    retries=0,
    dag=dag,
)

dbt_test_marts = BashOperator(
    task_id='dbt_test_marts',
    bash_command='cd /opt/airflow/dbt && dbt test --select marts',
    retries=0,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >> extract_data >> transform_data >> validate_quality >> dbt_staging  >> dbt_test_staging >> analyze_fraud >> dbt_marts >> dbt_test_marts >> end