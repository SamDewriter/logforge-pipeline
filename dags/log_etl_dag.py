from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from utils import extract, transform, load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow() - timedelta(days=2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='log_etl_dag',
    default_args=default_args,
    # The schedule should be 12:30
    schedule = '30 12 * * *',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        op_kwargs={'log_source': f"api_output/new_data_{datetime.utcnow().strftime('%Y%m%d')}.json"},
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load',
        op_kwargs={'db_file': 'db/logs.db', 
                   'parsed_entries': '{{ task_instance.xcom_pull(task_ids="transform") }}',
                   'error_entries': '{{ task_instance.xcom_pull(task_ids="transform", key="error") }}'},
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
