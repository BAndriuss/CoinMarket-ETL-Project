from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from main import extract
from main import transform
from main import load_to_gbq

default_arguments = {
    'owner':'airflow',
    'depends_on_past':True,
    'email':['airflow@example.com'],
    'start_date':datetime(2023, 3, 18),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retries_delay':timedelta(minutes=10),
}

dag=DAG(
    'Coinbase_Data_ETL',
    default_args=default_arguments,
    description='Full ETL Dag',
    catchup=False,
    schedule_interval=None
)

run_extract = PythonOperator(
    task_id = 'extract_data_from_coinbase',
    python_callable=extract,
    dag=dag,
   
)

run_transform = PythonOperator(
    task_id = 'transform_extracted_data',
    python_callable=transform,
    dag=dag,
)

run_load = PythonOperator(
    task_id = 'load_transformed_data_to_gbq',
    python_callable=load_to_gbq,
    dag=dag,
)

run_extract >> run_transform >> run_load

