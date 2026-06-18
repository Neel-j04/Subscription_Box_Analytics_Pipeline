from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl_pipeline import extract, transform, load

def extract_task():
    return extract()

def transform_task(ti):
    records = ti.xcom_pull(task_ids="extract_data")
    transformed_df = transform(records)

    transformed_df.to_json(
        "temp_transformed.json",
        orient="records"
    )

def load_task():
    import pandas as pd

    df = pd.read_json(
        "temp_transformed.json"
    )
    load(df)

with DAG(
    dag_id="subscription_box_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1)
    }
) as dag:
    
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_task
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_task
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_task
    )

    extract_data >> transform_data >> load_data