from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ingestion_stock import ingest_stock_data
from ingestion_weather import ingest_weather_data
from ingestion_mobility import ingest_mobility_data

from transform_stock import transform_stock_data
from transform_weather import transform_weather_data
from transform_mobility import transform_mobility_data

from load_mongo import load_processed_data

default_args = {
    'owner': 'ivan',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 20),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'main_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline: Stock + Weather + Mobility to MongoDB + Streamlit',
    schedule_interval='@daily',
    catchup=False,
    tags=['massive-data'],
) as dag:

    # INGESTION
    ingest_stock = PythonOperator(
        task_id='ingest_stock_data',
        python_callable=ingest_stock_data,
    )

    ingest_weather = PythonOperator(
        task_id='ingest_weather_data',
        python_callable=ingest_weather_data,
    )

    ingest_mobility = PythonOperator(
        task_id='ingest_mobility_data',
        python_callable=ingest_mobility_data,
    )

    # TRANSFORMATION
    transform_stock = PythonOperator(
        task_id='transform_stock_data',
        python_callable=transform_stock_data,
    )

    transform_weather = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data,
    )

    transform_mobility = PythonOperator(
        task_id='transform_mobility_data',
        python_callable=transform_mobility_data,
    )

    # LOAD
    load_data = PythonOperator(
        task_id='load_processed_data',
        python_callable=load_processed_data,
    )

    # FLOW
    [ingest_stock >> transform_stock,
     ingest_weather >> transform_weather,
     ingest_mobility >> transform_mobility]

    [transform_stock, transform_weather, transform_mobility] >> load_data
