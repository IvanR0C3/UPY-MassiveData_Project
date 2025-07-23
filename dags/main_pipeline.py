from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    'owner': 'ivan',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 20),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'main_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline: StockData + Weather + Mobility -> MongoDB + Streamlit',
    schedule_interval='@daily',
    catchup=False,
    tags=['massive-data'],
) as dag:

    from ingestion_stock import ingest_stock_data
    from ingestion_weather import ingest_weather_data
    from ingestion_mobility import ingest_mobility_data
    from transform_stock import transform_stock_data
    from load_mongo import load_processed_data

    task_ingest_stock = PythonOperator(
        task_id='ingest_stock_data',
        python_callable=ingest_stock_data
    )

    task_ingest_weather = PythonOperator(
        task_id='ingest_weather_data',
        python_callable=ingest_weather_data
    )

    task_ingest_mobility = PythonOperator(
        task_id='ingest_mobility_data',
        python_callable=ingest_mobility_data
    )

    task_transform = PythonOperator(
        task_id='transform_stock_data',
        python_callable=transform_stock_data
    )

    task_load = PythonOperator(
        task_id='load_processed_data',
        python_callable=load_processed_data
    )

    # Define task pipeline
    [task_ingest_stock, task_ingest_weather, task_ingest_mobility] >> task_transform >> task_load
