from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from weather_etl import fetch_weather, transform_weather, save_to_db

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'weather_dag',
    default_args=default_args,
    description='Weather ETL',
    schedule_interval='@daily',
)

def run_etl():
    cities = ["İstanbul", "Ankara", "İzmir", "Antalya", "Şanlıurfa", "Samsun", "Van"]
    
    for city in cities:
        print(f"Fetching weather data for {city}...")
        weather_data = fetch_weather(city)
        df = transform_weather(weather_data)
        
        if df is not None:
            save_to_db(df)

run_etl_task = PythonOperator(
    task_id='run_weather_etl',
    python_callable=run_etl,
    dag=dag,
)
