from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json 

LATITUDE='19.076090'
LONGITUDE='72.877426'
POSTGRESS_CONN_ID='postgres-default'
API_CONN_ID='open-meteo-api'

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

with DAG(dag_id='weather_etl_pipelin',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:
    
    @task()
    def extract_weather_data():
        http_hook = HttpHook(http_conn_id=API_CONN_ID,method='GET')

        # url : https://api.open-meteo.com
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        response=http_hook.run(endpoint=endpoint)

        return response.json()
    
    @task()
    def transform_weather_data(weather_data):
        current_weather=weather_data['current_weather']
        transform_data= {
            'latitude':LATITUDE,
            'longitude':LONGITUDE,
            'temperature':current_weather['temperature'],
            'windspeed':current_weather['windspeed'],
            'winddirection':current_weather['winddirection'],
            'weathercode':current_weather['weathercode']
        }

        return transform_data
    
    @task()
    def load_weather_data(transformed_data):
        pg_hook=PostgresHook(POSTGRESS_CONN_ID)
        conn=pg_hook.get_conn()
        cursor=conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                       latitude FLOAT,
                       longitude FLOAT,
                       temperature FLOAT,
                       windspeed FLOAT,
                       winddirection FLOAT,
                       weathercode INT,
                       timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """)
        
        cursor.execute("""
                INSERT INTO weather_data VALUES (%s,%s,%s,%s,%s,%s)""", (
                    transformed_data['latitude'],
                    transformed_data['longitude'],
                    transformed_data['temperature'],
                    transformed_data['windspeed'],
                    transformed_data['winddirection'],
                    transformed_data['weathercode']
                ))

        conn.commit()
        cursor.close()


    weather_data=extract_weather_data()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data(transformed_data)