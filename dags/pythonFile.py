from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook                     
from airflow.decorators import task
from datetime import datetime 
from airflow.providers.postgres.hooks.postgres import PostgresHook  
import os
from dotenv import load_dotenv


# api connection
api_request_id = "api_request_conn"

# final destination (postgres database) connection
postgres_id = "postgres_conn"


default_args = {
    'owner': 'fabrice selemani',
    'start_date': datetime(2025, 1, 27)  
}


# dag
with DAG(
    dag_id='data_from_open_weather_api',
    default_args=default_args,
    schedule='@daily',
        catchup=False) as dag:
    ids = []
    longitudes = []
    latitudes = []
    weather_mains = []
    descriptions = []
    temperatures = []
    wind_speeds = []
    wind_degrees = []
    temperature_maximums = []
    temperature_minimums = []
    dates = []
    sunrise_times = []
    sunset_times = []
    feels_likes = []
    time_zones = []
    humidity_list = []
    countries = []
    week_days = []
    day_lengths = []



    @task(task_id="Extract_data_from_open_weather_api") 
    def extract_data():
        # retrieve the URL from Airflow connection settings
        http_hook = HttpHook(method='GET', http_conn_id=api_request_id)
        
        # list of cities to retrieve
        cities = ['paris', 'london', 'dallas']

        # Load variables from .env file
        load_dotenv()
        api_key = os.getenv('myApiKey')

        for city in cities:
            try:

                end_point = f'{city}&appid={api_key}'
                response = http_hook.run(end_point)

            except Exception as e:
                print(e)
            
            else:
                if response.status_code == 200:
                    return response.json()
                else:
                    print("no data extracted ")



    data = extract_data()
    print(data)
