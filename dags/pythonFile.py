from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import List, Dict
import requests


# Load variables from .env file
load_dotenv()
api_key = os.getenv('myApiKey')   # Api key

api_request_conn = os.getenv('api_request_conn')  # Api connection  (url)
postgres_id = "postgres_conn"  # Postgres connection

default_args = {
    'owner': 'fabrice selemani',
    'start_date': datetime(2025, 1, 27)
}

with DAG(
        dag_id='data_from_open_weather_api',
        default_args=default_args,
        schedule='@daily',
        catchup=False
) as dag:
    # List of cities to fetch weather data for
    cities = ['fort worth', 'seattle', 'detroit', 'buffalo', 'springfield', 'san diego', 'chicago', 'denver', 'indianapolis', 'miami']
    


    @task(task_id="Extract_data_from_open_weather_api")
    def extract_data() -> List[Dict]:
        """Extract weather data for each city from the API."""
        data = []
        for city in cities:
            try:
                res = requests.get(f'{api_request_conn}{city}&appid={api_key}')
                if res.status_code == 200:
                    data.append(res.json())
                else:
                    print(f"Error fetching data for {city}: response code {res.status_code}")
            except Exception as e:
                print(f"Error fetching data for {city}: {e}")
        return data


    @task(task_id="Process_data")
    def process_data(data_of_all_cities: List[Dict]) -> List[Dict]:
        """Process the raw data and convert values where necessary."""
        processed_data_all_cities = []
        for data_of_each_city in data_of_all_cities:
            try:
                timezone_offset = data_of_each_city.get("timezone", 0)

                sunrise_time = datetime.utcfromtimestamp(data_of_each_city["sys"]["sunrise"]) + timedelta(
                    seconds=timezone_offset)
                sunset_time = datetime.utcfromtimestamp(data_of_each_city["sys"]["sunset"]) + timedelta(
                    seconds=timezone_offset)

                # Convert time objects to strings
                sunrise_time_str = sunrise_time.time().isoformat()
                sunset_time_str = sunset_time.time().isoformat()

                day_length_timedelta = sunset_time - sunrise_time
                total_seconds = day_length_timedelta.total_seconds()
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                day_length_str = f"{int(hours)} hours, {int(minutes)} minutes"

                weekday_int = sunrise_time.weekday()
                weekday_str = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][
                    weekday_int]

                data_ = {
                    "city_id": data_of_each_city["id"],
                    "cities": data_of_each_city["name"],
                    "countries": data_of_each_city["sys"]["country"],
                    "longitudes": data_of_each_city["coord"]["lon"],
                    "latitudes": data_of_each_city["coord"]["lat"],
                    "weather_mains": data_of_each_city["weather"][0]["main"],
                    "descriptions": data_of_each_city["weather"][0]["description"],
                    "temperatures": round(data_of_each_city["main"]["temp"] - 273.15),
                    "wind_speeds": data_of_each_city["wind"]["speed"],
                    "wind_degrees": data_of_each_city["wind"]["deg"],
                    "temperature_maximums": round(data_of_each_city["main"]["temp_max"] - 273.15),
                    "temperature_minimums": round(data_of_each_city["main"]["temp_min"] - 273.15),
                    "feels_likes": round(data_of_each_city["main"]["feels_like"] - 273.15),
                    "humidity_list": data_of_each_city["main"]["humidity"],
                    "time_zones": timezone_offset,
                    "dates": str(sunrise_time.date()),
                    "sunrise_times": sunrise_time_str,  
                    "sunset_times": sunset_time_str,  
                    "day_lengths": day_length_str,
                    "week_days": weekday_str
                }
                processed_data_all_cities.append(data_)
            except KeyError as ke:
                print(f"Key error while processing data: {ke}")
            except Exception as e:
                print(f"Unexpected error while processing data: {e}")
        return processed_data_all_cities


    @task(task_id="Load_cleaned_data")
    def load_data(record: Dict):
        """Load a single processed record into the Postgres database."""
        postgres_hook = PostgresHook(postgres_conn_id=postgres_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Create the first table to store the historical data info
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_weather_info (
                id SERIAL PRIMARY KEY,       
                city_id INT ,
                cities VARCHAR(50),
                countries VARCHAR(250),
                longitudes DOUBLE PRECISION,
                latitudes DOUBLE PRECISION,
                weather_mains VARCHAR(250),
                descriptions VARCHAR(250),
                temperatures INT,
                wind_speeds DOUBLE PRECISION,
                wind_degrees INT,
                temperature_maximums INT,
                temperature_minimums INT,
                feels_likes INT,
                humidity_list INT,
                time_zones INT,
                dates DATE,
                sunrise_times TIME WITHOUT TIME ZONE,
                sunset_times TIME WITHOUT TIME ZONE,
                day_lengths VARCHAR(250),
                week_days VARCHAR(250),
                       UNIQUE (city_id, dates)             
            )
        """)


        # Create the second table to store the current data info 
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS current_weather_info (
                city_id INT PRIMARY KEY,
                cities VARCHAR(50),
                countries VARCHAR(250),
                longitudes DOUBLE PRECISION,
                latitudes DOUBLE PRECISION,
                weather_mains VARCHAR(250),
                descriptions VARCHAR(250),
                temperatures INT,
                wind_speeds DOUBLE PRECISION,
                wind_degrees INT,
                temperature_maximums INT,
                temperature_minimums INT,
                feels_likes INT,
                humidity_list INT,
                time_zones INT,
                dates DATE,
                sunrise_times TIME WITHOUT TIME ZONE,
                sunset_times TIME WITHOUT TIME ZONE,
                day_lengths VARCHAR(250),
                week_days VARCHAR(250),
                       UNIQUE (city_id)
            )
        """)

        

        # Insert data into the historical data infos table   
        cursor.execute("""INSERT INTO historical_weather_info (city_id, cities, countries, longitudes, latitudes, weather_mains, descriptions, temperatures, wind_speeds, wind_degrees, temperature_maximums, temperature_minimums, feels_likes, humidity_list, time_zones, dates, sunrise_times, sunset_times, day_lengths, week_days) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (city_id, dates) DO NOTHING;"""
        , (
            record["city_id"],
            record["cities"],
            record["countries"],
            record["longitudes"],
            record["latitudes"],
            record["weather_mains"],
            record["descriptions"],
            record["temperatures"],
            record["wind_speeds"],
            record["wind_degrees"],
            record["temperature_maximums"],
            record["temperature_minimums"],
            record["feels_likes"],
            record["humidity_list"],
            record["time_zones"],
            record["dates"],
            record["sunrise_times"],
            record["sunset_times"],
            record["day_lengths"],
            record["week_days"]
        ))



        # Insert data into the current data infos table
        cursor.execute("""
    INSERT INTO current_weather_info (
        city_id, cities, countries, longitudes, latitudes, weather_mains, descriptions, temperatures, 
        wind_speeds, wind_degrees, temperature_maximums, temperature_minimums, feels_likes, 
        humidity_list, time_zones, dates, sunrise_times, sunset_times, day_lengths, week_days
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (city_id) DO UPDATE SET 
        weather_mains = EXCLUDED.weather_mains,
        descriptions = EXCLUDED.descriptions,
        temperatures = EXCLUDED.temperatures,
        wind_speeds = EXCLUDED.wind_speeds,
        wind_degrees = EXCLUDED.wind_degrees,
        temperature_maximums = EXCLUDED.temperature_maximums,
        temperature_minimums = EXCLUDED.temperature_minimums,
        feels_likes = EXCLUDED.feels_likes,
        humidity_list = EXCLUDED.humidity_list,
        dates = EXCLUDED.dates,
        sunrise_times = EXCLUDED.sunrise_times,
        sunset_times = EXCLUDED.sunset_times,
        day_lengths = EXCLUDED.day_lengths,
        week_days = EXCLUDED.week_days
""", (
    record["city_id"], record["cities"], record["countries"], record["longitudes"], record["latitudes"],
    record["weather_mains"], record["descriptions"], record["temperatures"], record["wind_speeds"],
    record["wind_degrees"], record["temperature_maximums"], record["temperature_minimums"], record["feels_likes"],
    record["humidity_list"], record["time_zones"], record["dates"], record["sunrise_times"],
    record["sunset_times"], record["day_lengths"], record["week_days"]
))


        conn.commit()
        cursor.close()
        conn.close()

          

    # Define the task pipeline:
    raw_data = extract_data()
    processed_data_task = process_data(raw_data)

    # pass the task result directly if it is already a list.
    load_data.expand(record=processed_data_task)