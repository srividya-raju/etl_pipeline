import json
import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import openmeteo_requests
import requests_cache
from retry_requests import retry
from airflow.decorators import dag, task

# -------------------------------
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


#DB_CONN = {
 #  "host": "db-postgres",
  #  "dbname": "mydb",
   # "user": "postgres",
    #"password": "postgres",
    #"port": 5432
#}





# task 2: get values from api for above outlets (Example code snippet from openmeteo) reused as is.
def fetch_hourly_weather(lat, lon):
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "2023-01-01",
        "end_date": "2024-12-31",
        "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    hourly = response.Hourly()

    # Assign variables in correct requested order
    temp = hourly.Variables(0).ValuesAsNumpy()
    hum = hourly.Variables(1).ValuesAsNumpy()
    wind = hourly.Variables(2).ValuesAsNumpy()

    # Time index
    time_index = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )

    df = pd.DataFrame({
        "timestamp": time_index,
        "temperature_2m": temp,
        "relative_humidity_2m": hum,
        "wind_speed_10m": wind,
    })

    return df

# task 1: get outlet data with latitude and longitude


    
    # Push result to XCom correctly
    #context["ti"].xcom_push(key="weather_rows", value=final_weather_df)





# ------------------------------
# DAG Definition
# ------------------------------

@dag(
    dag_id="fetch_weather_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    description="Fetch hourly weather data and load into dbttest.api_data",
)

def fetch_weather_dag():
    @task
    def get_outlets():
        
        hook = PostgresHook(postgres_conn_id="postgres_dbt")
        df = hook.get_pandas_df("SELECT * FROM dbttest.int_outlet;")
        
        return df

    @task
    def fetch_weather(df):
        all_data = []
        outlets = pd.DataFrame(df)

        for _, row in outlets.iterrows():
            outlet_id = row["outlet_id"]
            lat = row["latitude"]
            lon = row["longitude"]

            print(f"Fetching weather for outlet {outlet_id}: ({lat}, {lon})")

            df1 = fetch_hourly_weather(lat, lon)
            df1["outlet_id"] = outlet_id  # add location identifier
            all_data.append(df1)
        final_weather_df = pd.concat(all_data, ignore_index=True)

        hook = PostgresHook(postgres_conn_id="postgres_dbt")
        engine = hook.get_sqlalchemy_engine()

        final_weather_df.to_sql(
            name="api_data",
            schema="dbttest", # ideally should be added to datalae, but added to dbttest with intrmediate files
            con=engine,
            if_exists="append",
            index=False,
        )

    outlets_df=get_outlets()
    fetch_weather(outlets_df)

dag = fetch_weather_dag()
