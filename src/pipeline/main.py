import datetime

import httpx
import psycopg2

from prefect import flow, task, get_run_logger
from prefect.runtime import flow_run

from extract_weather_data import task_generate_url, task_extract_current_weather_data

base_url = "https://api.weatherapi.com"
path_url_realtime_api = "/v1/current.json"
path_url_history_api = "/v1/history.json"
api_key = "f0c69355d9a1440dbe6200909240412"

db_user = ""
db_password = ""
db_name = ""
db_host = ""

def generate_city_flow_run_name():
    flow_name = flow_run.flow_name

    location_data = flow_run.parameters['location_data']

    return f"{flow_name}-for-{location_data['name']}-{location_data['country']}"

def generate_current_weather_flow_run_name():
    flow_name = flow_run.flow_name
    city = flow_run.parameters['city']
    current_datetime = datetime.datetime.now()
    formatted_date = current_datetime.strftime("%Y-%m-%d-in-%H%M:%S")
    return f"{flow_name}-for-{city}-on-{formatted_date}"

@flow(flow_run_name=generate_current_weather_flow_run_name, log_prints=True)
def flow_extract_current_weather_data(city: str = "New York"):
    url = task_generate_url(city)
    weather_data = task_extract_current_weather_data(url)
    return weather_data

# @flow(flow_run_name=generate_current_weather_flow_run_name, log_prints=True)
# def flow_extract_current_weather_data(city: str = "Berlin"):
#     logger = get_run_logger()
#     url = f"{base_url}{path_url_realtime_api}?key={api_key}&q={city}"
#
#     response = httpx.get(url)
#     try:
#         response.raise_for_status()
#     except Exception as e:
#         logger.exception("Could not retrieve current weather data")
#         raise e
#
#     weather_data = response.json()
#     return weather_data

@flow(flow_run_name=generate_city_flow_run_name, log_prints=True)
def flow_load_city_data_if_necessary(location_data: dict):
    with psycopg2.connect(user=db_user, password=db_password,
                          host=db_host, dbname=db_name) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO city (
                    name,
                    region,
                    country,
                    time_zone,
                    latitude,
                    longitude
                )
                VALUES(%(name)s, %(region)s, %(country)s, %(time_zone)s, 
                CAST(%(latitude)s AS FLOAT), CAST(%(longitude)s AS FLOAT))
                ON CONFLICT ON CONSTRAINT city_unique_constraint DO NOTHING
                """,
                {'name': location_data['name'], 'region': location_data['region'],
                 'country': location_data['country'], 'time_zone': location_data['tz_id'],
                 'latitude': location_data['lat'], 'longitude': location_data['lon']}
            )


def transform_data(weather_data: dict):
    pass


@flow(log_prints=True)
def weather_data_pipeline():
    weather_data = flow_extract_current_weather_data()
    print(weather_data)
    flow_load_city_data_if_necessary(weather_data['location'])

def main():
    weather_data_pipeline()

if __name__ == "__main__":
    main()