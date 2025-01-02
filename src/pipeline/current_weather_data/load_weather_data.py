import psycopg2
import os

from dotenv import load_dotenv
from prefect import task
from prefect.runtime import task_run

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_host = os.getenv("DB_HOST")


def generate_city_task_run_name():
    flow_name = task_run.task_name
    city_data_to_insert = task_run.parameters['city_data_to_insert']
    return (f"{flow_name}-for-{city_data_to_insert['name'].replace(' ', '-')}-"
            f"{city_data_to_insert['country'].replace(' ', '-')}")

@task(task_run_name=generate_city_task_run_name, retries=2, retry_delay_seconds=10, timeout_seconds=60, log_prints=True)
def task_load_city_data_if_necessary(city_data_to_insert: dict):
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
                ON CONFLICT ON CONSTRAINT city_unique_constraint
                DO UPDATE SET name=EXCLUDED.name
                RETURNING id
                """, city_data_to_insert
            )
            result_index = cursor.fetchone()[0]
            cursor.execute("SELECT setval('city_id_seq', (SELECT MAX(id) FROM city))")
            return result_index

@task(retries=2, retry_delay_seconds=10, timeout_seconds=60, log_prints=True)
def task_load_weather_data_if_necessary(weather_data_to_insert: dict, city_id: str):
    weather_data_to_insert['city_id'] = city_id
    with psycopg2.connect(user=db_user, password=db_password,
                          host=db_host, dbname=db_name) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO current_weather (
                    city_id,
                    date,
                    time,
                    temp_c,
                    feels_like_c,
                    weather_condition_code,
                    weather_condition_text,
                    weather_condition_icon,
                    wind_speed_kph,
                    wind_speed_mps,
                    wind_dir,
                    pressure_mb,
                    precip_mm,
                    humidity_perc,
                    cloud_perc,
                    uv_index
                )
                VALUES(CAST(%(city_id)s AS INT), CAST(%(date)s AS DATE), CAST(%(time)s AS TIME), CAST(%(temp_c)s AS FLOAT), 
                CAST(%(feels_like_c)s AS FLOAT), CAST(%(weather_condition_code)s AS INT), %(weather_condition_text)s,
                %(weather_condition_icon)s, CAST(%(wind_speed_kph)s AS FLOAT), CAST(%(wind_speed_mps)s AS FLOAT),
                %(wind_dir)s, CAST(%(pressure_mb)s AS FLOAT), CAST(%(precip_mm)s AS FLOAT),
                CAST(%(humidity_perc)s AS INT), CAST(%(cloud_perc)s AS INT), CAST(%(uv_index)s AS FLOAT))
                ON CONFLICT ON CONSTRAINT weather_unique_constraint
                DO UPDATE SET date=EXCLUDED.date
                RETURNING id
                """, weather_data_to_insert
            )
            result_index = cursor.fetchone()[0]
            cursor.execute("SELECT setval('current_weather_id_seq', (SELECT MAX(id) FROM current_weather))")
            return result_index