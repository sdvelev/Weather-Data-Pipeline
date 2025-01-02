import psycopg2
import os

from dotenv import load_dotenv
from prefect import task

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_host = os.getenv("DB_HOST")


@task(retries=2, retry_delay_seconds=10, timeout_seconds=60, log_prints=True)
def task_load_daily_weather_analysis_if_necessary(daily_weather_analysis_to_insert: dict):
    stringified_daily_weather_analysis_to_insert = {key: str(value) for key, value in
                                                    daily_weather_analysis_to_insert.items()}
    with psycopg2.connect(user=db_user, password=db_password,
                          host=db_host, dbname=db_name) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO daily_weather_analyses (
                    city_id,
                    date,
                    max_temp_c,
                    min_temp_c,
                    avg_temp_c,
                    max_wind_speed_kph,
                    max_wind_speed_mps,
                    avg_wind_speed_kph,
                    avg_wind_speed_mps,
                    total_precip_mm,
                    avg_humidity_perc,
                    sunrise,
                    sunset,
                    moonrise,
                    moonset,
                    moon_phase
                )
                VALUES(CAST(%(city_id)s AS INT), CAST(%(date)s AS DATE), CAST(%(max_temp_c)s AS FLOAT), 
                CAST(%(min_temp_c)s AS FLOAT), CAST(%(avg_temp_c)s AS FLOAT), CAST(%(max_wind_speed_kph)s AS FLOAT),
                CAST(%(max_wind_speed_mps)s AS FLOAT), CAST(%(avg_wind_speed_kph)s AS FLOAT),
                CAST(%(avg_wind_speed_mps)s AS FLOAT), CAST(%(total_precip_mm)s AS FLOAT),
                CAST(%(avg_humidity_perc)s AS INT), CAST(%(sunrise)s AS TIME), CAST(%(sunset)s AS TIME),
                CAST(%(moonrise)s AS TIME), CAST(%(moonset)s AS TIME), %(moon_phase)s)
                ON CONFLICT ON CONSTRAINT daily_weather_unique_constraint
                DO UPDATE SET date=EXCLUDED.date
                RETURNING id
                """, stringified_daily_weather_analysis_to_insert
            )
            result_index = cursor.fetchone()
            if result_index is None:
                return None
            cursor.execute("SELECT setval('daily_weather_analyses_id_seq', (SELECT MAX(id) FROM daily_weather_analyses))")
            return result_index[0]