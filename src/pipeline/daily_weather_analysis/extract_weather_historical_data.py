import httpx
import psycopg2
import os

from datetime import datetime, timedelta
from dotenv import load_dotenv
from prefect import get_run_logger
from prefect import task
from zoneinfo import ZoneInfo

load_dotenv()

base_url = "https://api.weatherapi.com"
path_url_realtime_api = "/v1/current.json"
path_url_history_api = "/v1/history.json"
api_key = os.getenv("WEATHER_API_KEY")

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_host = os.getenv("DB_HOST")


@task(retries=2, retry_delay_seconds=3, timeout_seconds=10, log_prints=True)
def task_generate_historical_data_url(city: str, previous_date: str):
    return f"{base_url}{path_url_history_api}?key={api_key}&q={city}&dt={previous_date}"

@task(retries=2, retry_delay_seconds=10, timeout_seconds=20, log_prints=True)
def task_extract_weather_historical_data(url: str):
    logger = get_run_logger()
    response = httpx.get(url)
    try:
        response.raise_for_status()
    except Exception as e:
        logger.exception(f"Could not retrieve weather historical data with url: {url}")
        raise e

    return response.json()

@task(retries=2, retry_delay_seconds=2, timeout_seconds=10, log_prints=True)
def task_extract_astro_data(astro_data: dict):
    return {'sunrise': astro_data['sunrise'], 'sunset': astro_data['sunset'], 'moonrise': astro_data['moonrise'],
                  'moonset': astro_data['moonset'], 'moon_phase': astro_data['moon_phase']}

@task(retries=2, retry_delay_seconds=10, timeout_seconds=60)
def task_extract_city_id(location_data: dict):
    with psycopg2.connect(user=db_user, password=db_password,
                          host=db_host, dbname=db_name) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id 
                FROM city 
                WHERE name=%(name)s AND region=%(region)s AND country=%(country)s
                """, {"name": location_data['name'], "region": location_data['region'],
                      "country": location_data['country']}
            )
            result = cursor.fetchone()
            return result[0] if result else None

@task(retries=2, retry_delay_seconds=2, timeout_seconds=10)
def task_extract_previous_date(time_zone: str):
    date_obj = datetime.now(ZoneInfo(time_zone))
    previous_day = date_obj - timedelta(days=1)
    return previous_day.date()

@task(retries=2, retry_delay_seconds=10, timeout_seconds=60)
def task_extract_weather_record(city_id: str, previous_date: str):
    with psycopg2.connect(user=db_user, password=db_password,
                          host=db_host, dbname=db_name) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT * 
                FROM current_weather 
                WHERE city_id=%(city_id)s AND date=%(date)s
                """, {"city_id": city_id, "date": previous_date}
            )
            return cursor.fetchall()