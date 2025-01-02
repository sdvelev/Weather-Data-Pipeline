import httpx
import os

from dotenv import load_dotenv
from prefect import get_run_logger
from prefect import task

load_dotenv()

api_key = os.getenv("WEATHER_API_KEY")

base_url = "https://api.weatherapi.com"
path_url_realtime_api = "/v1/current.json"
path_url_history_api = "/v1/history.json"


@task(retries=2, retry_delay_seconds=3, timeout_seconds=10, log_prints=True)
def task_generate_url(city: str):
    return f"{base_url}{path_url_realtime_api}?key={api_key}&q={city}"

@task(retries=2, retry_delay_seconds=10, timeout_seconds=20, log_prints=True)
def task_extract_current_weather_data(url: str):
    logger = get_run_logger()
    response = httpx.get(url)
    try:
        response.raise_for_status()
    except Exception as e:
        logger.exception(f"Could not retrieve current weather data with url: {url}")
        raise e

    return response.json()