import httpx
from prefect import get_run_logger
from prefect import task

base_url = "https://api.weatherapi.com"
path_url_realtime_api = "/v1/current.json"
path_url_history_api = "/v1/history.json"
api_key = ""

@task
def task_generate_url(city: str):
    return f"{base_url}{path_url_realtime_api}?key={api_key}&q={city}"

@task
def task_extract_current_weather_data(url: str):
    logger = get_run_logger()
    response = httpx.get(url)
    try:
        response.raise_for_status()
    except Exception as e:
        logger.exception(f"Could not retrieve current weather data with url: {url}")
        raise e

    weather_data = response.json()
    return weather_data