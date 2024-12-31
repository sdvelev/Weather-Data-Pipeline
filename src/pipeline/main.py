import datetime
import pytz

from prefect import flow, task, get_run_logger
from prefect.runtime import flow_run

from extract_weather_data import task_generate_url, task_extract_current_weather_data
from load_weather_data import task_load_city_data_if_necessary, task_load_weather_data_if_necessary
from transform_weather_data import (task_fill_direct_city_fields, task_fill_direct_weather_fields,
                                    task_transform_date_time_fields, task_transform_wind_speed_mps,
                                    task_transform_wind_dir)


def generate_extract_weather_flow_run_name():
    flow_name = flow_run.flow_name
    city = flow_run.parameters['city']
    current_datetime = datetime.datetime.now()
    formatted_date = current_datetime.strftime("%Y-%m-%d-in-%H:%M:%S")
    return f"{flow_name}-for-{city.replace(' ', '-')}-on-{formatted_date}"

def generate_transform_weather_data_flow_run_name():
    flow_name = flow_run.flow_name
    weather_data = flow_run.parameters['weather_data']

    last_updated_time = weather_data['current']['last_updated']
    input_timezone = weather_data['location']['tz_id']

    naive_datetime = datetime.datetime.strptime(last_updated_time, "%Y-%m-%d %H:%M")
    input_tz = pytz.timezone(input_timezone)
    localized_datetime = input_tz.localize(naive_datetime)

    output_tz = pytz.timezone('Europe/Sofia')
    converted_datetime = localized_datetime.astimezone(output_tz)
    return (f"{flow_name}-for-{weather_data['location']['name'].replace(' ', '-')}"
            f"-last-updated-{str(converted_datetime.strftime('%Y-%m-%d-in-%H:%M:%S')).replace(' ', '-')}")

def generate_load_weather_data_flow_run_name():
    flow_name = flow_run.flow_name
    city_data_to_insert = flow_run.parameters['city_data_to_insert']
    weather_data_to_insert = flow_run.parameters['weather_data_to_insert']

    last_updated_time = f"{weather_data_to_insert['date']} {weather_data_to_insert['time']}"
    input_timezone = city_data_to_insert['time_zone']

    naive_datetime = datetime.datetime.strptime(last_updated_time, "%Y-%m-%d %H:%M")
    input_tz = pytz.timezone(input_timezone)
    localized_datetime = input_tz.localize(naive_datetime)

    output_tz = pytz.timezone('Europe/Sofia')
    converted_datetime = localized_datetime.astimezone(output_tz)
    return (f"{flow_name}-for-{city_data_to_insert['name'].replace(' ', '-')}"
            f"-last-updated-{str(converted_datetime.strftime('%Y-%m-%d-in-%H:%M:%S')).replace(' ', '-')}")

@flow(flow_run_name=generate_extract_weather_flow_run_name, log_prints=True)
def flow_extract_weather_data(city: str = "Sofia"):
    url = task_generate_url(city)
    weather_data = task_extract_current_weather_data(url)
    return weather_data

@flow(flow_run_name=generate_transform_weather_data_flow_run_name, log_prints=True)
def flow_transform_weather_data(weather_data: dict):
    city_data_to_insert = {}
    task_fill_direct_city_fields(weather_data, city_data_to_insert)

    weather_data_to_insert = {}
    task_fill_direct_weather_fields(weather_data, weather_data_to_insert)
    task_transform_date_time_fields(weather_data, weather_data_to_insert)
    task_transform_wind_speed_mps(weather_data, weather_data_to_insert)
    task_transform_wind_dir(weather_data, weather_data_to_insert)

    return city_data_to_insert, weather_data_to_insert


@flow(flow_run_name=generate_load_weather_data_flow_run_name, log_prints=True)
def flow_load_weather_data(city_data_to_insert: dict, weather_data_to_insert: dict):
    city_id = task_load_city_data_if_necessary(city_data_to_insert)
    load_status = task_load_weather_data_if_necessary(weather_data_to_insert, city_id)


@flow(log_prints=True)
def weather_data_pipeline():
    weather_data = flow_extract_weather_data()
    print(weather_data)
    city_data_to_insert, weather_data_to_insert = flow_transform_weather_data(weather_data)
    print(city_data_to_insert)
    print(weather_data_to_insert)
    flow_load_weather_data(city_data_to_insert, weather_data_to_insert)

def main():
    weather_data_pipeline()

if __name__ == "__main__":
    main()