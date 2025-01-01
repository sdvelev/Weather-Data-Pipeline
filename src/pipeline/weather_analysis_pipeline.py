import datetime
import pytz

from prefect import flow, task, get_run_logger, serve
from prefect.client.schemas.schedules import CronSchedule
from prefect.runtime import flow_run

from extract_weather_data import task_generate_url, task_extract_current_weather_data
from extract_weather_historical_data import (task_extract_previous_date, task_extract_city_id,
                                             task_extract_weather_record, task_generate_historical_data_url,
                                             task_extract_astro_data)
from load_weather_data import task_load_city_data_if_necessary, task_load_weather_data_if_necessary
from transform_weather_data import (task_fill_direct_city_fields, task_fill_direct_weather_fields,
                                    task_transform_date_time_fields, task_transform_wind_speed_mps,
                                    task_transform_wind_dir)
from transform_weather_historical_data import (task_transform_to_pd_df, task_fill_direct_weather_analysis_fields,
                                               task_find_temp_c, task_find_max_wind_speed, task_find_avg_wind_speed,
                                               task_find_total_precip_mm, task_find_avg_humidity_perc,
                                               task_transform_astro_fields)

from load_weather_historical_data import task_load_daily_weather_analysis_if_necessary

def generate_historical_weather_flow_run_name():
    flow_name = flow_run.flow_name
    city = flow_run.parameters['city']
    return f"{flow_name}-for-{city.replace(' ', '-')}"

def generate_extract_weather_historical_data_flow_run_name():
    flow_name = flow_run.flow_name
    city = flow_run.parameters['city']
    current_datetime = datetime.datetime.now()
    formatted_date = current_datetime.strftime("%Y-%m-%d-in-%H:%M:%S")
    return f"{flow_name}-for-{city.replace(' ', '-')}-on-{formatted_date}"

def generate_transform_weather_historical_data_flow_run_name():
    flow_name = flow_run.flow_name
    weather_data_list = flow_run.parameters['weather_data_list']
    city = flow_run.parameters['city']

    return f"{flow_name}-for-{city.replace(' ', '-')}-on-{weather_data_list[0][2]}"

def generate_load_weather_historical_data_flow_run_name():
    flow_name = flow_run.flow_name
    city = flow_run.parameters['city']
    daily_weather_analysis_to_insert = flow_run.parameters['daily_weather_analysis_to_insert']

    return f"{flow_name}-for-{city.replace(' ', '-')}-on-{daily_weather_analysis_to_insert['date']}"

@flow(flow_run_name=generate_extract_weather_historical_data_flow_run_name, log_prints=True)
def flow_extract_weather_historical_data(city: str, time_zone: str):
    previous_date = task_extract_previous_date(time_zone)
    url = task_generate_historical_data_url(city, previous_date)
    weather_data = task_extract_current_weather_data(url)
    astro_dict = task_extract_astro_data(weather_data["forecast"]["forecastday"][0]["astro"])
    city_id = task_extract_city_id(weather_data["location"])
    if city_id is None:
        return None, None

    weather_data_list = task_extract_weather_record(city_id, previous_date)
    return weather_data_list, astro_dict

@flow(flow_run_name=generate_transform_weather_historical_data_flow_run_name, log_prints=True)
def flow_transform_weather_historical_data(weather_data_list: list, astro_dict: dict, city: str):
    daily_weather_analysis_to_insert = {}
    weather_data_df = task_transform_to_pd_df(weather_data_list)
    task_fill_direct_weather_analysis_fields(weather_data_df, daily_weather_analysis_to_insert)
    task_find_temp_c(weather_data_df, daily_weather_analysis_to_insert)
    task_find_max_wind_speed(weather_data_df, daily_weather_analysis_to_insert)
    task_find_avg_wind_speed(weather_data_df, daily_weather_analysis_to_insert)
    task_find_total_precip_mm(weather_data_df, daily_weather_analysis_to_insert)
    task_find_avg_humidity_perc(weather_data_df, daily_weather_analysis_to_insert)
    task_transform_astro_fields(astro_dict, daily_weather_analysis_to_insert)

    return daily_weather_analysis_to_insert


@flow(flow_run_name=generate_load_weather_historical_data_flow_run_name, log_prints=True)
def flow_load_weather_historical_data(daily_weather_analysis_to_insert: dict, city: str):
    load_status = task_load_daily_weather_analysis_if_necessary(daily_weather_analysis_to_insert)
    return load_status


@flow(flow_run_name=generate_historical_weather_flow_run_name, log_prints=True)
def weather_analysis_pipeline(city: str = "Sofia", time_zone: str = "Europe/Sofia"):
    weather_data_list, astro_dict = flow_extract_weather_historical_data(city, time_zone)
    if weather_data_list is not None:
        print(weather_data_list)
        print(astro_dict)
        daily_weather_analysis_to_insert = flow_transform_weather_historical_data(weather_data_list, astro_dict, city)
        print(daily_weather_analysis_to_insert)
        flow_load_weather_historical_data(daily_weather_analysis_to_insert, city)

def main():
    weather_analysis_london_deploy = weather_analysis_pipeline.to_deployment(
        name="weather-analysis-london-daily-flow-deployment",
        parameters={"city": "London", "time_zone": "Europe/London"},
        schedules=[
            CronSchedule(
                cron="6 0 * * *",
                timezone="Europe/London"
            )
        ]
    )

    weather_analysis_sofia_deploy = weather_analysis_pipeline.to_deployment(
        name="weather-analysis-sofia-daily-flow-deployment",
        parameters={"city": "Sofia", "time_zone": "Europe/Sofia"},
        schedules=[
            CronSchedule(
                cron="6 0 * * *",
                timezone="Europe/Sofia"
            )
        ]
    )

    serve(weather_analysis_london_deploy, weather_analysis_sofia_deploy)


if __name__ == "__main__":
    main()
    # weather_analysis_pipeline("Sofia", "Europe/Sofia")