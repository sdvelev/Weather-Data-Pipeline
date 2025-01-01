from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
import pytz
import os
from dotenv import load_dotenv
from prefect import get_run_logger
from prefect import task

@task
def task_transform_to_pd_df(weather_data_list: list):
    return pd.DataFrame(weather_data_list, columns=['id', 'city_id', 'date', 'time', 'temp_c', 'feels_like_c',
                                                    'weather_condition_code', 'weather_condition_text',
                                                    'weather_condition_icon', 'wind_speed_kph', 'wind_speed_mps',
                                                    'wind_dir', 'pressure_mb', 'precip_mm', 'humidity_perc',
                                                    'cloud_perc', 'uv_index'])

@task
def task_fill_direct_weather_analysis_fields(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    daily_weather_analysis_to_insert['city_id'] = weather_data_df['city_id'].iloc[0]
    daily_weather_analysis_to_insert['date'] = weather_data_df['date'].iloc[0]

@task
def task_find_temp_c(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    daily_weather_analysis_to_insert['max_temp_c'] = weather_data_df['temp_c'].max()
    daily_weather_analysis_to_insert['min_temp_c'] = weather_data_df['temp_c'].min()
    daily_weather_analysis_to_insert['avg_temp_c'] = weather_data_df['temp_c'].mean()

@task
def task_find_max_wind_speed(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    max_wind_speed_kph = weather_data_df['wind_speed_kph'].max()
    daily_weather_analysis_to_insert['max_wind_speed_kph'] = max_wind_speed_kph
    daily_weather_analysis_to_insert['max_wind_speed_mps'] = max_wind_speed_kph / 3.6

@task
def task_find_avg_wind_speed(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    avg_wind_speed_kph = weather_data_df['wind_speed_kph'].mean()
    daily_weather_analysis_to_insert['avg_wind_speed_kph'] = avg_wind_speed_kph
    daily_weather_analysis_to_insert['avg_wind_speed_mps'] = avg_wind_speed_kph / 3.6

@task
def task_find_total_precip_mm(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    daily_weather_analysis_to_insert['total_precip_mm'] = weather_data_df['precip_mm'].sum()

@task
def task_find_avg_humidity_perc(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    daily_weather_analysis_to_insert['avg_humidity_perc'] = int(weather_data_df['humidity_perc'].mean())

@task
def task_transform_astro_fields(astro_dict: dict, daily_weather_analysis_to_insert: dict):
    daily_weather_analysis_to_insert['sunrise'] = datetime.strptime(astro_dict['sunrise'], "%I:%M %p").strftime("%H:%M")
    daily_weather_analysis_to_insert['sunset'] = datetime.strptime(astro_dict['sunset'], "%I:%M %p").strftime("%H:%M")
    daily_weather_analysis_to_insert['moonrise'] = datetime.strptime(astro_dict['moonrise'], "%I:%M %p").strftime("%H:%M")
    daily_weather_analysis_to_insert['moonset'] = datetime.strptime(astro_dict['moonset'], "%I:%M %p").strftime("%H:%M")
    daily_weather_analysis_to_insert['moon_phase'] = astro_dict['moon_phase']