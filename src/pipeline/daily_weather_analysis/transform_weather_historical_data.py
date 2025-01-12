import os
import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime
from prefect import task
from windrose import WindroseAxes


@task(retries=2, retry_delay_seconds=3, timeout_seconds=20, log_prints=True)
def task_transform_to_pd_df(weather_data_list: list):
    return pd.DataFrame(weather_data_list, columns=['id', 'city_id', 'date', 'time', 'temp_c', 'feels_like_c',
                                                    'weather_condition_code', 'weather_condition_text',
                                                    'weather_condition_icon', 'wind_speed_kph', 'wind_speed_mps',
                                                    'wind_dir', 'pressure_mb', 'precip_mm', 'humidity_perc',
                                                    'cloud_perc', 'uv_index'])

@task(retries=2, retry_delay_seconds=2, timeout_seconds=6)
def task_fill_direct_weather_analysis_fields(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    daily_weather_analysis_to_insert['city_id'] = weather_data_df['city_id'].iloc[0]
    daily_weather_analysis_to_insert['date'] = weather_data_df['date'].iloc[0]

@task(retries=2, retry_delay_seconds=2, timeout_seconds=16)
def task_find_temp_c(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    daily_weather_analysis_to_insert['max_temp_c'] = weather_data_df['temp_c'].max()
    daily_weather_analysis_to_insert['min_temp_c'] = weather_data_df['temp_c'].min()
    daily_weather_analysis_to_insert['avg_temp_c'] = weather_data_df['temp_c'].mean()

@task(retries=2, retry_delay_seconds=2, timeout_seconds=16)
def task_find_max_wind_speed(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    max_wind_speed_kph = weather_data_df['wind_speed_kph'].max()
    daily_weather_analysis_to_insert['max_wind_speed_kph'] = max_wind_speed_kph
    daily_weather_analysis_to_insert['max_wind_speed_mps'] = max_wind_speed_kph / 3.6

@task(retries=2, retry_delay_seconds=2, timeout_seconds=16)
def task_find_avg_wind_speed(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    avg_wind_speed_kph = weather_data_df['wind_speed_kph'].mean()
    daily_weather_analysis_to_insert['avg_wind_speed_kph'] = avg_wind_speed_kph
    daily_weather_analysis_to_insert['avg_wind_speed_mps'] = avg_wind_speed_kph / 3.6

@task(retries=2, retry_delay_seconds=2, timeout_seconds=10)
def task_find_total_precip_mm(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    daily_weather_analysis_to_insert['total_precip_mm'] = weather_data_df['precip_mm'].sum()

@task(retries=2, retry_delay_seconds=2, timeout_seconds=10)
def task_find_avg_humidity_perc(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    daily_weather_analysis_to_insert['avg_humidity_perc'] = int(weather_data_df['humidity_perc'].mean())

@task(retries=2, retry_delay_seconds=2, timeout_seconds=10)
def task_transform_astro_fields(astro_dict: dict, daily_weather_analysis_to_insert: dict):
    daily_weather_analysis_to_insert['sunrise'] = datetime.strptime(astro_dict['sunrise'], "%I:%M %p").strftime("%H:%M")
    daily_weather_analysis_to_insert['sunset'] = datetime.strptime(astro_dict['sunset'], "%I:%M %p").strftime("%H:%M")
    daily_weather_analysis_to_insert['moonrise'] = datetime.strptime(astro_dict['moonrise'], "%I:%M %p").strftime("%H:%M")
    daily_weather_analysis_to_insert['moonset'] = datetime.strptime(astro_dict['moonset'], "%I:%M %p").strftime("%H:%M")
    daily_weather_analysis_to_insert['moon_phase'] = astro_dict['moon_phase']

@task(retries=2, retry_delay_seconds=10, timeout_seconds=60, log_prints=True)
def task_generate_temp_changes_plot(weather_data_df: pd.DataFrame, city: str, country: str):
    plot_date = weather_data_df['date'].iloc[0]
    weather_data_df['time'] = pd.to_datetime(weather_data_df['time'], format='%H:%M:%S')

    df = weather_data_df.sort_values(by='time')

    output_dir = f"./plots/{plot_date}"
    os.makedirs(output_dir, exist_ok=True)

    plt.figure(figsize=(12, 6))
    plt.plot(df['time'].dt.strftime('%H:%M'), df['temp_c'], marker='o', label='Temperature (°C)',
             color='goldenrod', linewidth=2)

    plt.xlabel('Time of Day', fontsize=12, fontweight='bold')
    plt.ylabel('Temperature (°C)', fontsize=12, fontweight='bold')
    plt.title(f"Temperature Changes Throughout the Day for {city}, {country}\n(Date: {plot_date})",
              fontsize=14, fontweight='bold')
    plt.grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
    plt.xticks(rotation=45, fontsize=10)
    plt.yticks(fontsize=10)
    plt.legend(fontsize=10)

    for i, (time, temp) in enumerate(zip(df['time'].dt.strftime('%H:%M'), df['temp_c'])):
        plt.annotate(f'{temp}°C', xy=(time, temp), xytext=(0, 5), textcoords='offset points',
                     ha='center', fontsize=10, color='black')

    output_file = os.path.join(output_dir, f"temperature_changes_{city.replace(' ', '_')}_"
                                           f"{country.replace(' ', '_')}.png")
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()

@task(retries=2, retry_delay_seconds=10, timeout_seconds=60, log_prints=True)
def task_generate_wind_speed_changes_plot(weather_data_df: pd.DataFrame, city: str, country: str):
    plot_date = weather_data_df['date'].iloc[0]
    weather_data_df['time'] = pd.to_datetime(weather_data_df['time'], format='%H:%M:%S')

    df = weather_data_df.sort_values(by='time')

    output_dir = f"./plots/{plot_date}"
    os.makedirs(output_dir, exist_ok=True)

    plt.figure(figsize=(12, 6))
    plt.plot(df['time'].dt.strftime('%H:%M'), df['wind_speed_kph'].apply(lambda x: float('{:,.2f}'.format(x))),
             marker='o', label='Wind Speed (km/h)', color='darkblue', linewidth=2)

    plt.xlabel('Time of Day', fontsize=12, fontweight='bold')
    plt.ylabel('Wind Speed (km/h)', fontsize=12, fontweight='bold')
    plt.title(f"Wind Speed Changes Throughout the Day for {city}, {country}\n(Date: {plot_date})",
              fontsize=14, fontweight='bold')
    plt.grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
    plt.xticks(rotation=45, fontsize=10)
    plt.yticks(fontsize=10)
    plt.legend(fontsize=10)

    output_file = os.path.join(output_dir, f"wind_speed_changes_{city.replace(' ', '_')}_"
                                           f"{country.replace(' ', '_')}.png")
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()

@task(retries=2, retry_delay_seconds=10, timeout_seconds=60, log_prints=True)
def task_generate_precipitation_changes_plot(weather_data_df: pd.DataFrame, city: str, country: str):
    plot_date = weather_data_df['date'].iloc[0]
    weather_data_df['time'] = pd.to_datetime(weather_data_df['time'], format='%H:%M:%S')

    df = weather_data_df.sort_values(by='time')

    output_dir = f"./plots/{plot_date}"
    os.makedirs(output_dir, exist_ok=True)

    plt.figure(figsize=(12, 6))
    plt.bar(df['time'].dt.strftime('%H:%M'), df['precip_mm'], color='skyblue')

    plt.xlabel('Time of Day', fontsize=12, fontweight='bold')
    plt.ylabel('Precipitation (mm)', fontsize=12, fontweight='bold')
    plt.title(f"Precipitation Changes Throughout the Day for {city}, {country}\n(Date: {plot_date})",
              fontsize=14, fontweight='bold')
    plt.xticks(rotation=45, fontsize=10)
    plt.yticks(fontsize=10)

    output_file = os.path.join(output_dir, f"precipitation_changes_{city.replace(' ', '_')}_"
                                           f"{country.replace(' ', '_')}.png")
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()

@task(retries=2, retry_delay_seconds=10, timeout_seconds=60, log_prints=True)
def task_plot_temperature_distribution(weather_data_df: pd.DataFrame, city: str, country: str):
    plot_date = weather_data_df['date'].iloc[0]

    output_dir = f"./plots/{plot_date}"
    os.makedirs(output_dir, exist_ok=True)

    plt.figure(figsize=(8, 6))
    plt.boxplot(weather_data_df['temp_c'])
    plt.ylabel('Temperature (°C)', fontsize=12, fontweight='bold')
    plt.title(f"Daily Temperature Distribution for {city}, {country}\n(Date: {plot_date})",
              fontsize=14, fontweight='bold')

    output_file = os.path.join(output_dir, f"temperature_distribution_{city.replace(' ', '_')}_"
                                           f"{country.replace(' ', '_')}.png")
    plt.savefig(output_file, dpi=300)
    plt.close()

@task(retries=2, retry_delay_seconds=10, timeout_seconds=60, log_prints=True)
def task_plot_wind_rose(weather_data_df: pd.DataFrame, city: str, country: str):
    plot_date = weather_data_df['date'].iloc[0]
    output_dir = f"./plots/{plot_date}"
    os.makedirs(output_dir, exist_ok=True)

    direction_to_angle = {
        "N": 0,
        "NNE": 22.5,
        "NE": 45,
        "ENE": 67.5,
        "E": 90,
        "ESE": 112.5,
        "SE": 135,
        "SSE": 157.5,
        "S": 180,
        "SSW": 202.5,
        "SW": 225,
        "WSW": 247.5,
        "W": 270,
        "WNW": 292.5,
        "NW": 315,
        "NNW": 337.5
    }

    directions = weather_data_df['wind_dir']
    angles = [direction_to_angle[direct] for direct in directions]

    ax = WindroseAxes.from_ax()
    ax.bar(angles, weather_data_df['wind_speed_mps'], normed=True, opening=0.8, edgecolor='white')
    ax.set_title(f"Daily Wind Rose for {city}, {country}\n(Date: {plot_date})", fontsize=14, fontweight="bold")
    ax.set_legend(title = 'Wind Speed (m/s)')

    output_file = os.path.join(output_dir, f"wind_rose_{city.replace(' ', '_')}_"
                                           f"{country.replace(' ', '_')}.png")
    plt.savefig(output_file, dpi=300)
    plt.close()