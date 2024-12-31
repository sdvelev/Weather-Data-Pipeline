from prefect import task

@task
def task_fill_direct_city_fields(weather_data: dict, city_data_to_insert: dict):
    city_data_to_insert['name'] = weather_data['location']['name']
    city_data_to_insert['region'] = weather_data['location']['region']
    city_data_to_insert['country'] = weather_data['location']['country']
    city_data_to_insert['time_zone'] = weather_data['location']['tz_id']
    city_data_to_insert['latitude'] = weather_data['location']['lat']
    city_data_to_insert['longitude'] = weather_data['location']['lon']

@task
def task_fill_direct_weather_fields(weather_data: dict, weather_data_to_insert: dict):
    weather_data_to_insert['temp_c'] = weather_data['current']['temp_c']
    weather_data_to_insert['feels_like_c'] = weather_data['current']['feelslike_c']
    weather_data_to_insert['weather_condition_code'] = weather_data['current']['condition']['code']
    weather_data_to_insert['weather_condition_text'] = weather_data['current']['condition']['text']
    weather_data_to_insert['weather_condition_icon'] = weather_data['current']['condition']['icon']
    weather_data_to_insert['wind_speed_kph'] = weather_data['current']['wind_kph']
    weather_data_to_insert['pressure_mb'] = weather_data['current']['pressure_mb']
    weather_data_to_insert['precip_mm'] = weather_data['current']['precip_mm']
    weather_data_to_insert['humidity_perc'] = weather_data['current']['humidity']
    weather_data_to_insert['cloud_perc'] = weather_data['current']['cloud']
    weather_data_to_insert['uv_index'] = weather_data['current']['uv']

@task
def task_transform_date_time_fields(weather_data: dict, weather_data_to_insert: dict):
    weather_data_to_insert['date'], weather_data_to_insert['time'] = weather_data['current']['last_updated'].split()

@task
def task_transform_wind_speed_mps(weather_data: dict, weather_data_to_insert: dict):
    weather_data_to_insert['wind_speed_mps'] = weather_data['current']['wind_kph'] / 3.6

@task
def degree_to_compass_dir(wind_degrees: int):
    dir_position = int((wind_degrees / 22.5) + 0.5)
    dirs_list = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return dirs_list[(dir_position % 16)]

@task
def task_transform_wind_dir(weather_data: dict, weather_data_to_insert: dict):
    weather_data_to_insert['wind_dir'] = degree_to_compass_dir(weather_data['current']['wind_degree'])

