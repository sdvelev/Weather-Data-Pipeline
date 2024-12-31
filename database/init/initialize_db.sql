CREATE TABLE city (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    region VARCHAR(100),
    country VARCHAR(50) NOT NULL,
    time_zone VARCHAR(50) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
);

CREATE TABLE current_weather(
    id SERIAL PRIMARY KEY,
    city_id INT REFERENCES city(id),
    date DATE NOT NULL,
    time TIME NOT NULL,
    temp_c FLOAT NOT NULL,
    feels_like_c FLOAT NOT NULL,
    weather_condition_code INT NOT NULL,
    weather_condition_text VARCHAR(50) NOT NULL,
    weather_condition_icon VARCHAR(100) NOT NULL,
    wind_speed_kph FLOAT NOT NULL,
    wind_speed_mps FLOAT NOT NULL,
    wind_dir VARCHAR(3) NOT NULL,
    pressure_mb FLOAT NOT NULL,
    precip_mm FLOAT NOT NULL,
    humidity_perc INT NOT NULL,
    cloud_perc INT NOT NULL,
    uv_index FLOAT NOT NULL
);

CREATE TABLE weather_history(
    id SERIAL PRIMARY KEY,
    city_id INT REFERENCES city(id),
    date DATE NOT NULL,
    max_temp FLOAT NOT NULL,
    min_temp FLOAT NOT NULL,
    avg_temp FLOAT NOT NULL,
    max_wind_speed FLOAT NOT NULL,
    total_precip FLOAT NOT NULL,
    avg_humidity FLOAT NOT NULL,
    sunrise VARCHAR(5) NOT NULL,
    sunset VARCHAR(5) NOT NULL,
    moonrise VARCHAR(5) NOT NULL,
    moonset VARCHAR(5) NOT NULL,
    moon_phase VARCHAR(50) NOT NULL
);

ALTER TABLE city
ADD CONSTRAINT city_unique_constraint
UNIQUE(name, region, country, time_zone, latitude, longitude);

ALTER TABLE current_weather
ADD CONSTRAINT weather_unique_constraint
UNIQUE(city_id, date, time);