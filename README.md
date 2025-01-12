# Weather Analysis Data Pipeline ☀️☁️🌧️⛈️🌨️❄️

## Purpose 📋

The purpose of the pipeline is to analyse information for the weather. It extracts hourly weather data from [Weather API](https://www.weatherapi.com/) for different cities – in my case for Sofia, Rome, London and New York. At the end of the day (at different time for the different time zones of the cities) some interesting visualisations are produced using statistical methods and plotting. 

## Technology Stack ⚙️

The technology stack entails **Python** for writing the scripts, **Prefect** for automation, **Docker** for hosting the database container, **PostgreSQL** for data storage, **Adminer** for managing the content of the database, **HTTPX** for HTTP client for Python, **Pandas** for working with data frames, **DateTime** for working with dates and time, **Psycopg2** for PostgreSQL database adapter for Python, **Matplotlib** and **Windrose** for data visualisation purposes. The main methodology I have relied on is the classical **ETL (Extract, Transform, Load)**. Having that in mind, I have constructed **two data pipelines**. The first one is responsible for extracting **current weather data every hour**. The second one is responsible for **summarising the weather data at the end of the day**.

## Packages and Libraries 📚

Here is the list with all dependencies and their versions which can be found in the `requirements.txt` file:  

```
httpx~=1.0.0b0
prefect~=3.1.10
psycopg2~=2.9.10
psycopg2-binary~=2.9.10
python-dotenv~=1.0.1
pytz~=2024.2
pandas~=2.2.3
matplotlib~=3.10.0
windrose~=1.9.2
```

## Execution Guide 🏃

**1. Clone this repository from GitHub and open it in the terminal:**
``` bash
git clone https://github.com/sdvelev/Weather-Data-Pipeline
```

**2. Create a virtual environment for the project where specific dependencies can be installed.**

You can do that through the IDE or by running the following commands:
``` bash
python3 -m venv myenv
source myenv/bin/activate
```

**3. Install the necessary packages using the ‘requirements.txt’ file by running the command:**
``` bash
pip install -r requirements.txt
```

**4. Create .env file with the following variables and give them appropriate values:**
```
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
DB_USER=
DB_PASSWORD=
DB_NAME=
DB_HOST=localhost
WEATHER_API_KEY=
```

**5. Install Docker Desktop (if not already installed)**

We have a `docker-compose.yaml` file in which two containers are defined. The first one is for **PostgreSQL** database. The Postgres user, password and db are already defined in the *.env* file so we take them from there with `${POSTGRES_USER}`, `${POSTGRES_PASSWORD}` and `${POSTGRES_DB}`. The `docker-compose.yaml` file automatically detects *.env* file. The other container used is **Adminer** which is a tool for managing content of databases. We will use it on port 8080 to check the structure of the database and its content.

<p align="center">
<img width="620px" src="https://github.com/sdvelev/Weather-Data-Pipeline/blob/main/resources/adminer_UI.png" alt="adminer_UI">
</p>

**5.1 Start the Docker container with the following command:**
``` bash
docker-compose up
```

**5.2 You can stop the Docker container later with the following command:**
``` bash
docker-compose down
```

**5.3 You can stop the Docker container and delete all the volumes associated with it (i.e. the database content) with the following command:**
``` bash
docker-compose down -v
```

**6. Configure Prefect to use local Prefect server instead of Prefect Cloud by running the following command:**
``` bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

**7. Run the Prefect server at port 4200 with the following command:**
``` bash
prefect server start
```

**8. Run the current weather data pipeline deployments with the following command:**
``` bash
python3 ./src/pipeline/current_weather_data/current_weather_pipeline.py
```

**9. Run the daily weather analysis data pipeline deployments with the following command:**
``` bash
python3 ./src/pipeline/daily_weather_analysis/weather_analysis_pipeline.py
```

The last two steps will activate the pipeline automation. Every hour new current weather data will be stored in the PostgreSQL database. We can inspect that in the Adminer UI client on port 8080. By default, we get hourly data for Sofia, Rome, London and New York. Summarisations for theses cities are conducted at the end of the day as well. We can see the diagrams for these cities in the ‘plots’ directory for the different dates.

## Project Components and Program Logic 👨‍💻

**1. Data Ingestion**

The source of weather data is the public Weather API. It is designed for developers providing ultimate weather and geolocation API. It offers a free plan that gives access to 1 million calls per month. Various kinds of data are available in Weather API. For the purposes of my project, I need to use two APIs – **Realtime API** (for the first pipeline that extracts hourly weather data) and **History API** (for the second pipeline that extracts daily astronomical data used for summarising the day such as time of sunrise, sunset, moonrise, moonset, etc.).

The authentication to the [Weather API](https://weatherapi.com) is provided by passing my API key as request parameter to a URL. The API key can be obtained after signing up in the profile section.
The base URL format is: `https://api.weatherapi.com/v1`. The path URL for the Realtime API is `/current.json`. The path URL for the History API is `/history.json`. The format of the summarised URL is:
`{base_url}{path_url}?key={api_key}&q={city}` where *API key* is our own API key (for security reasons it is located in **.env** file as an environment variable so that it is not uploaded to public code repositories) and *city* is a query parameter based on which data is sent back. It could be in one of the following formats: latitude, longitude; city name; US zip, UK postcode, Canada postal code, IP address, etc.

For the **current weather data pipeline**, the extraction logic is divided into two separate *Prefect* tasks. The first one is for generating the URL. That task uses the base URL, the path URL for the Realtime API, the provided API key from the **.env** file and the city as an input parameter. The second task is responsible for the sheer extraction of the current weather data. It uses the URL generated from the first task and a logger that is used for error handling. The whole process is carried out with a GET query. The result is returned in json format for easier manipulation. Actually, most of the returned fields will not be used. Therefore, it is important that we can easily separate the required ones from the others.

For the **historical weather data pipeline** (responsible for conducting daily summaries based on the extracted data from the previous data pipeline), the extraction logic is more complex as it relies on two data sources. The **first data source** is the Historical API for which a URL is generated as a *Prefect* task. The format of the summarised URL is: `{base_url}{path_url_history_api}?key={api_key}&q={city}&dt={previous_date}` where all of the parameters follow the same logic as is in the first data pipeline except the additional parameter *previous_date* which contains the date for which we want to extract the astronomical data. With another *Prefect* task I create a dictionary that extracts only the required astronomical fields. With another task I get the current date and time for a provided time zone. 

The **second data source** for the historical weather data pipeline is my **PostgreSQL** database. Since the weather records contain only *city_id* which is a foreign key connected to the primary key of another table, the first step is to find that id. That is done in another task executing the following SELECT query:

``` SQL
SELECT id 
FROM city 
WHERE name=%(name)s AND region=%(region)s AND country=%(country)s  
```

For extracting the hourly records for a given city (*city_id*) and a specified date from the database I execute the following SELECT query:

``` SQL
SELECT * 
FROM current_weather 
WHERE city_id=%(city_id)s AND date=%(date)s
```

The result from the above query is a list of tuples which will be transformed into data frame in the Transform phase of the ETL process.

**2. Data Cleaning**

**Data Cleaning** is part of the Transform phase of the ETL methodology. The main issue for that part regarding my project is related to the data type mismatches. Since I load the data to be stored into a dictionary, I do not take into consideration for now the issues related to reading numbers as strings. This will be addressed in the Data Storage part where suitable casts need to be considered.

As the API guarantees that all the fields are available, I do not consider a scenario where I have to handle missing data, remove duplicates or address inconsistences. Another difficulty that can arise is when reading dates and times since there are different formats with different level of detail. Fortunately, in most of the cases (except for the astronomical fields) the [Weather API](https://www.weatherapi.com) presents the date and time in machine-readable format (e.g. 2025-01-08 16:06).

**3. Data Transformation**

**Data Transformation** is part of the Transform phase of the ETL methodology. That part entails renaming of columns. I give them more talkable names such as `latitude` instead of `lat`, `longitude` instead of `lon`. I follow the convention to include the metric units after the parameter name such as `humidity_perc` instead of `humidity`, `cloud_perc` instead of `cloud` and other examples.

Another field that needs modification is the *last updated time* of the weather measurements. In the result extracted from the API it is only one field that contains the date and time. For my purposes, I split them and put them in separate fields. From the API I get the wind speed in km/h. As I want to store the data in m/s as well, I have another field that gets the speed in km/h and divide it by 3.6 so that I receive the answer in m/s.

I use another more complex mathematical transformation for defining the wind direction only by having data for the wind degrees. The transformation is based on the 16-point compass directions. Another transformation is used for the time of the astronomical fields as it is in 12-hour time convention. With the help of the **DateTime** package and its `strftime()` method, I convert the time format into 24-hour clock. Another transformation is turning the list of tuples into **Pandas** data frame. When performing that task, it is important to give proper names of the columns.

**4. Data Agregation**

**Data Aggregation** is part of the Transform phase of the ETL methodology. For my project it is visible in the second pipeline (the one that summarises weather records once at the end of the day). Having the whole data in one data frame helps so that different aggregate operations can be used. For example, to find the maximal temperature for the day, I use the `max()` function. For finding the minimum temperature, I use the `min()` function. For finding the average temperature I use the `mean()` function. For finding the total precipitation I use the `sum()` function and so on. 

**5. Exploratory Data Analysis (EDA)**

The **Exploratory Data Analysis** entails producing thoughtful visualisations for the weather data. With the help of the data, I make five different types of diagrams each allocated as a separate *Prefect* task – **Temperature Changes Throughout the Day** (line plot), **Wind Speed Changes Throughout the Day** (line plot), **Precipitation Changes Throughout the Day** (bar plot), **Daily Temperature Distribution** (boxplot), **Daily Wind Rose** (wind rose plot). The plots are generated at the end of the day depending on the time zone of the city and are stored as *png* files in a separate folder called *plots* that contains separate folders for the different days. The name of the image files contain the name of the city for which the weather data relates to. The plots are created with the help of the **Matplotlib** and **Windrose** packages in Python.

<p align="center">
<img width="620px" src="https://github.com/sdvelev/Weather-Data-Pipeline/blob/main/resources/temperature_changes_Sofia_Bulgaria.png" alt="temperature_changes_Sofia_Bulgaria">
</p>

<p align="center"><em>From that line plot we can notice how big the temperature amplitude can be for 24 hours.</em></p>

<p align="center">
<img width="620px" src="https://github.com/sdvelev/Weather-Data-Pipeline/blob/main/resources/wind_speed_changes_New_York_United_States_of_America.png" alt="wind_speed_changes_New_York_United_States_of_America">
</p>

<p align="center">
<img width="620px" src="https://github.com/sdvelev/Weather-Data-Pipeline/blob/main/resources/precipitation_changes_Rome_Italy.png" alt="precipitation_changes_Rome_Italy">
</p>

<p align="center">
<img width="620px" src="https://github.com/sdvelev/Weather-Data-Pipeline/blob/main/resources/temperature_distribution_London_United_Kingdom.png" alt="temperature_distribution_London_United_Kingdom">
</p>

<p align="center">
<img width="620px" src="https://github.com/sdvelev/Weather-Data-Pipeline/blob/main/resources/wind_rose_Rome_Italy.png" alt="wind_rose_Rome_Italy">
</p>

<p align="center"><em>From that wind rose we show the frequency and speed of south winds in Rome, highlighting prevailing patterns and calm conditions for meteorological and practical applications</em></p>

**6. Data Storage**

The **Data Storage part** represents the final Load step of the ETL methodology. I chose to load the data into PostgreSQL relational database. For creation of the tables I have initialization script (`initialize_db.sql`) which shapes the database. I have modelled the problem with three relations and two one-to-many relationships. The visual representation of the tables can be seen below:

<p align="center">
<img width="620px" src="https://github.com/sdvelev/Weather-Data-Pipeline/blob/main/resources/db_ERD_schema.png" alt="db_ERD_schema">
</p>

In addition to the relations, I have created three unique constraints so that we do not have duplicate records. With the automation and deployment repetition that is not possible, but I have undertaken additional preventive measures so that this absence is guaranteed:

```SQL
ALTER TABLE city
ADD CONSTRAINT city_unique_constraint
UNIQUE(name, region, country, time_zone, latitude, longitude);

ALTER TABLE current_weather
ADD CONSTRAINT weather_unique_constraint
UNIQUE(city_id, date, time);

ALTER TABLE daily_weather_analyses
ADD CONSTRAINT daily_weather_unique_constraint
UNIQUE(city_id, date);
```

The sheer Load part is done with the help of the **Psycopg2**. To connect to the database, I get the required environment variables (`DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_NAME`) from the *.env* file. For instance, the INSERT query for the daily historical data pipeline is:

``` SQL
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
```

**7. Pipeline Automation**

I use the **Prefect** framework for the automation of the pipeline. It simplifies the creation, scheduling, and monitoring of complex data pipelines. The framework’s documentation is detailed and easy to read. I relied heavily on it since I had not worked with such data pipeline technologies before. Using decorators for `@flow` and `@task` we can transform any Python project into units of work that can be observed and orchestrated. We only have to define workflows as Python script and Prefect handles the rest. It provides error handling and retry mechanism that I have used for each task. In this way we can ensure that tasks are re-attempted in a robust and configurable manner, helping address transient failures. Having that we increase the chance of recovery from temporary issues.

``` Python
@flow(flow_run_name=generate_historical_weather_flow_run_name, log_prints=True)
def weather_analysis_pipeline(city: str, time_zone: str):
    weather_data_list, astro_dict, country = flow_extract_weather_historical_data(city, time_zone)
    if weather_data_list is not None:
        daily_weather_analysis_to_insert = flow_transform_weather_historical_data(weather_data_list,   
                                                                          astro_dict, city, country)
        flow_load_weather_historical_data(daily_weather_analysis_to_insert, city)
```

``` Python
@task(retries=2, retry_delay_seconds=2, timeout_seconds=16)
def task_find_temp_c(weather_data_df: pd.DataFrame, daily_weather_analysis_to_insert: dict):
    daily_weather_analysis_to_insert['max_temp_c'] = weather_data_df['temp_c'].max()
    daily_weather_analysis_to_insert['min_temp_c'] = weather_data_df['temp_c'].min()
    daily_weather_analysis_to_insert['avg_temp_c'] = weather_data_df['temp_c'].mean()
```

In addition, **Prefect** provides a user-friendly dashboard for monitoring of the runs, flows, deployments, etc. For the name of the flows and the tasks I generate suitable names so that I can easily trace and find if there is something wrong with the processes.

<p align="center">
<img width="620px" src="https://github.com/sdvelev/Weather-Data-Pipeline/blob/main/resources/prefect_dashboard.png" alt="prefect_dashboard">
</p>

For the **deployment** and **scheduling** part I run multiple flows in local processes. This can be achieved with the serve utility along with to_deployment method of flows. I use multiple flows by only changing the parameters with the city names and time zones for the different cities I have chosen to analyse. For assigning the repetition times I use the **cron** job scheduler syntax as it is supported by Prefect. For instance, I activate the flow for Sofia at 30 minutes past the hour every day:

``` Python
weather_data_sofia_deploy = current_weather_data_pipeline.to_deployment(
        name="weather-data-sofia-hourly-flow-deployment",
        cron="30 * * * *",
        parameters={"city": "Sofia"},
    )  
```

It is interesting when it comes to the daily analysis of the weather in London, for instance. Since I want to make it at the end of the day but for the Greenwich Mean Time, I pass information for the zone as well so that the **cron scheduler** knows which time to consider at 23:22 every day:

``` Python
weather_analysis_london_deploy = weather_analysis_pipeline.to_deployment(
        name="weather-analysis-london-daily-flow-deployment",
        parameters={"city": "London", "time_zone": "Europe/London"},
        schedules=[
            CronSchedule(
                cron="22 23 * * *",
                timezone="Europe/London"
            )
        ]
    )
```

The **temporal dependency** for the first data pipeline (e.g. **current-weather-data-pipeline-for-Sofia**) is:

<p align="center">
<img width="520px" src="https://github.com/sdvelev/Weather-Data-Pipeline/blob/main/resources/temporal_dependency_current_weather_data_pipeline_for_sofia.png" alt="temporal_dependency_current_weather_data_pipeline_for_sofia">
</p>

The **temporal dependency** for the second data pipeline (e.g. **weather-analysis-pipeline-for-Sofia**) is:

<p align="center">
<img width="520px" src="https://github.com/sdvelev/Weather-Data-Pipeline/blob/main/resources/temporal_dependency_weather_analysis_pipeline_for_sofia.png" alt="temporal_dependency_weather_analysis_pipeline_for_sofia">
</p>
