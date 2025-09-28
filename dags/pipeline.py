import os
import time
import logging
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine



def First(**kwrg):


    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    start = time.time()

    url_base_current = "http://api.weatherapi.com/v1/current.json"
    url_base_forecast = "http://api.weatherapi.com/v1/forecast.json"

    api_key = os.getenv("WEATHER_API_KEY", "your_api_key_from_weatherapi/")

    cities = pd.read_csv("/opt/airflow/data/valid_weatherapi_cities_top10.csv")["city_ascii"].dropna().unique()[:]
    # all the paths you will need to mount them manually in the docker-compose.yaml file
    logger.info(f"Total cities to process: {len(cities)}")

    semaphore = asyncio.Semaphore(2000)


    error_counts = defaultdict(int)
    successful_cities = []
    failed_cities = []

    async def fetch_weather(session, city):
        params_current = {"key": api_key, "q": city, "aqi": "no"}
        params_forecast = {"key": api_key, "q": city, "days": 3, "aqi": "no", "alerts": "no"}

        async with semaphore:
            try:
                await asyncio.sleep(0.1)
                
                async with session.get(url_base_current, params=params_current, timeout=15) as resp_current:
                    if resp_current.status == 429:
                        logger.warning(f"Rate limited for {city}, waiting...")
                        await asyncio.sleep(5)
                        return {"city": city, "error": "rate_limited", "retry": True}
                    
                    if resp_current.status != 200:
                        error_msg = f"HTTP {resp_current.status}"
                        error_counts[error_msg] += 1
                        failed_cities.append(city)
                        return {"city": city, "error": error_msg}
                    
                    data_current = await resp_current.json()

                if "error" in data_current:
                    error_msg = data_current['error']['message']
                    error_counts[error_msg] += 1
                    failed_cities.append(city)
                    logger.warning(f"API Error for {city}: {error_msg}")
                    return {"city": city, "error": error_msg}
                
                async with session.get(url_base_forecast, params=params_forecast, timeout=15) as resp_forecast:
                    if resp_forecast.status != 200:
                        error_msg = f"Forecast HTTP {resp_forecast.status}"
                        error_counts[error_msg] += 1
                        failed_cities.append(city)
                        return {"city": city, "error": error_msg}
                    
                    data_forecast = await resp_forecast.json()

                if "error" in data_forecast:
                    error_msg = data_forecast["error"]["message"]
                    error_counts[error_msg] += 1
                    failed_cities.append(city)
                    return {"city": city, "error": error_msg}

                
                result = {
                            "city": city,
                            "country": data_current["location"]["country"],
                            "timezone": data_current["location"]["tz_id"],
                            "localtime": data_current["location"]["localtime"],
                            "last_updated": data_current["current"]["last_updated"],
                            "temp_c": data_current["current"]["temp_c"],
                            "condition": data_current["current"]["condition"]["text"],
                            "condition_icon": "https:" + data_current["current"]["condition"]["icon"],
                            "wind_kph": data_current["current"]["wind_kph"],
                            "wind_dir": data_current["current"]["wind_dir"],
                            "cloud": data_current["current"]["cloud"],
                            "humidity": data_current["current"]["humidity"],
                            "pressure_mb": data_current["current"]["pressure_mb"],
                            "day_1_temp": None,
                            "day_1_condition": None,
                            "day_2_temp": None,
                            "day_2_condition": None,
                            "day_3_temp": None,
                            "day_3_condition": None,
                            "time_data_gotten": datetime.now(),
                            "error": None
                        }


                for i, day in enumerate(data_forecast["forecast"]["forecastday"], start=1):
                    result[f"day_{i}_temp"] = day["day"]["avgtemp_c"]
                    result[f"day_{i}_condition"] = day["day"]["condition"]["text"]

                successful_cities.append(city)
                return result

            except asyncio.TimeoutError:
                error_counts["timeout"] += 1
                failed_cities.append(city)
                logger.warning(f"Timeout for {city}")
                return {"city": city, "error": "timeout"}
            except Exception as e:
                error_msg = str(e)
                error_counts[error_msg] += 1
                failed_cities.append(city)
                logger.error(f"Unexpected error with {city}: {e}")
                return {"city": city, "error": error_msg}


    async def main():
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            batch_size = 500
            all_results = []
            
            for i in range(0, len(cities), batch_size):
                batch_cities = cities[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(cities)-1)//batch_size + 1} ({len(batch_cities)} cities)")
                
                tasks = [fetch_weather(session, city) for city in batch_cities]
                batch_results = await asyncio.gather(*tasks)
                all_results.extend([r for r in batch_results if r is not None])
                
                if i + batch_size < len(cities):
                    logger.info("Waiting 10 seconds between batches...")
                    await asyncio.sleep(10)
            
            successful_results = [r for r in all_results if r.get("error") is None]
            failed_results = [r for r in all_results if r.get("error") is not None]
            
            logger.info(f"Successful: {len(successful_results)}, Failed: {len(failed_results)}")
            
            logger.info("Error Summary:")
            for error, count in error_counts.items():
                logger.info(f"  {error}: {count}")
            
            if successful_results:
                df = pd.DataFrame(successful_results)
                
                for i in range(1, 4):
                    if f"day_{i}_temp" not in df.columns:
                        df[f"day_{i}_temp"] = None
                    if f"day_{i}_condition" not in df.columns:
                        df[f"day_{i}_condition"] = None

                df.to_csv("/tmp/airflow_temp/weather_data.csv", index=False)
                logger.info(f"Saved {len(df)} successful records to weather_data.csv")
            
            return pd.DataFrame(successful_results) if successful_results else pd.DataFrame()


    weather_df = asyncio.run(main())

    if not weather_df.empty:
        try:
            engine = create_engine(
                "postgresql+psycopg2://postgres:011212369@localhost:5432/postgres"
            )
        except Exception as e:
            logger.error(f"Database connection failed: {e}")

    end = time.time()
    logger.info(f"⏱ Script finished in {end - start:.2f} seconds")
    logger.info(f"📊 Final stats: {len(successful_cities)} successful, {len(failed_cities)} failed out of {len(cities)} total")






def insertion(**kwarg):

    file = pd.read_csv("/tmp/airflow_temp/weather_data.csv")
    engine = create_engine(
    "postgresql+psycopg2://your_connection_credens")
    with engine.begin() as conn:
        file.to_sql(
            "Weather_snapshots_now_and_prev",
            conn,
            schema = "Weather",
            if_exists="replace"
        )

with DAG(
    dag_id="Weather_Pipeline",
    default_args={"owner": "noah", "retries": 1},
    start_date=days_ago(1),
    catchup=False,
    schedule_interval=timedelta(minutes=15),
) as dag:

    ingesting = PythonOperator(
        task_id="ingesting",
        python_callable=First,
    )

    insert_task = PythonOperator(
        task_id="insert_to_postgres",
        python_callable=insertion,
    )

    ingesting >> insert_task