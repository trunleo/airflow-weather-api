"""
DAG: weather_api_pipeline

This Airflow DAG fetches and processes weather data from the TMD (Thai Meteorological Department) WeatherToday API.

Tasks:
    - extract: Extracts weather data from the TMD API using parameters provided via DAG params.
    - transform: Transforms the raw API response into structured station and weather data using pandas DataFrames.
    - load: Loads (prints) the number of stations and weather records processed.

DAG Parameters:
    - data_type (str): Type of data to fetch ("today" or "forecast"). Default is "today".
    - uid (str): API user ID.
    - ukey_map (dict): Mapping of data_type to API keys.

Schedule:
    - Runs daily, starting from 2025-01-01.
    - No catchup.

Tags:
    - weather
    - tmd
"""

from airflow import DAG
from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import requests
import pandas as pd
from typing import Dict, Any



BASE_URL = "http://data.tmd.go.th/api/WeatherToday/V2/"
CONTENT_TYPE = "application/json"
PARAM_DATA_TYPE = "data_type"
PARAM_UID = "uid"
PARAM_UKEY_MAP = "ukey_map"



with DAG(
    dag_id="weather_api_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["weather", "tmd"],
    params={
        PARAM_DATA_TYPE: "today",
        PARAM_UID: "api",
        PARAM_UKEY_MAP: {
            "today": "api12345",
            "forecast": "api123"
        }
    }
) as dag:


    @task()
    def extract(**context) -> Dict[str, Any]:
        """Extract weather data from TMD API."""
        log = LoggingMixin().log
        params = context["params"]
        # Parameter validation
        for key in [PARAM_DATA_TYPE, PARAM_UID, PARAM_UKEY_MAP]:
            if key not in params:
                log.error(f"Missing required DAG param: {key}")
                raise ValueError(f"Missing required DAG param: {key}")
        data_type = params[PARAM_DATA_TYPE]
        uid = params[PARAM_UID]
        ukey_map = params[PARAM_UKEY_MAP]
        if data_type not in ukey_map:
            log.error(f"data_type '{data_type}' not found in ukey_map")
            raise ValueError(f"data_type '{data_type}' not found in ukey_map")
        ukey = ukey_map[data_type]

        query_params = {
            "uid": uid,
            "ukey": ukey,
            "format": "json"
        }
        try:
            response = requests.get(BASE_URL, params=query_params, headers={"Content-Type": CONTENT_TYPE}, timeout=60)
            response.raise_for_status()
            log.info(f"Successfully fetched data from TMD API for data_type={data_type}")
            return response.json()
        except requests.RequestException as e:
            log.error(f"Failed to fetch data from TMD API: {e}")
            raise


    @task()
    def transform(data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw API response into structured station and weather data."""
        log = LoggingMixin().log
        stations = data.get('Stations', {}).get('Station', [])
        if not isinstance(stations, list):
            stations = [stations] if stations else []

        # Get station list
        list_station = [
            {k: v for k, v in st.items() if k != 'Observation'}
            for st in stations
        ]
        station_df = pd.DataFrame(list_station) if list_station else pd.DataFrame()

        # Get weather data
        weather_data = []
        for st in stations:
            obs = st.get('Observation', {})
            if obs:
                records = {k: v for k, v in obs.items() if k != '@attributes'}
                records.update({"StationNumber": st.get("WmoStationNumber")})
                weather_data.append(records)
        weather_df = pd.DataFrame(weather_data) if weather_data else pd.DataFrame()

        log.info(f"Transformed {len(station_df)} stations and {len(weather_df)} weather records.")
        return {
            "station": station_df.to_dict(orient="records"),
            "weather": weather_df.to_dict(orient="records"),
        }


    @task()
    def load(data: Dict[str, Any]):
        """Load step: log the number of records processed."""
        log = LoggingMixin().log
        station_count = len(data.get('station', []))
        weather_count = len(data.get('weather', []))
        log.info(f"Stations: {station_count}, Weather records: {weather_count}")


    # Flow
    raw = extract()
    transformed = transform(raw)
    load(transformed)