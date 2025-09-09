from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import requests
import pandas as pd


BASE_URL = "http://data.tmd.go.th/api/WeatherToday/V2/"


with DAG(
    dag_id="weather_api_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "tmd"],
    params={   # <- default params
        "data_type": "today",   # có thể override khi trigger DAG
        "uid": "api",
        "ukey_map": {
            "today": "api12345",
            "forecast": "api123"
        }
    }
) as dag:

    @task()
    def extract(**context) -> dict:
        params = context["params"]
        data_type = params["data_type"]
        uid = params["uid"]
        ukey = params["ukey_map"][data_type]

        query_params = {
            "uid": uid,
            "ukey": ukey,
            "format": "json"
        }

        response = requests.get(BASE_URL, params=query_params, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        return response.json()

    @task()
    def transform(data: dict):
        # Get station list
        list_station = [
            {k: v for k, v in st.items() if k != 'Observation'}
            for st in (data.get('Stations', {}).get('Station', {}))
        ]
        station_df = pd.DataFrame(list_station)

        # Get weather data
        weather_data = []
        for st in (data.get('Stations', {}).get('Station', [])):
            records = {k: v for k, v in (st.get('Observation', {})).items() if k != '@attributes'}
            records.update({"StationNumber": st.get("WmoStationNumber")})
            weather_data.append(records)
        weather_df = pd.DataFrame(weather_data)

        return {
            "station": station_df.to_dict(orient="records"),
            "weather": weather_df.to_dict(orient="records"),
        }

    @task()
    def load(data: dict):
        print(f"Stations: {len(data['station'])}, Weather records: {len(data['weather'])}")

    # Flow
    raw = extract()
    transformed = transform(raw)
    load(transformed)