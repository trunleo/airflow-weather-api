"""Weather current ETL functions."""


import json, logging
import pendulum
import os, requests
from operators.postgres_insert import DbInsertOperator

# from utils.env_loader import load_env
# load_env()

logger = logging.getLogger(__name__)

API_URL     = os.getenv("FORECAST_API_URL", "http://data.tmd.go.th/api/WeatherToday/V2/")
FIELDS      = os.getenv("FORECAST_FIELDS", "tc,rh,slp,rain,ws10m,wd10m,ws925,wd925,ws850,wd850,ws700,wd700,ws500,wd500,ws200,wd200,cloudlow,cloudmed,cloudhigh,cond")


def fetch_raw():
    uid = os.getenv("TMD_UID")
    ukey = os.getenv("TMD_UKEY")
    r = requests.get(API_URL, params={"uid": uid, "ukey": ukey}, timeout=300)
    r.raise_for_status()
    return r.text, (r.headers.get("Content-Type","").lower())

def parse_rows(raw_and_ct):
    raw, content_type = raw_and_ct
    from xml.etree import ElementTree as ET
    def _clean(s): return (s or "").strip()
    def _num(s):
        try:
            if s is None: return None
            s = s.strip()
            return float(s) if s else None
        except Exception:
            return None

    rows = []
    if "json" in content_type:
        try:
            payload = json.loads(raw) if raw else {}
        except Exception:
            payload = {}
        candidates = []
        if isinstance(payload, dict):
            for k in ("Stations","stations","data"):
                if isinstance(payload.get(k), list):
                    candidates = payload[k]; break
        for st in candidates or []:
            if not isinstance(st, dict): continue
            province = _clean(str(st.get("Province","")))
            name_th  = _clean(str(st.get("StationNameThai","")))
            obs = st.get("Observation") or st.get("Observations") or []
            if isinstance(obs, dict): obs = [obs]
            if not isinstance(obs, list): obs = []
            for o in obs:
                rows.append({
                    "station_name_th": name_th,
                    "province": province,
                    "date": _clean(str(o.get("DateTime",""))),
                    "slp": _num(o.get("MeanSeaLevelPressure")),
                    "tc": _num(o.get("Temperature")),
                    "max_tc": _num(o.get("MaxTemperature")),
                    "min_tc": _num(o.get("MinTemperature")),
                    "rh": _num(o.get("RelativeHumidity")),
                    "wd10m": _num(o.get("WindDirection")),
                    "ws10m": _num(o.get("WindSpeed")),
                    "rain": _num(o.get("Rainfall")),
                })
    else:
        root = ET.fromstring(raw)
        for st in root.iterfind(".//Stations/Station"):
            province = _clean(st.findtext("Province"))
            name_th  = _clean(st.findtext("StationNameThai"))
            for o in st.findall("Observation"):
                rows.append({
                    "station_name_th": name_th,
                    "province": province,
                    "date": _clean(o.findtext("DateTime")),
                    "slp": _num(o.findtext("MeanSeaLevelPressure")),
                    "tc": _num(o.findtext("Temperature")),
                    "max_tc": _num(o.findtext("MaxTemperature")),
                    "min_tc": _num(o.findtext("MinTemperature")),
                    "rh": _num(o.findtext("RelativeHumidity")),
                    "wd10m": _num(o.findtext("WindDirection")),
                    "ws10m": _num(o.findtext("WindSpeed")),
                    "rain": _num(o.findtext("Rainfall")),
                })
    return rows


def etl_weather_current(run_date: str, **kwargs: dict) -> None:
    """Extract, transform, and load current weather data.

    Args:
        run_date: Execution date in YYYY-MM-DD format.
        **kwargs: Additional Airflow context parameters.
    """

    # 1) Extract
    print(f"ETL current weather for date: {run_date}")
    raw_and_ct = fetch_raw()
    # 2) Transform
    rows = parse_rows(raw_and_ct) or []
    logger.info("Fetched %d rows for run_date=%s", len(rows), run_date)
    if not rows:
        logger.info("No records. Exiting gracefully.")
        return 0

    insert = DbInsertOperator(
        task_id="insert_weather_current",
        postgres_conn_id="weather_pg",
        table="public.weather_current",
        # Insert-only, no upsert. Toggle on_conflict to "DO NOTHING" if you want to skip dupes.
        on_conflict="DO NOTHING",  # or None to raise on constraint violations
        page_size=2000,
        mapping={
            "station_name_th": "station_name_th",
            "province": "province",
            "observation_time": "date",   # API DateTime (ISO8601 with TZ)
            "slp": "slp",
            "tc": "tc",
            "max_tc": "max_tc",
            "min_tc": "min_tc",
            "rh": "rh",
            "wd10m": "wd10m",
            "ws10m": "ws10m",
            "rain": "rain",
        },
    )

    insert.expand(records=parse_rows(fetch_raw()))  

    print(f"ETL current weather for date: {run_date}")