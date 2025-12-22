from __future__ import annotations
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
from hooks.postgres_hook import ForecastPostgresHook
from airflow.models import Variable
import json
import logging
import os
import time
import requests

logger = logging.getLogger(__name__)
id_filter=""

POSTGRES_CONN_ID_OUT = Variable.get("POSTGRES_CONN_ID_OUT", default_var=Variable.get("POSTGRES_CONN_ID", default_var="weather-pg"))
POSTGRES_CONN_ID_IN  = Variable.get("POSTGRES_CONN_ID_IN", default_var=POSTGRES_CONN_ID_OUT)
FORECAST_TABLE       = Variable.get("FORECAST_TABLE", default_var="public.weather_forecast")
FORECAST_SCHEDULE    = Variable.get("FORECAST_SCHEDULE", default_var="@daily")
TZ_STR               = Variable.get("FORECAST_TZ", default_var="Asia/Bangkok")  # UTC+7

FLAG_ADHOC_FORECAST = str(Variable.get("FLAG_ADHOC_FORECAST", default_var="true")).strip().lower() in {"1", "true", "yes", "y"}
ADHOC_FORECAST_CONDITION = [s.strip() for s in Variable.get("ADHOC_FORECAST_CONDITION", default_var="678, 2994, 2789, 503").split(",")] if FLAG_ADHOC_FORECAST else None

if ADHOC_FORECAST_CONDITION:
        id_list = ", ".join(str(int(x)) for x in ADHOC_FORECAST_CONDITION if x)
        id_filter = f"AND id IN ({id_list})"
# Where to read locations (lat/lon + optional metadata) from Postgres
FORECAST_LOCATIONS_SQL = Variable.get(
        "FORECAST_LOCATIONS_SQL",
        default_var=f"""
        SELECT
            id,
            latitude,
            longitude
        FROM public.coordinate
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        {id_filter}
        """
)

API_URL     = Variable.get("FORECAST_API_URL", default_var="https://data.tmd.go.th/nwpapi/v1/forecast/location/hourly/at")
FIELDS      = Variable.get("FORECAST_FIELDS", default_var="tc,rh,slp,rain,ws10m,wd10m,ws925,wd925,ws850,wd850,ws700,wd700,ws500,wd500,ws200,wd200,cloudlow,cloudmed,cloudhigh,cond")
TOTAL_HOURS = int(Variable.get("FORECAST_TOTAL_HOURS", default_var="48"))
MAX_HOURS_PER_CALL = int(Variable.get("FORECAST_MAX_HOURS_PER_CALL", default_var="12"))
REQUEST_PAUSE_SEC = float(Variable.get("FORECAST_REQUEST_PAUSE_SEC", default_var="0.2"))
MAX_RETRIES = int(Variable.get("FORECAST_MAX_RETRIES", default_var="3"))
RETRY_BACKOFF_BASE_SEC = float(Variable.get("FORECAST_RETRY_BACKOFF_BASE_SEC", default_var="1.5"))

# INSERT settings
ON_CONFLICT = Variable.get("FORECAST_ON_CONFLICT", default_var="""(coordinate_id, forecast_datetime)
DO UPDATE SET
    updated_at = EXCLUDED.updated_at,
    cloudhigh = EXCLUDED.cloudhigh,
    cloudlow = EXCLUDED.cloudlow,
    cloudmed = EXCLUDED.cloudmed,  
    cond = EXCLUDED.cond,
    rain = EXCLUDED.rain,
    rh = EXCLUDED.rh,
    slp = EXCLUDED.slp,
    tc = EXCLUDED.tc,
    wd10m = EXCLUDED.wd10m,
    wd200 = EXCLUDED.wd200,
    wd500 = EXCLUDED.wd500,
    wd700 = EXCLUDED.wd700,
    wd850 = EXCLUDED.wd850,
    wd925 = EXCLUDED.wd925,
    ws10m = EXCLUDED.ws10m,
    ws200 = EXCLUDED.ws200,
    ws500 = EXCLUDED.ws500,
    ws700 = EXCLUDED.ws700,
    ws850 = EXCLUDED.ws850,
    ws925 = EXCLUDED.ws925,
    wdfn = EXCLUDED.wdfn,
    created_at = EXCLUDED.created_at
    """)  # e.g. "(lat, lon, forecast_date) DO NOTHING" or full DO UPDATEPAGE_SIZE   = int(Variable.get("FORECAST_PAGE_SIZE", default_var="2000"))

PAGE_SIZE   = int(Variable.get("FORECAST_PAGE_SIZE", default_var="2000"))

# Auth
raw_token = Variable.get("TMD_TOKEN", default_var="").strip()
if not raw_token:
        logger.warning("Missing TMD_TOKEN Airflow Variable; runs will be skipped.")
AUTH_HEADER = raw_token if raw_token.lower().startswith("bearer ") else (f"Bearer {raw_token}" if raw_token else "")
DIR16 = (
    "N", "NNE", "NE", "ENE",
    "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW",
    "W", "WNW", "NW", "NNW"
)


def dir16_from_deg(value: Any) -> str | None:
    """
    Convert wind direction in degrees (0–360) to a 16-point compass label.

    The 16-point compass includes: 
    N, NNE, NE, ENE, E, ESE, SE, SSE, S, SSW, SW, WSW, W, WNW, NW, NNW.

    Each compass point covers 22.5 degrees (360° / 16).  
    The input is normalized into [0, 360) and mapped into the correct bin.

    Args:
        value: Wind direction in degrees (numeric or string).

    Returns:
        The 16-point compass direction (e.g., "NW", "SSE"), 
        or None if the input is invalid or cannot be parsed.
    """
    try:
        if value is None or value == "":
            return None
        x = float(value)
        # Normalize to [0, 360)
        x = (x % 360.0 + 360.0) % 360.0
        idx = int(((x + 11.25) % 360.0) // 22.5) # 16 bins
        return DIR16[idx]
    except Exception:
        return None

def build_segments(start_dt: datetime, total_hours: int, max_chunk: int) -> List[Tuple[str, str, str]]:
    """
    Split a total forecast duration into smaller API-friendly segments.

    The TMD forecast API only allows a limited number of hours per request 
    (e.g., 12 or 24). This helper function divides the total requested 
    duration (e.g., 48 hours) into smaller segments that do not cross day boundaries.

    Args:
        start_dt: The starting datetime (usually local time in UTC+7 or UTC+8).
        total_hours: Total number of forecast hours to retrieve.
        max_chunk: Maximum number of hours allowed per API call.

    Returns:
        A list of tuples (date, hour, duration), where:
          - date:  "YYYY-MM-DD"
          - hour:  starting hour (0–23)
          - duration: number of hours to request in this segment
    """
    segments: List[Tuple[str, str, str]] = []
    remaining = total_hours
    cur = start_dt
    while remaining > 0:
        chunk = min(remaining, max_chunk)
        hours_left_today = 24 - cur.hour
        chunk = min(chunk, hours_left_today)
        segments.append((cur.strftime("%Y-%m-%d"), str(cur.hour), str(chunk)))
        cur = cur + timedelta(hours=chunk)
        remaining -= chunk
    return segments


def fetch_forecast_once(lat: str, lon: str, date: str, hour: str, duration: str) -> List[Dict[str, Any]]:
    """
    Fetch a single segment of weather forecast data from the TMD API.

    This function retrieves forecast data for a specific latitude/longitude
    over a limited time window (e.g., 6 or 12 hours), as part of a multi-segment
    ETL process. It automatically handles network retries and rate-limited
    responses (HTTP 429, 5xx).

    Args:
        lat: Latitude of the target location.
        lon: Longitude of the target location.
        date: Date string in 'YYYY-MM-DD' format for the starting forecast hour.
        hour: Starting hour (0–23) in local time (UTC+7 or UTC+8 depending on TZ_STR).
        duration: Number of hours to retrieve from the start time.

    Returns:
        A list of dictionaries containing forecast data for this segment only.
        Each dictionary includes location info, timestamp, and weather variables.

    Raises:
        AirflowSkipException: If the TMD_TOKEN environment variable is missing.
        requests.HTTPError: If the API request fails permanently (non-retryable error).
    """
    if not AUTH_HEADER:
        raise AirflowSkipException("TMD_TOKEN not set; skipping run")

    params = {"lat": str(lat), "lon": str(lon), "fields": FIELDS, "date": date, "hour": hour, "duration": duration}
    headers = {"accept": "application/json", "authorization": AUTH_HEADER}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(API_URL, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json()
            out: List[Dict[str, Any]] = []
            for block in payload.get("WeatherForecasts", []):
                bloc_lat = block.get("location", {}).get("lat", lat)
                bloc_lon = block.get("location", {}).get("lon", lon)
                for f in block.get("forecasts", []):
                    row = {"lat": bloc_lat, "lon": bloc_lon, "time": f.get("time")}
                    row.update(f.get("data", {}))
                    out.append(row)
            return out
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in (429,) or (isinstance(status, int) and status >= 500):
                if attempt < MAX_RETRIES:
                    sleep_s = (RETRY_BACKOFF_BASE_SEC ** (attempt - 1))
                    logger.warning("HTTP %s (attempt %s/%s) -> retry in %.1fs", status, attempt, MAX_RETRIES, sleep_s)
                    time.sleep(sleep_s)
                    continue
            raise
        except (requests.ConnectionError, requests.Timeout):
            if attempt < MAX_RETRIES:
                sleep_s = (RETRY_BACKOFF_BASE_SEC ** (attempt - 1))
                logger.warning("Network error (attempt %s/%s) -> retry in %.1fs", attempt, MAX_RETRIES, sleep_s)
                time.sleep(sleep_s)
                continue
            raise
    return []


def fetch_window(lat: str, lon: str, start_dt: datetime, total_hours: int) -> List[Dict[str, Any]]:
    """
    Fetch the weather forecast for a specific location over a time window.

    This function divides the total requested duration (e.g., 48h) into smaller
    API-friendly segments (defined by MAX_HOURS_PER_CALL), then sequentially
    fetches each segment from the TMD API. All results are aggregated into one list.

    Args:
        lat: Latitude of the target location (stringified for API params).
        lon: Longitude of the target location.
        start_dt: Local starting datetime (UTC+7 or UTC+8 depending on TZ_STR).
        total_hours: Total number of forecast hours to fetch (e.g., 48).

    Returns:
        A list of dictionaries containing forecast data for all segments.
    """
    rows: List[Dict[str, Any]] = []
    segments = build_segments(start_dt, total_hours, MAX_HOURS_PER_CALL)
    for idx, (date, hour, duration) in enumerate(segments, start=1):
        logger.info("Segment %s/%s: date=%s hour=%s duration=%s", idx, len(segments), date, hour, duration)
        seg = fetch_forecast_once(lat, lon, date, hour, duration)
        rows.extend(seg)
        time.sleep(REQUEST_PAUSE_SEC)
    return rows
    
def etl_weather_forecast(run_date: str, **kwargs: dict) -> None:
    """
    Main ETL pipeline for TMD weather forecast data.

    This function is designed to be executed as an Airflow task.
    It performs the following steps:
      1. Determine the current local time (based on TZ_STR).
      2. Read coordinate data (lat/lon) from Postgres.
      3. For each coordinate, call the TMD forecast API, transform the results,
         and enrich them with metadata (coordinate_id, direction, timestamps).
      4. Insert all fetched forecast records back into the Postgres database.

    Args:
        run_date: The execution date (YYYY-MM-DD), provided by Airflow context.
        **kwargs: Additional Airflow context parameters (unused here).

    Returns:
        The number of forecast rows inserted into Postgres.
    """
    print(f"ETL weather forecast for date: {run_date}")

    # 1) Time base in UTC+7 -- Thailand timezone
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(TZ_STR)
    now_local = datetime.now(tz)
    logger.info("Start forecast ingest at %s (tz=%s) for next %sh", now_local.isoformat(), TZ_STR, TOTAL_HOURS)

    # 2) Load locations from PG
    pg_hook_in = ForecastPostgresHook(postgres_conn_id=POSTGRES_CONN_ID_IN)
    locs = pg_hook_in.load_locations(FORECAST_LOCATIONS_SQL)
    if not locs:
        raise AirflowSkipException("No locations returned from Postgres query")
    logger.info("Loaded %s locations from Postgres", len(locs))

    # 3) Fetch & merge
    all_rows: List[Dict[str, Any]] = []
    for i, loc in enumerate(locs, start=1):
        lat, lon, coordinate_id = loc.get("latitude", ""), loc.get("longitude", ""), loc.get("id")
        # extras = {k: v for k, v in loc.items() if k not in ("latitude", "longitude")}
        try:
            logger.info("[%s/%s] %s,%s", i, len(locs), lat, lon)
            rows = fetch_window(lat, lon, now_local, TOTAL_HOURS)
            print(" -> fetched %s rows", len(rows))
            for r in rows:
                # r.update(extras)  # carry id/name/coordinate_id
                if "wdfn" not in r or r["wdfn"] is None:
                    r["wdfn"] = dir16_from_deg(r.get("wd10m"))
                r["coordinate_id"] = coordinate_id
                r["created_at"] = now_local.strftime("%Y-%m-%d %H:%M:%S")
                r["updated_at"] = now_local.strftime("%Y-%m-%d %H:%M:%S")
                r["forecast_datetime"] = r["time"]
                r['observation_datetime'] = r["time"]
            all_rows.extend(rows)
            logger.info(" -> %s rows", len(rows))
        except AirflowSkipException:
            raise
        except Exception as e:
            logger.exception(" -> error for %s,%s: %s", lat, lon, e)
        finally:
            time.sleep(REQUEST_PAUSE_SEC)

    # 4) Load to PG
    pg_hook_out = ForecastPostgresHook(postgres_conn_id=POSTGRES_CONN_ID_OUT)
    count = pg_hook_out.insert_forecast(
        table=FORECAST_TABLE,
        rows=all_rows,
        on_conflict=ON_CONFLICT,
        page_size=PAGE_SIZE,
        column_name=[
            "forecast_datetime", "cloudhigh", "cloudlow", "cloudmed", "cond",
            "rain", "rh", "slp", "tc", "wd10m", "wd200", "wd500", "wd700",
            "wd850", "wd925", "ws10m", "ws200", "ws500", "ws700", "ws850",
            "ws925", "coordinate_id", "wdfn", "created_at", "updated_at"
        ],
        column_value=""" %(forecast_datetime)s, %(cloudhigh)s, %(cloudlow)s, %(cloudmed)s, %(cond)s,
          %(rain)s, %(rh)s, %(slp)s, %(tc)s, %(wd10m)s, %(wd200)s, %(wd500)s, %(wd700)s,
          %(wd850)s, %(wd925)s, %(ws10m)s, %(ws200)s, %(ws500)s, %(ws700)s, %(ws850)s,
          %(ws925)s, %(coordinate_id)s, %(wdfn)s, %(created_at)s, %(updated_at)s"""
    )
    
    logger.info("Inserted %s forecast rows into Postgres table %s", count, FORECAST_TABLE)
    # 5) Load to workaround table for adhoc forecast if enabled
    if kwargs['WORKAROUND_CURRENT_TBL']:
        workaround_tbl = kwargs['WORKAROUND_CURRENT_TBL']
        pg_hook_out = ForecastPostgresHook(postgres_conn_id=POSTGRES_CONN_ID_OUT)
        workaround_count = pg_hook_out.insert_forecast(
            table=workaround_tbl,
            rows=all_rows,
            on_conflict=ON_CONFLICT.replace("forecast_datetime", "observation_datetime"),
            page_size=PAGE_SIZE,
            column_name=[
                "observation_datetime", "cloudhigh", "cloudlow", "cloudmed", "cond",
                "rain", "rh", "slp", "tc", "wd10m", "wd200", "wd500", "wd700",
                "wd850", "wd925", "ws10m", "ws200", "ws500", "ws700", "ws850",
                "ws925", "coordinate_id", "wdfn", "created_at", "updated_at"
            ],
            column_value=""" %(observation_datetime)s, %(cloudhigh)s, %(cloudlow)s, %(cloudmed)s, %(cond)s,
            %(rain)s, %(rh)s, %(slp)s, %(tc)s, %(wd10m)s, %(wd200)s, %(wd500)s, %(wd700)s,
            %(wd850)s, %(wd925)s, %(ws10m)s, %(ws200)s, %(ws500)s, %(ws700)s, %(ws850)s,
            %(ws925)s, %(coordinate_id)s, %(wdfn)s, %(created_at)s, %(updated_at)s"""
        )
        logger.info("Loaded workaround current forecast for conditions: %s", workaround_count)

    return count