"""Weather alert detection service."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook


@dataclass
class AlertThresholds:
    """Weather alert threshold configuration.

    Attributes:
        heavy_rain_min: Minimum rainfall for heavy rain alert (mm).
        heavy_rain_max: Maximum rainfall for heavy rain alert (mm).
        dry_spell_min_days: Minimum days without rain for dry spell alert.
        dry_spell_max_days: Maximum days without rain for dry spell alert.
        heat_wave_temp: Temperature threshold for heat wave (°C).
        heat_wave_days: Consecutive days for heat wave alert.
        strong_wind_min: Minimum wind speed for strong wind alert (m/s).
        strong_wind_max: Maximum wind speed for strong wind alert (m/s).
        tropical_storm_min: Minimum wind speed for tropical storm (m/s).
        tropical_storm_max: Maximum wind speed for tropical storm (m/s).
        low_humidity_min: Minimum humidity for low humidity alert (%).
        low_humidity_max: Maximum humidity for low humidity alert (%).
        high_humidity_min: Minimum humidity for high humidity alert (%).
        high_humidity_max: Maximum humidity for high humidity alert (%).
    """

    heavy_rain_min: float = 35.0
    heavy_rain_max: float = 90.0
    dry_spell_min_days: int = 7
    dry_spell_max_days: int = 15
    heat_wave_temp: float = 35.0
    heat_wave_days: int = 2
    strong_wind_min: float = 10.8
    strong_wind_max: float = 16.9
    tropical_storm_min: float = 24.7
    tropical_storm_max: float = 32.5
    low_humidity_min: float = 20.0
    low_humidity_max: float = 40.0
    high_humidity_min: float = 80.0
    high_humidity_max: float = 95.0


class WeatherAlertService:
    """Professional weather alert detection service.

    This service handles weather data retrieval and alert detection across
    multiple weather phenomena.

    Attributes:
        postgres_conn_id: Airflow Postgres connection ID.
        thresholds: Alert threshold configuration.
    """

    def __init__(
        self,
        postgres_conn_id: str = "WEATHER_POSTGRES_CONN",
        thresholds: AlertThresholds | None = None,
    ) -> None:
        """Initialize weather alert service.

        Args:
            postgres_conn_id: Airflow Postgres connection ID.
            thresholds: Custom alert thresholds (uses defaults if None).
        """
        self.postgres_conn_id = postgres_conn_id
        self.thresholds = thresholds or AlertThresholds()

    def _get_forecast_data(
        self,
        run_date: str,
        forecast_days: int = 3,
    ) -> list[dict[str, Any]]:
        """Get forecast data using PostgresHook.

        Args:
            run_date: Date to fetch forecast data (YYYY-MM-DD).
            forecast_days: Number of days to forecast ahead.

        Returns:
            list[dict[str, Any]]: List of forecast records.

        Raises:
            Exception: If database query fails.
        """
        from datetime import datetime

        query = """
            SELECT
                f.id,
                f.date,
                f.hour,
                f.tc,
                f.rh,
                f.rain,
                f.ws10m,
                f.wd10m,
                c.latitude,
                c.longitude,
                p.name as province_name,
                r.name as region_name
            FROM forecast f
            JOIN coordinates c ON f.coordinates_id = c.id
            LEFT JOIN province p ON c.id = p.coordinate_id
            LEFT JOIN region r ON p.region_id = r.id
            WHERE f.date BETWEEN %s AND %s
            ORDER BY f.date, f.hour, p.name
        """

        start_date = datetime.strptime(run_date, "%Y-%m-%d").date()
        end_date = start_date + timedelta(days=forecast_days)

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        records = hook.get_records(query, parameters=(start_date, end_date))

        columns = [
            "id",
            "date",
            "hour",
            "tc",
            "rh",
            "rain",
            "ws10m",
            "wd10m",
            "latitude",
            "longitude",
            "province_name",
            "region_name",
        ]
        return [dict(zip(columns, row, strict=False)) for row in records]

    def _get_historical_rain_data(
        self,
        run_date: str,
        days_back: int = 15,
    ) -> list[dict[str, Any]]:
        """Get historical rainfall data using PostgresHook.

        Args:
            run_date: Reference date (YYYY-MM-DD).
            days_back: Number of days to look back.

        Returns:
            list[dict[str, Any]]: Historical rainfall records.

        Raises:
            Exception: If database query fails.
        """
        from datetime import datetime

        query = """
            SELECT
                wc.date,
                wc.rain,
                p.name as province_name,
                s.latitude,
                s.longitude
            FROM weather_current wc
            JOIN station s ON wc.station_id = s.id
            LEFT JOIN province p ON s.province_id = p.id
            WHERE wc.date BETWEEN %s AND %s
            ORDER BY p.name, wc.date DESC
        """

        end_date = datetime.strptime(run_date, "%Y-%m-%d").date()
        start_date = end_date - timedelta(days=days_back)

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        records = hook.get_records(query, parameters=(start_date, end_date))

        columns = ["date", "rain", "province_name", "latitude", "longitude"]
        return [dict(zip(columns, row, strict=False)) for row in records]

    def _detect_heavy_rain(
        self,
        forecast_data: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Detect heavy rainfall alerts (35-90mm).

        Args:
            forecast_data: Forecast records with rain data.

        Returns:
            list[dict[str, Any]]: List of heavy rain alerts.
        """
        alerts = []
        daily_rain: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"rain": 0.0, "province": None, "date": None},
        )

        for record in forecast_data:
            if (
                record.get("rain")
                and record.get("date")
                and record.get("province_name")
            ):
                key = f"{record['province_name']}_{record['date']}"
                daily_rain[key]["rain"] += record["rain"]
                daily_rain[key]["province"] = record["province_name"]
                daily_rain[key]["date"] = str(record["date"])
                daily_rain[key]["latitude"] = record.get("latitude")
                daily_rain[key]["longitude"] = record.get("longitude")

        for data in daily_rain.values():
            total_rain = data["rain"]
            rain_min = self.thresholds.heavy_rain_min
            rain_max = self.thresholds.heavy_rain_max
            if rain_min <= total_rain <= rain_max:
                alerts.append(
                    {
                        "alert_type": "heavy_rain",
                        "severity": "medium",
                        "province": data["province"],
                        "date": data["date"],
                        "value": round(total_rain, 2),
                        "unit": "mm",
                        "message": (
                            f"Heavy rainfall ({total_rain:.1f}mm) expected. "
                            "Risk of water logging, crop disease, and fungal "
                            "outbreaks."
                        ),
                        "latitude": data.get("latitude"),
                        "longitude": data.get("longitude"),
                    },
                )

        return alerts

    def _detect_dry_spell(
        self,
        historical_rain: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Detect dry spell alerts (7-15 days no rain).

        Args:
            historical_rain: Historical rainfall records.

        Returns:
            list[dict[str, Any]]: List of dry spell alerts.
        """
        alerts = []
        by_province: dict[str, list] = defaultdict(list)

        for record in historical_rain:
            province = record.get("province_name")
            if province:
                by_province[province].append(record)

        for province, records in by_province.items():
            records_sorted = sorted(records, key=lambda x: x["date"], reverse=True)
            consecutive_dry_days = 0

            for record in records_sorted:
                rain = record.get("rain", 0.0) or 0.0
                if rain < 0.1:  # No significant rain
                    consecutive_dry_days += 1
                else:
                    break

            min_days = self.thresholds.dry_spell_min_days
            max_days = self.thresholds.dry_spell_max_days
            if min_days <= consecutive_dry_days < max_days:
                last_record = records_sorted[0]
                alerts.append(
                    {
                        "alert_type": "dry_spell",
                        "severity": "medium",
                        "province": province,
                        "date": str(last_record["date"]),
                        "value": consecutive_dry_days,
                        "unit": "days",
                        "message": (
                            f"Prolonged dry spell - no rain for "
                            f"{consecutive_dry_days}+ days. "
                            "Soil moisture declining, drought risk increasing."
                        ),
                        "latitude": last_record.get("latitude"),
                        "longitude": last_record.get("longitude"),
                    },
                )

        return alerts

    def _detect_heat_wave(
        self,
        forecast_data: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Detect heat wave alerts (≥35°C for ≥2 consecutive days).

        Args:
            forecast_data: Forecast records with temperature data.

        Returns:
            list[dict[str, Any]]: List of heat wave alerts.
        """
        alerts = []
        by_province: dict[str, list] = defaultdict(list)

        for record in forecast_data:
            province = record.get("province_name")
            if province and record.get("tc"):
                by_province[province].append(record)

        for province, records in by_province.items():
            daily_max: dict[str, float] = defaultdict(lambda: -999.0)

            for record in records:
                date_key = str(record["date"])
                temp = record["tc"]
                if temp > daily_max[date_key]:
                    daily_max[date_key] = temp

            sorted_dates = sorted(daily_max.keys())
            consecutive_hot_days = 0
            start_date = None

            for date in sorted_dates:
                if daily_max[date] >= self.thresholds.heat_wave_temp:
                    if consecutive_hot_days == 0:
                        start_date = date
                    consecutive_hot_days += 1
                else:
                    consecutive_hot_days = 0
                    start_date = None

                if consecutive_hot_days >= self.thresholds.heat_wave_days:
                    max_temp = max(daily_max[d] for d in sorted_dates)
                    alerts.append(
                        {
                            "alert_type": "heat_wave",
                            "severity": "high",
                            "province": province,
                            "date": start_date,
                            "value": round(max_temp, 1),
                            "unit": "°C",
                            "message": (
                                f"Extreme heat above "
                                f"{self.thresholds.heat_wave_temp}°C for several "
                                "days. Crops and livestock at high risk of heat "
                                "stress."
                            ),
                        },
                    )
                    break

        return alerts

    def _detect_strong_winds(
        self,
        forecast_data: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Detect strong wind alerts (10.8-16.9 m/s).

        Args:
            forecast_data: Forecast records with wind speed data.

        Returns:
            list[dict[str, Any]]: List of strong wind alerts.
        """
        alerts = []

        for record in forecast_data:
            ws10m = record.get("ws10m")
            wind_min = self.thresholds.strong_wind_min
            wind_max = self.thresholds.strong_wind_max
            if ws10m and wind_min <= ws10m <= wind_max:
                alerts.append(
                    {
                        "alert_type": "strong_wind",
                        "severity": "medium",
                        "province": record.get("province_name"),
                        "date": str(record["date"]),
                        "value": round(ws10m, 1),
                        "unit": "m/s",
                        "message": (
                            f"Strong winds {ws10m:.1f} m/s "
                            f"(~{ws10m * 3.6:.0f} km/h). "
                            "May tilt trees, blow objects, and stress weak "
                            "structures."
                        ),
                        "latitude": record.get("latitude"),
                        "longitude": record.get("longitude"),
                    },
                )

        return alerts

    def _detect_tropical_storms(
        self,
        forecast_data: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Detect tropical storm alerts (24.7-32.5 m/s).

        Args:
            forecast_data: Forecast records with wind speed data.

        Returns:
            list[dict[str, Any]]: List of tropical storm alerts.
        """
        alerts = []

        for record in forecast_data:
            ws10m = record.get("ws10m")
            storm_min = self.thresholds.tropical_storm_min
            storm_max = self.thresholds.tropical_storm_max
            if ws10m and storm_min <= ws10m <= storm_max:
                alerts.append(
                    {
                        "alert_type": "tropical_storm",
                        "severity": "critical",
                        "province": record.get("province_name"),
                        "date": str(record["date"]),
                        "value": round(ws10m, 1),
                        "unit": "m/s",
                        "message": (
                            f"Tropical storm winds {ws10m:.1f} m/s "
                            f"(~{ws10m * 3.6:.0f} km/h). "
                            "Potential damage to trees, houses, and farmland."
                        ),
                        "latitude": record.get("latitude"),
                        "longitude": record.get("longitude"),
                    },
                )

        return alerts

    def _detect_humidity_alerts(
        self,
        forecast_data: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Detect low/high humidity alerts.

        Args:
            forecast_data: Forecast records with humidity data.

        Returns:
            list[dict[str, Any]]: List of humidity alerts.
        """
        alerts = []
        daily_humidity: dict[str, dict] = defaultdict(
            lambda: {"min": 100.0, "max": 0.0, "province": None, "date": None},
        )

        for record in forecast_data:
            rh = record.get("rh")
            if rh and record.get("province_name"):
                key = f"{record['province_name']}_{record['date']}"
                daily_humidity[key]["min"] = min(daily_humidity[key]["min"], rh)
                daily_humidity[key]["max"] = max(daily_humidity[key]["max"], rh)
                daily_humidity[key]["province"] = record["province_name"]
                daily_humidity[key]["date"] = str(record["date"])
                daily_humidity[key]["latitude"] = record.get("latitude")
                daily_humidity[key]["longitude"] = record.get("longitude")

        for data in daily_humidity.values():
            min_rh = data["min"]
            max_rh = data["max"]

            # Low humidity alert
            low_min = self.thresholds.low_humidity_min
            low_max = self.thresholds.low_humidity_max
            if low_min <= min_rh <= low_max:
                alerts.append(
                    {
                        "alert_type": "low_humidity",
                        "severity": "medium",
                        "province": data["province"],
                        "date": data["date"],
                        "value": round(min_rh, 1),
                        "unit": "%",
                        "message": (
                            f"Very dry air ({min_rh:.0f}%). "
                            "Crops may wilt, pest infestations likely."
                        ),
                        "latitude": data.get("latitude"),
                        "longitude": data.get("longitude"),
                    },
                )

            # High humidity alert
            high_min = self.thresholds.high_humidity_min
            high_max = self.thresholds.high_humidity_max
            if high_min <= max_rh <= high_max:
                alerts.append(
                    {
                        "alert_type": "high_humidity",
                        "severity": "medium",
                        "province": data["province"],
                        "date": data["date"],
                        "value": round(max_rh, 1),
                        "unit": "%",
                        "message": (
                            f"High humidity ({max_rh:.0f}%). "
                            "Conditions favor plant fungal diseases and dew "
                            "formation."
                        ),
                        "latitude": data.get("latitude"),
                        "longitude": data.get("longitude"),
                    },
                )

        return alerts

    def detect_all_alerts(
        self,
        run_date: str,
        forecast_days: int = 3,
        historical_days: int = 15,
    ) -> list[dict[str, Any]]:
        """Run all alert detection algorithms.

        Args:
            run_date: Date to run detection for (YYYY-MM-DD).
            forecast_days: Number of days to forecast ahead.
            historical_days: Number of historical days to analyze.

        Returns:
            list[dict[str, Any]]: Combined list of all detected alerts.

        Raises:
            Exception: If data retrieval or detection fails.
        """
        # Fetch weather data
        forecast_data = self._get_forecast_data(run_date, forecast_days)
        historical_rain = self._get_historical_rain_data(run_date, historical_days)

        # Run all detection algorithms
        all_alerts = []
        all_alerts.extend(self._detect_heavy_rain(forecast_data))
        all_alerts.extend(self._detect_dry_spell(historical_rain))
        all_alerts.extend(self._detect_heat_wave(forecast_data))
        all_alerts.extend(self._detect_strong_winds(forecast_data))
        all_alerts.extend(self._detect_tropical_storms(forecast_data))
        all_alerts.extend(self._detect_humidity_alerts(forecast_data))

        return all_alerts

