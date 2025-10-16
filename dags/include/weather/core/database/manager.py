"""PostgreSQL Database Manager for Weather Data.

This module provides a comprehensive database manager class for handling
weather data operations with PostgreSQL using Airflow's PostgresHook.
"""

from typing import Any, Optional, Union
from datetime import datetime, date, timedelta
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
import pandas as pd


logger = logging.getLogger(__name__)


class WeatherDBManager:
    """PostgreSQL database manager for weather data operations.
    
    This class provides methods for CRUD operations, data retrieval,
    and weather-specific database operations using Airflow's PostgresHook.
    
    Attributes:
        conn_id: Airflow PostgreSQL connection ID.
        hook: PostgresHook instance for database operations.
    """
    
    def __init__(self, postgres_conn_id: str = "WEATHER_POSTGRES_CONN") -> None:
        """Initialize the database manager.
        
        Args:
            postgres_conn_id: Airflow PostgreSQL connection ID.
        """
        self.conn_id = postgres_conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
        logger.info(f"Initialized WeatherDBManager with connection: {self.conn_id}")
    
    def execute_query(
        self, 
        query: str, 
        parameters: Optional[tuple] = None,
        autocommit: bool = False
    ) -> None:
        """Execute a SQL query without returning results.
        
        Args:
            query: SQL query string.
            parameters: Query parameters tuple.
            autocommit: Whether to autocommit the transaction.
            
        Raises:
            AirflowException: If query execution fails.
        """
        try:
            self.hook.run(query, parameters=parameters, autocommit=autocommit)
            logger.info("Query executed successfully")
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise AirflowException(f"Database query execution failed: {e}")
    
    def fetch_records(
        self, 
        query: str, 
        parameters: Optional[tuple] = None
    ) -> list[tuple]:
        """Fetch records from database.
        
        Args:
            query: SQL query string.
            parameters: Query parameters tuple.
            
        Returns:
            List of records as tuples.
            
        Raises:
            AirflowException: If query execution fails.
        """
        try:
            records = self.hook.get_records(query, parameters=parameters)
            logger.info(f"Fetched {len(records)} records")
            return records
        except Exception as e:
            logger.error(f"Failed to fetch records: {e}")
            raise AirflowException(f"Database query failed: {e}")
    
    def fetch_dataframe(
        self, 
        query: str, 
        parameters: Optional[tuple] = None
    ) -> pd.DataFrame:
        """Fetch records as pandas DataFrame.
        
        Args:
            query: SQL query string.
            parameters: Query parameters tuple.
            
        Returns:
            Pandas DataFrame with query results.
            
        Raises:
            AirflowException: If query execution fails.
        """
        try:
            df = self.hook.get_pandas_df(query, parameters=parameters)
            logger.info(f"Fetched DataFrame with shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Failed to fetch DataFrame: {e}")
            raise AirflowException(f"Database query failed: {e}")
    
    def get_weather_current(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
        provinces: Optional[list[str]] = None
    ) -> list[dict[str, Any]]:
        """Get current weather data for specified date range.
        
        Args:
            start_date: Start date (YYYY-MM-DD or date object).
            end_date: End date (YYYY-MM-DD or date object).
            provinces: Optional list of province names to filter.
            
        Returns:
            List of current weather records as dictionaries.
        """
        query = """
            SELECT
                wc.id,
                wc.date,
                wc.time,
                wc.tc,
                wc.rh,
                wc.rain,
                wc.ws10m,
                wc.wd10m,
                wc.ps,
                wc.cc,
                s.latitude,
                s.longitude,
                s.name as station_name,
                p.name as province_name,
                wc.created_at,
                wc.updated_at
            FROM weather_current wc
            JOIN station s ON wc.station_id = s.id
            LEFT JOIN province p ON s.province_id = p.id
            WHERE wc.date BETWEEN %s AND %s
        """
        
        parameters = [start_date, end_date]
        
        if provinces:
            placeholders = ', '.join(['%s'] * len(provinces))
            query += f" AND p.name IN ({placeholders})"
            parameters.extend(provinces)
        
        query += " ORDER BY wc.date, wc.time, p.name"
        
        records = self.fetch_records(query, tuple(parameters))
        
        columns = [
            "id", "date", "time", "tc", "rh", "rain", "ws10m", "wd10m",
            "ps", "cc", "latitude", "longitude", "station_name", "province_name",
            "created_at", "updated_at"
        ]
        
        return [dict(zip(columns, row, strict=False)) for row in records]
    
    def get_provinces(self) -> list[dict[str, Any]]:
        """Get all provinces with their coordinates.
        
        Returns:
            List of province records.
        """
        query = """
            SELECT
                p.id,
                p.name,
                c.latitude,
                c.longitude,
                r.name as region_name
            FROM province p
            JOIN coordinates c ON p.coordinate_id = c.id
            LEFT JOIN region r ON p.region_id = r.id
            ORDER BY p.name
        """
        
        records = self.fetch_records(query)
        columns = ["id", "name", "latitude", "longitude", "region_name"]
        
        return [dict(zip(columns, row, strict=False)) for row in records]
    
    def get_stations(self) -> list[dict[str, Any]]:
        """Get all weather stations.
        
        Returns:
            List of station records.
        """
        query = """
            SELECT
                id,
                name,
                latitude,
                longitude,
                name_th as station_name_th,
                name_en as station_name_en
            FROM station
            ORDER BY name
        """
        
        records = self.fetch_records(query)
        columns = ["id", "name", "latitude", "longitude", "name_th", "name_en"]
        
        return [dict(zip(columns, row, strict=False)) for row in records]
    
    def get_coordinates(self) -> list[dict[str, Any]]:
        """Get all coordinates.
        
        Returns:
            List of coordinate records.
        """
        query = """
            SELECT
                id,
                latitude,
                longitude
            FROM coordinate
            ORDER BY id
        """
        
        records = self.fetch_records(query)
        columns = ["id", "latitude", "longitude"]
        
        return [dict(zip(columns, row, strict=False)) for row in records]

