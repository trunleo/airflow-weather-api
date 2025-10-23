"""
Custom Weather Operators for Airflow
Uses the existing weather modules for API interaction and database operations
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from typing import Dict, Any, List, Optional
from datetime import datetime, date, timedelta
import logging
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from weather.api.client import WeatherAPIClient
from weather.database.operations import WeatherDatabaseOperations
from weather.utils.transformations import transform_to_db_format

logger = logging.getLogger(__name__)


class WeatherDataFetchOperator(BaseOperator):
    """
    Custom operator to fetch weather forecast data from API
    Uses the WeatherAPIClient module
    """
    
    template_fields = ['coordinate_ids', 'start_date', 'end_date']
    
    @apply_defaults
    def __init__(
        self,
        coordinate_ids: List[int],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        days_ahead: int = 7,
        api_key_var: str = 'weather_api_key',
        api_url_var: str = 'weather_api_base_url',
        api_timeout: int = 30,
        xcom_key: str = 'weather_forecast_data',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.coordinate_ids = coordinate_ids
        self.start_date = start_date
        self.end_date = end_date
        self.days_ahead = days_ahead
        self.api_key_var = api_key_var
        self.api_url_var = api_url_var
        self.api_timeout = api_timeout
        self.xcom_key = xcom_key
    
    def execute(self, context):
        """Execute weather data fetch"""
        logger.info(f"Starting weather data fetch for coordinates: {self.coordinate_ids}")
        
        # Get API configuration
        api_key = Variable.get(self.api_key_var)
        api_url = Variable.get(self.api_url_var, 'https://api.weather.com/v1')
        
        # Initialize API client
        client = WeatherAPIClient(
            api_key=api_key,
            base_url=api_url,
            timeout=self.api_timeout
        )
        
        # Determine date range
        execution_date = context['execution_date']
        start_date = self.start_date or execution_date.date()
        end_date = self.end_date or (execution_date + timedelta(days=self.days_ahead)).date()
        
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Fetch data for all coordinates
        all_forecast_data = []
        successful_coords = []
        failed_coords = []
        
        for coord_id in self.coordinate_ids:
            try:
                logger.info(f"Fetching data for coordinate {coord_id}")
                forecast_data = client.get_forecast_data(
                    coordinate_id=coord_id,
                    start_date=start_date,
                    end_date=end_date
                )
                
                # Transform data to database format
                transformed_data = [transform_to_db_format(record) for record in forecast_data]
                all_forecast_data.extend(transformed_data)
                successful_coords.append(coord_id)
                
                logger.info(f"Successfully fetched {len(forecast_data)} records for coordinate {coord_id}")
                
            except Exception as e:
                logger.error(f"Failed to fetch data for coordinate {coord_id}: {str(e)}")
                failed_coords.append(coord_id)
                # Continue with other coordinates
                continue
        
        # Log summary
        logger.info(f"Fetch completed: {len(successful_coords)} successful, {len(failed_coords)} failed")
        logger.info(f"Total records fetched: {len(all_forecast_data)}")
        
        if failed_coords:
            logger.warning(f"Failed coordinates: {failed_coords}")
        
        # Store in XCom for next tasks
        context['task_instance'].xcom_push(key=self.xcom_key, value=all_forecast_data)
        context['task_instance'].xcom_push(key='fetch_summary', value={
            'total_records': len(all_forecast_data),
            'successful_coordinates': successful_coords,
            'failed_coordinates': failed_coords,
            'fetch_date_range': f"{start_date} to {end_date}"
        })
        
        return len(all_forecast_data)


class WeatherDataStoreOperator(BaseOperator):
    """
    Custom operator to store weather data in database
    Uses the WeatherDatabaseOperations module
    """
    
    template_fields = ['connection_string_var', 'xcom_key']
    
    @apply_defaults
    def __init__(
        self,
        connection_string_var: str = 'weather_db_connection_string',
        xcom_key: str = 'weather_forecast_data',
        source_task_id: str = 'fetch_weather_data',
        create_tables: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.connection_string_var = connection_string_var
        self.xcom_key = xcom_key
        self.source_task_id = source_task_id
        self.create_tables = create_tables
    
    def execute(self, context):
        """Execute weather data storage"""
        logger.info("Starting weather data storage")
        
        # Get forecast data from previous task
        forecast_data = context['task_instance'].xcom_pull(
            task_ids=self.source_task_id,
            key=self.xcom_key
        )
        
        if not forecast_data:
            logger.warning("No forecast data found in XCom")
            return 0
        
        logger.info(f"Found {len(forecast_data)} records to store")
        
        # Get database connection
        connection_string = Variable.get(self.connection_string_var)
        
        # Initialize database operations
        db_ops = WeatherDatabaseOperations(connection_string)
        
        # Create tables if needed
        if self.create_tables:
            try:
                db_ops.create_tables()
                logger.info("Database tables verified/created")
            except Exception as e:
                logger.warning(f"Table creation check failed: {str(e)}")
        
        # Store forecast data
        try:
            rows_affected = db_ops.upsert_forecast_data(forecast_data)
            logger.info(f"Successfully stored {rows_affected} forecast records")
            
            # Store storage summary in XCom
            context['task_instance'].xcom_push(key='storage_summary', value={
                'records_processed': len(forecast_data),
                'rows_affected': rows_affected,
                'storage_timestamp': datetime.utcnow().isoformat()
            })
            
            return rows_affected
            
        except Exception as e:
            logger.error(f"Failed to store forecast data: {str(e)}")
            raise


class WeatherDataValidationOperator(BaseOperator):
    """
    Custom operator to validate stored weather data
    Uses the WeatherDatabaseOperations module
    """
    
    template_fields = ['connection_string_var']
    
    @apply_defaults
    def __init__(
        self,
        connection_string_var: str = 'weather_db_connection_string',
        min_records_threshold: int = 100,
        max_missing_temp_percentage: float = 5.0,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.connection_string_var = connection_string_var
        self.min_records_threshold = min_records_threshold
        self.max_missing_temp_percentage = max_missing_temp_percentage
    
    def execute(self, context):
        """Execute data validation"""
        logger.info("Starting weather data validation")
        
        # Get database connection
        connection_string = Variable.get(self.connection_string_var)
        
        # Initialize database operations
        db_ops = WeatherDatabaseOperations(connection_string)
        
        # Run validation
        try:
            validation_results = db_ops.validate_recent_data()
            
            # Check validation criteria
            recent_count = validation_results['recent_records_count']
            missing_temp_count = validation_results['missing_temperature_count']
            quality_score = validation_results['data_quality_score']
            
            # Log results
            logger.info(f"Validation results: {validation_results}")
            
            # Check thresholds
            if recent_count < self.min_records_threshold:
                raise ValueError(f"Insufficient recent data: {recent_count} < {self.min_records_threshold}")
            
            missing_percentage = (missing_temp_count / recent_count) * 100
            if missing_percentage > self.max_missing_temp_percentage:
                raise ValueError(f"Too many missing temperatures: {missing_percentage:.1f}% > {self.max_missing_temp_percentage}%")
            
            logger.info(f"Data validation passed - Quality score: {quality_score:.1f}%")
            
            # Store validation results in XCom
            context['task_instance'].xcom_push(key='validation_results', value=validation_results)
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Data validation failed: {str(e)}")
            raise


class WeatherDataCleanupOperator(BaseOperator):
    """
    Custom operator to cleanup old weather data
    Uses the WeatherDatabaseOperations module
    """
    
    template_fields = ['connection_string_var', 'days_to_keep']
    
    @apply_defaults
    def __init__(
        self,
        connection_string_var: str = 'weather_db_connection_string',
        days_to_keep: int = 30,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.connection_string_var = connection_string_var
        self.days_to_keep = days_to_keep
    
    def execute(self, context):
        """Execute data cleanup"""
        logger.info(f"Starting weather data cleanup - keeping {self.days_to_keep} days")
        
        # Get database connection
        connection_string = Variable.get(self.connection_string_var)
        
        # Initialize database operations
        db_ops = WeatherDatabaseOperations(connection_string)
        
        # Perform cleanup
        try:
            deleted_count = db_ops.delete_old_forecasts(days_to_keep=self.days_to_keep)
            
            logger.info(f"Cleanup completed - deleted {deleted_count} old records")
            
            # Store cleanup summary in XCom
            context['task_instance'].xcom_push(key='cleanup_summary', value={
                'deleted_records': deleted_count,
                'retention_days': self.days_to_keep,
                'cleanup_timestamp': datetime.utcnow().isoformat()
            })
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Data cleanup failed: {str(e)}")
            raise


class WeatherHealthCheckOperator(BaseOperator):
    """
    Custom operator to check weather API health
    Uses the WeatherAPIClient module
    """
    
    template_fields = ['api_key_var', 'api_url_var']
    
    @apply_defaults
    def __init__(
        self,
        api_key_var: str = 'weather_api_key',
        api_url_var: str = 'weather_api_base_url',
        api_timeout: int = 30,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.api_key_var = api_key_var
        self.api_url_var = api_url_var
        self.api_timeout = api_timeout
    
    def execute(self, context):
        """Execute API health check"""
        logger.info("Starting weather API health check")
        
        # Get API configuration
        api_key = Variable.get(self.api_key_var)
        api_url = Variable.get(self.api_url_var, 'https://api.weather.com/v1')
        
        # Initialize API client
        client = WeatherAPIClient(
            api_key=api_key,
            base_url=api_url,
            timeout=self.api_timeout
        )
        
        # Perform health check
        try:
            is_healthy = client.health_check()
            
            if not is_healthy:
                raise ValueError("Weather API health check failed")
            
            logger.info("Weather API health check passed")
            
            # Store health check result in XCom
            context['task_instance'].xcom_push(key='health_check_result', value={
                'status': 'healthy',
                'api_url': api_url,
                'check_timestamp': datetime.utcnow().isoformat()
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Weather API health check failed: {str(e)}")
            raise
