from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from typing import List, Dict, Any
import logging
import pandas as pd
from datetime import datetime, timedelta
from .models import Base, Coordinate, WeatherForecast

logger = logging.getLogger(__name__)

class WeatherDatabaseOperations:
    """Handle all weather database operations"""
    
    def __init__(self, connection_string: str):
        self.engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False
        )
        self.Session = sessionmaker(bind=self.engine)
    
    def create_tables(self):
        """Create all tables if they don't exist"""
        Base.metadata.create_all(self.engine)
        logger.info("Database tables created successfully")
    
    def get_active_coordinates(self) -> List[int]:
        """Get list of active coordinate IDs"""
        with self.Session() as session:
            coordinates = session.query(Coordinate.id).filter(
                Coordinate.is_active == True
            ).all()
            return [coord.id for coord in coordinates]
    
    def upsert_forecast_data(self, forecast_data: List[Dict[str, Any]]) -> int:
        """Insert or update forecast data using PostgreSQL UPSERT"""
        if not forecast_data:
            logger.warning("No forecast data to upsert")
            return 0
        
        with self.Session() as session:
            try:
                # Use PostgreSQL UPSERT (ON CONFLICT DO UPDATE)
                stmt = insert(WeatherForecast).values(forecast_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['coordinate_id', 'date', 'hour'],
                    set_={
                        'tc': stmt.excluded.tc,
                        'rh': stmt.excluded.rh,
                        'slp': stmt.excluded.slp,
                        'rain': stmt.excluded.rain,
                        'ws_10m': stmt.excluded.ws_10m,
                        'ws_925': stmt.excluded.ws_925,
                        'ws_850': stmt.excluded.ws_850,
                        'ws_700': stmt.excluded.ws_700,
                        'ws_500': stmt.excluded.ws_500,
                        'ws_200': stmt.excluded.ws_200,
                        'wd_10m': stmt.excluded.wd_10m,
                        'wd_925': stmt.excluded.wd_925,
                        'wd_850': stmt.excluded.wd_850,
                        'wd_700': stmt.excluded.wd_700,
                        'wd_500': stmt.excluded.wd_500,
                        'wd_200': stmt.excluded.wd_200,
                        'cloudlow': stmt.excluded.cloudlow,
                        'cloudmed': stmt.excluded.cloudmed,
                        'cloudhigh': stmt.excluded.cloudhigh,
                        'cond': stmt.excluded.cond,
                        'wdfn': stmt.excluded.wdfn,
                        'updated_at': stmt.excluded.updated_at
                    }
                )
                
                result = session.execute(stmt)
                session.commit()
                
                rows_affected = result.rowcount
                logger.info(f"Successfully upserted {rows_affected} forecast records")
                return rows_affected
                
            except Exception as e:
                session.rollback()
                logger.error(f"Error upserting forecast data: {str(e)}")
                raise
    
    def delete_old_forecasts(self, days_to_keep: int = 30) -> int:
        """Delete forecast records older than specified days"""
        with self.Session() as session:
            try:
                cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
                
                deleted_count = session.query(WeatherForecast).filter(
                    WeatherForecast.created_at < cutoff_date
                ).delete()
                
                session.commit()
                logger.info(f"Deleted {deleted_count} old forecast records")
                return deleted_count
                
            except Exception as e:
                session.rollback()
                logger.error(f"Error deleting old forecasts: {str(e)}")
                raise
    
    def validate_recent_data(self) -> Dict[str, Any]:
        """Validate recent data quality"""
        with self.Session() as session:
            try:
                # Check for recent data
                recent_data_count = session.query(WeatherForecast).filter(
                    WeatherForecast.created_at >= datetime.utcnow() - timedelta(hours=6)
                ).count()
                
                # Check for missing critical fields
                missing_temp_count = session.query(WeatherForecast).filter(
                    and_(
                        WeatherForecast.created_at >= datetime.utcnow() - timedelta(hours=6),
                        WeatherForecast.tc.is_(None)
                    )
                ).count()
                
                validation_results = {
                    'recent_records_count': recent_data_count,
                    'missing_temperature_count': missing_temp_count,
                    'data_quality_score': (recent_data_count - missing_temp_count) / max(recent_data_count, 1) * 100,
                    'validation_time': datetime.utcnow()
                }
                
                if recent_data_count == 0:
                    raise ValueError("No recent forecast data found")
                
                logger.info(f"Data validation completed: {validation_results}")
                return validation_results
                
            except Exception as e:
                logger.error(f"Data quality validation failed: {str(e)}")
                raise