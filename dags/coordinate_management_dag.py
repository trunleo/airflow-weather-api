"""
Coordinate Management DAG
DAG để quản lý coordinates trong database - thêm, cập nhật, và quản lý trạng thái active
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sys
import os
import logging

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from weather.database.operations import WeatherDatabaseOperations

logger = logging.getLogger(__name__)

# DAG Configuration
default_args = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Create DAG
dag = DAG(
    'coordinate_management',
    default_args=default_args,
    description='Manage coordinates in database',
    schedule_interval=None,  # Manual trigger only
    max_active_runs=1,
    catchup=False,
    tags=['weather', 'coordinates', 'management']
)

def add_sample_coordinates(**context):
    """
    Add sample coordinates to database for testing
    """
    logger.info("Adding sample coordinates to database")
    
    # Sample coordinates for Vietnam weather stations
    sample_coordinates = [
        {'id': 1, 'latitude': 21.0285, 'longitude': 105.8542, 'location_name': 'Hanoi', 'is_active': True},
        {'id': 2, 'latitude': 10.8231, 'longitude': 106.6297, 'location_name': 'Ho Chi Minh City', 'is_active': True},
        {'id': 3, 'latitude': 16.0678, 'longitude': 108.2208, 'location_name': 'Da Nang', 'is_active': True},
        {'id': 4, 'latitude': 15.8801, 'longitude': 108.3380, 'location_name': 'Hoi An', 'is_active': True},
        {'id': 5, 'latitude': 12.2388, 'longitude': 109.1967, 'location_name': 'Nha Trang', 'is_active': True},
        {'id': 6, 'latitude': 20.8449, 'longitude': 106.6881, 'location_name': 'Hai Phong', 'is_active': False},  # Inactive for testing
        {'id': 7, 'latitude': 14.0583, 'longitude': 108.2772, 'location_name': 'Buon Ma Thuot', 'is_active': True},
    ]
    
    # Get database connection
    db_connection_string = Variable.get('weather_db_connection_string')
    db_ops = WeatherDatabaseOperations(db_connection_string)
    
    try:
        # Ensure tables exist
        db_ops.create_tables()
        
        # Add coordinates using raw SQL for simplicity
        from sqlalchemy import text
        
        with db_ops.engine.connect() as conn:
            for coord in sample_coordinates:
                # Check if coordinate already exists
                check_sql = text("SELECT COUNT(*) FROM coordinates WHERE id = :coord_id")
                result = conn.execute(check_sql, coord_id=coord['id']).scalar()
                
                if result == 0:
                    # Insert new coordinate
                    insert_sql = text("""
                        INSERT INTO coordinates (id, latitude, longitude, location_name, is_active, created_at, updated_at)
                        VALUES (:id, :latitude, :longitude, :location_name, :is_active, NOW(), NOW())
                    """)
                    conn.execute(insert_sql, **coord)
                    logger.info(f"Added coordinate: {coord['location_name']} ({coord['id']})")
                else:
                    # Update existing coordinate
                    update_sql = text("""
                        UPDATE coordinates 
                        SET latitude = :latitude, longitude = :longitude, 
                            location_name = :location_name, is_active = :is_active, 
                            updated_at = NOW()
                        WHERE id = :id
                    """)
                    conn.execute(update_sql, **coord)
                    logger.info(f"Updated coordinate: {coord['location_name']} ({coord['id']})")
            
            conn.commit()
        
        logger.info(f"Successfully processed {len(sample_coordinates)} coordinates")
        
        # Return summary
        summary = {
            'coordinates_processed': len(sample_coordinates),
            'processing_time': datetime.now().isoformat()
        }
        
        context['task_instance'].xcom_push(key='add_coordinates_summary', value=summary)
        
        return summary
        
    except Exception as e:
        logger.error(f"Failed to add coordinates: {str(e)}")
        raise

def list_all_coordinates(**context):
    """
    List all coordinates in database with their status
    """
    logger.info("Listing all coordinates from database")
    
    # Get database connection
    db_connection_string = Variable.get('weather_db_connection_string')
    db_ops = WeatherDatabaseOperations(db_connection_string)
    
    try:
        from sqlalchemy import text
        
        with db_ops.engine.connect() as conn:
            # Get all coordinates
            sql = text("""
                SELECT id, latitude, longitude, location_name, is_active, 
                       created_at, updated_at
                FROM coordinates 
                ORDER BY id
            """)
            
            result = conn.execute(sql)
            coordinates = []
            
            logger.info("=== ALL COORDINATES IN DATABASE ===")
            for row in result:
                coord_info = {
                    'id': row.id,
                    'latitude': row.latitude,
                    'longitude': row.longitude,
                    'location_name': row.location_name,
                    'is_active': row.is_active,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'updated_at': row.updated_at.isoformat() if row.updated_at else None
                }
                coordinates.append(coord_info)
                
                status = "ACTIVE" if row.is_active else "INACTIVE"
                logger.info(f"ID: {row.id:2d} | {status:8s} | {row.location_name:20s} | ({row.latitude:.4f}, {row.longitude:.4f})")
            
            # Get active coordinates count
            active_count = sum(1 for coord in coordinates if coord['is_active'])
            logger.info(f"Total: {len(coordinates)} coordinates ({active_count} active, {len(coordinates) - active_count} inactive)")
            logger.info("===================================")
        
        # Store in XCom
        summary = {
            'total_coordinates': len(coordinates),
            'active_coordinates': active_count,
            'inactive_coordinates': len(coordinates) - active_count,
            'coordinates': coordinates,
            'listing_time': datetime.now().isoformat()
        }
        
        context['task_instance'].xcom_push(key='coordinates_list', value=summary)
        
        return summary
        
    except Exception as e:
        logger.error(f"Failed to list coordinates: {str(e)}")
        raise

def toggle_coordinate_status(**context):
    """
    Toggle active status of specific coordinates
    Uses 'coordinates_to_toggle' variable with comma-separated IDs
    """
    logger.info("Toggling coordinate status")
    
    # Get coordinates to toggle from variable
    try:
        toggle_ids_str = Variable.get('coordinates_to_toggle', default_var=None)
        if not toggle_ids_str:
            logger.info("No 'coordinates_to_toggle' variable found. Skipping toggle operation.")
            return {'message': 'No coordinates to toggle'}
        
        toggle_ids = [int(coord_id.strip()) for coord_id in toggle_ids_str.split(',')]
        logger.info(f"Will toggle status for coordinates: {toggle_ids}")
        
    except Exception as e:
        logger.error(f"Failed to parse coordinates_to_toggle variable: {str(e)}")
        raise
    
    # Get database connection
    db_connection_string = Variable.get('weather_db_connection_string')
    db_ops = WeatherDatabaseOperations(db_connection_string)
    
    try:
        from sqlalchemy import text
        
        with db_ops.engine.connect() as conn:
            toggled_coordinates = []
            
            for coord_id in toggle_ids:
                # Get current status
                check_sql = text("SELECT id, location_name, is_active FROM coordinates WHERE id = :coord_id")
                result = conn.execute(check_sql, coord_id=coord_id).fetchone()
                
                if result:
                    new_status = not result.is_active
                    
                    # Update status
                    update_sql = text("""
                        UPDATE coordinates 
                        SET is_active = :new_status, updated_at = NOW()
                        WHERE id = :coord_id
                    """)
                    conn.execute(update_sql, coord_id=coord_id, new_status=new_status)
                    
                    old_status_str = "ACTIVE" if result.is_active else "INACTIVE"
                    new_status_str = "ACTIVE" if new_status else "INACTIVE"
                    
                    logger.info(f"Coordinate {coord_id} ({result.location_name}): {old_status_str} → {new_status_str}")
                    
                    toggled_coordinates.append({
                        'id': coord_id,
                        'location_name': result.location_name,
                        'old_status': result.is_active,
                        'new_status': new_status
                    })
                else:
                    logger.warning(f"Coordinate {coord_id} not found in database")
            
            conn.commit()
            
            logger.info(f"Successfully toggled {len(toggled_coordinates)} coordinates")
        
        # Store summary
        summary = {
            'toggled_coordinates': toggled_coordinates,
            'total_toggled': len(toggled_coordinates),
            'toggle_time': datetime.now().isoformat()
        }
        
        context['task_instance'].xcom_push(key='toggle_summary', value=summary)
        
        return summary
        
    except Exception as e:
        logger.error(f"Failed to toggle coordinate status: {str(e)}")
        raise

# Define tasks
add_coordinates_task = PythonOperator(
    task_id='add_sample_coordinates',
    python_callable=add_sample_coordinates,
    dag=dag
)

list_coordinates_task = PythonOperator(
    task_id='list_all_coordinates',
    python_callable=list_all_coordinates,
    dag=dag
)

toggle_status_task = PythonOperator(
    task_id='toggle_coordinate_status',
    python_callable=toggle_coordinate_status,
    dag=dag
)

# Define task dependencies - can run independently
# No dependencies - all tasks can be run manually as needed