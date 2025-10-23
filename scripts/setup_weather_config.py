"""
File: setup_weather_config.py
Path: ./scripts/setup_weather_config.py
Description: Script to set up Airflow Variables and Connections for weather forecast DAG
"""

from airflow.models import Variable, Connection
from airflow import settings
import logging

logger = logging.getLogger(__name__)

def setup_weather_variables():
    """Set up required Airflow Variables for weather forecast DAG"""
    
    variables = {
        # Weather API Configuration
        'weather_api_key': 'your_weather_api_key_here',
        'weather_api_base_url': 'https://api.weatherservice.com/v1',
        'weather_api_timeout': '30',
        
        # Database Configuration
        'weather_db_connection_id': 'weather_postgres',
        
        # Data Processing Configuration
        'data_retention_days': '30',
        'coordinates_to_fetch': '1,2,3,4,5',  # Coordinate IDs to process
        
        # Notification Configuration (optional)
        'weather_notification_email': 'data-team@company.com',
        'weather_slack_webhook': 'https://hooks.slack.com/services/...',
        
        # Data Quality Thresholds
        'min_data_quality_score': '80',
        'max_missing_data_percentage': '20'
    }
    
    for key, value in variables.items():
        Variable.set(key, value)
        logger.info(f"✅ Set variable: {key}")

def setup_weather_connections():
    """Set up required Airflow Connections for weather forecast DAG"""
    
    session = settings.Session()
    
    try:
        # Weather PostgreSQL database connection
        weather_conn = Connection(
            conn_id='weather_postgres',
            conn_type='postgres',
            host='localhost',
            schema='weather_db',
            login='airflow',
            password='airflow',
            port=5432,
            extra={
                "sslmode": "prefer",
                "connect_timeout": 10
            }
        )
        
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == 'weather_postgres'
        ).first()
        
        if existing_conn:
            logger.info("Updating existing weather_postgres connection")
            session.delete(existing_conn)
        
        session.add(weather_conn)
        session.commit()
        logger.info("✅ Set up database connection: weather_postgres")
        
        # Optional: Weather API HTTP connection (if needed)
        api_conn = Connection(
            conn_id='weather_api_http',
            conn_type='http',
            host='api.weatherservice.com',
            schema='https',
            extra={
                "timeout": 30,
                "headers": {
                    "Content-Type": "application/json",
                    "User-Agent": "Airflow-Weather-Pipeline/1.0"
                }
            }
        )
        
        # Check if API connection already exists
        existing_api_conn = session.query(Connection).filter(
            Connection.conn_id == 'weather_api_http'
        ).first()
        
        if existing_api_conn:
            logger.info("Updating existing weather_api_http connection")
            session.delete(existing_api_conn)
        
        session.add(api_conn)
        session.commit()
        logger.info("✅ Set up API connection: weather_api_http")
        
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Error setting up connections: {e}")
        raise
    finally:
        session.close()

def verify_setup():
    """Verify that all required variables and connections are set up correctly"""
    
    required_variables = [
        'weather_api_key',
        'weather_api_base_url',
        'weather_db_connection_id',
        'coordinates_to_fetch'
    ]
    
    logger.info("🔍 Verifying Airflow Variables...")
    
    for var_key in required_variables:
        try:
            value = Variable.get(var_key)
            if value:
                logger.info(f"✅ Variable '{var_key}' is set")
            else:
                logger.warning(f"⚠️  Variable '{var_key}' is empty")
        except Exception as e:
            logger.error(f"❌ Variable '{var_key}' is missing: {e}")
    
    # Verify connections
    session = settings.Session()
    try:
        required_connections = ['weather_postgres', 'weather_api_http']
        
        logger.info("🔍 Verifying Airflow Connections...")
        
        for conn_id in required_connections:
            conn = session.query(Connection).filter(
                Connection.conn_id == conn_id
            ).first()
            
            if conn:
                logger.info(f"✅ Connection '{conn_id}' exists")
            else:
                logger.warning(f"⚠️  Connection '{conn_id}' is missing")
                
    except Exception as e:
        logger.error(f"❌ Error verifying connections: {e}")
    finally:
        session.close()

def display_configuration_guide():
    """Display configuration guide for manual setup"""
    
    guide = """
    
    🔧 Weather Forecast DAG Configuration Guide
    ==========================================
    
    1. Update API Key:
       - Go to Airflow UI > Admin > Variables
       - Update 'weather_api_key' with your actual API key
    
    2. Update Database Connection:
       - Go to Airflow UI > Admin > Connections
       - Update 'weather_postgres' connection with your database details
    
    3. Configure Coordinates:
       - Update 'coordinates_to_fetch' variable with your coordinate IDs
       - Format: "1,2,3,4,5" (comma-separated)
    
    4. Optional Configurations:
       - Adjust 'data_retention_days' (default: 30)
       - Set notification email in 'weather_notification_email'
       - Configure data quality thresholds
    
    5. Database Setup:
       - Run the SQL schema file: sql/migrations/weather_schema.sql
       - Insert your coordinate data into the coordinates table
    
    6. Test DAG:
       - Enable the 'weather_forecast_ingestion' DAG
       - Trigger manually to test the pipeline
    
    For production deployment:
    - Use secure credential management (e.g., AWS Secrets Manager)
    - Set up proper monitoring and alerting
    - Configure resource limits and scaling
    
    """
    
    print(guide)

def main():
    """Main setup function"""
    
    print("🚀 Setting up Weather Forecast DAG Configuration...")
    
    try:
        # Setup variables
        setup_weather_variables()
        
        # Setup connections
        setup_weather_connections()
        
        # Verify setup
        verify_setup()
        
        # Display guide
        display_configuration_guide()
        
        print("✅ Weather Forecast DAG setup completed successfully!")
        print("📖 Please follow the configuration guide above to complete the setup.")
        
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        print("❌ Setup failed. Please check the logs for more details.")
        raise

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()