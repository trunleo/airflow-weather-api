# Weather Forecast Data Ingestion DAG

**File**: `weather_forecast_dag.py`  
**Path**: `./dags/weather_forecast_dag.py`  

## Overview

This Airflow DAG fetches weather forecast data from an external API and stores it in a PostgreSQL database. The pipeline includes data validation, quality checks, and automatic cleanup of old data.

## Features

- 🌤️ **Weather Data Ingestion**: Fetches comprehensive weather forecast data
- 🔄 **Automated Processing**: Runs every 6 hours with retry logic
- 📊 **Data Quality Validation**: Built-in data quality checks and monitoring
- 🗄️ **Database Storage**: PostgreSQL with optimized schema and indexes
- 🧹 **Data Cleanup**: Automatic removal of old data based on retention policy
- 📈 **Monitoring**: Data quality metrics and validation reports

## DAG Structure

```
weather_forecast_ingestion
├── fetch_weather_data          # Fetch data from weather API
├── store_forecast_data         # Store data in PostgreSQL database
├── validate_data_quality       # Validate data quality and completeness
└── cleanup_old_data           # Remove old data based on retention policy
```

## Data Schema

The DAG processes weather forecast data with the following structure:

| Column | Type | Description |
|--------|------|-------------|
| `id` | int (PK) | Forecast record identifier |
| `coordinate_id` | int (FK) | Reference to coordinate |
| `date` | date | Forecast date |
| `hour` | int | Forecast hour (0–23) |
| `tc` | float | Temperature (°C) |
| `rh` | float | Relative humidity (%) |
| `slp` | float | Sea-level pressure (hPa) |
| `rain` | float | Rainfall (mm) |
| `ws_*` | float | Wind speed at different pressure levels |
| `wd_*` | float | Wind direction at different pressure levels |
| `cloudlow/med/high` | float | Cloud cover at different levels (%) |
| `cond` | int | Weather condition code |
| `wdfn` | varchar(50) | Wind direction full name |
| `created_at` | datetime | Record creation time |
| `updated_at` | datetime | Record update time |

## Setup Instructions

### 1. Database Setup

Run the database schema to create required tables:

```sql
-- Execute the schema file
\i sql/migrations/weather_schema.sql
```

### 2. Configuration Setup

Run the configuration setup script:

```python
python scripts/setup_weather_config.py
```

### 3. Update Airflow Variables

Go to Airflow UI > Admin > Variables and update:

| Variable | Description | Example |
|----------|-------------|---------|
| `weather_api_key` | Your weather API key | `your_api_key_here` |
| `weather_api_base_url` | Weather API base URL | `https://api.weather.com/v1` |
| `weather_db_connection_id` | Database connection ID | `weather_postgres` |
| `coordinates_to_fetch` | Coordinate IDs to process | `1,2,3,4,5` |
| `data_retention_days` | Data retention period | `30` |

### 4. Database Connection

Update the `weather_postgres` connection in Airflow UI > Admin > Connections:

```
Connection Id: weather_postgres
Connection Type: Postgres
Host: your_db_host
Schema: weather_db
Login: your_username
Password: your_password
Port: 5432
```

## Usage

### Manual Trigger

1. Go to Airflow UI > DAGs
2. Find `weather_forecast_ingestion`
3. Click the trigger button to run manually

### Scheduled Execution

The DAG runs automatically every 6 hours:
- `00:00`, `06:00`, `12:00`, `18:00` UTC

### Monitoring

Check the following for monitoring:

1. **Task Logs**: View individual task execution logs
2. **Data Quality View**: Query `forecast_data_quality` view
3. **Latest Data**: Query `latest_forecasts` view

```sql
-- Check recent data quality
SELECT * FROM forecast_data_quality 
ORDER BY ingestion_date DESC 
LIMIT 7;

-- Check latest forecasts
SELECT * FROM latest_forecasts 
WHERE date >= CURRENT_DATE 
LIMIT 100;
```

## Configuration Options

### API Configuration

```python
# Airflow Variables
weather_api_key: str           # Required: API authentication key
weather_api_base_url: str      # API endpoint base URL
weather_api_timeout: int       # Request timeout in seconds (default: 30)
```

### Data Processing

```python
coordinates_to_fetch: str      # Comma-separated coordinate IDs
data_retention_days: int       # Days to keep historical data (default: 30)
```

### Quality Thresholds

```python
min_data_quality_score: int    # Minimum acceptable quality score (default: 80)
max_missing_data_percentage: int # Maximum missing data threshold (default: 20)
```

## Error Handling

The DAG includes comprehensive error handling:

- **API Failures**: Retries with exponential backoff
- **Database Errors**: Transaction rollback and error logging
- **Data Quality Issues**: Warning logs and quality score calculation
- **Partial Failures**: Continues processing other coordinates if one fails

## Data Quality Checks

The validation task performs:

1. **Completeness Check**: Ensures recent data exists
2. **Missing Data Detection**: Identifies missing critical fields
3. **Quality Score Calculation**: Computes overall data quality percentage
4. **Threshold Validation**: Alerts if quality falls below acceptable levels

## Troubleshooting

### Common Issues

1. **API Key Invalid**
   - Check `weather_api_key` variable
   - Verify API key is active and has proper permissions

2. **Database Connection Failed**
   - Verify `weather_postgres` connection settings
   - Check database server accessibility
   - Ensure proper credentials

3. **No Recent Data**
   - Check API response format
   - Verify coordinate IDs exist
   - Review API rate limits

4. **Data Quality Low**
   - Check API data completeness
   - Review transformation logic
   - Verify coordinate data validity

### Debug Commands

```bash
# Test DAG syntax
python dags/weather_forecast_dag.py

# Validate DAG structure
python scripts/airflow/validate_dags.py dags/weather_forecast_dag.py

# Test database connection
airflow connections test weather_postgres

# Test API endpoint manually
curl -H "Authorization: Bearer YOUR_API_KEY" \
     "https://api.weather.com/v1/forecast?coordinate_id=1"
```

## Performance Optimization

### Database Indexes

The schema includes optimized indexes:
- `(coordinate_id, date, hour)` for unique constraints
- `(date)` for date-based queries
- `(created_at)` for cleanup operations

### Batch Processing

- Uses bulk insert with `execute_values`
- Processes multiple coordinates in parallel
- Implements upsert to handle duplicates

### Memory Management

- Processes data in chunks
- Uses XCom for inter-task communication
- Cleans up old data automatically

## Security Considerations

### Production Deployment

1. **Secrets Management**
   - Use AWS Secrets Manager or similar
   - Avoid storing API keys in Variables for production

2. **Database Security**
   - Use connection pooling
   - Implement proper SSL/TLS
   - Regular security updates

3. **API Security**
   - Implement rate limiting
   - Monitor API usage
   - Use API key rotation

## Extensions

### Adding New Data Sources

1. Create new API client class
2. Add transformation logic
3. Update database schema if needed
4. Create new DAG or extend existing

### Custom Validation Rules

```python
def custom_validation(**context):
    # Add your validation logic here
    pass

# Add to DAG
custom_validate_task = PythonOperator(
    task_id='custom_validation',
    python_callable=custom_validation,
    dag=dag
)
```

### Notifications

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

slack_alert = SlackWebhookOperator(
    task_id='slack_notification',
    http_conn_id='slack_default',
    message='Weather data processing completed',
    dag=dag
)
```

## Support

For issues and questions:
1. Check Airflow logs for detailed error messages
2. Review configuration variables and connections
3. Verify database schema and data
4. Test API connectivity manually

---

**Last Updated**: October 2025  
**Version**: 1.0.0  
**Maintainer**: Data Team