# Weather Forecast DAGs Documentation

## Tổng quan

Tôi đã tạo hai Airflow DAGs sử dụng các module weather mà bạn đã định nghĩa:

1. **Weather Simplified DAG** (`weather_simplified_dag.py`) - DAG đơn giản sử dụng PythonOperator
2. **Weather Custom Operators DAG** (`weather_forecast_custom_dag.py`) - DAG sử dụng custom operators

## Kiến trúc Module

### Modules được sử dụng:

- **`src/weather/api/client.py`** - `WeatherAPIClient` class để tương tác với Weather API
- **`src/weather/database/operations.py`** - `WeatherDatabaseOperations` class để xử lý database operations
- **`src/weather/utils/transformations.py`** - Các hàm transformation để xử lý dữ liệu
- **`src/weather/api/models.py`** - Data models cho API response
- **`plugins/operators/weather_operator.py`** - Custom operators đã tạo

## 1. Weather Simplified DAG

### Đặc điểm:
- Sử dụng `PythonOperator` tiêu chuẩn
- Đơn giản, dễ hiểu và maintain
- Combine nhiều operations trong ít tasks

### Tasks:
1. **`check_api_health`** - Kiểm tra API health status
2. **`fetch_and_store_weather_data`** - Fetch data từ API và store vào database
3. **`validate_weather_data`** - Validate data quality
4. **`cleanup_old_data`** - Cleanup old data theo retention policy

### Schedule: 
- Chạy mỗi 6 giờ (`0 */6 * * *`)

## 2. Weather Custom Operators DAG  

### Đặc điểm:
- Sử dụng custom operators chuyên biệt
- Modular và reusable operators
- Nhiều tasks với separation of concerns rõ ràng

### Custom Operators:
- **`WeatherDataFetchOperator`** - Fetch weather data từ API
- **`WeatherDataStoreOperator`** - Store data vào database
- **`WeatherDataValidationOperator`** - Validate data quality
- **`WeatherDataCleanupOperator`** - Cleanup old data
- **`WeatherHealthCheckOperator`** - Check API health

### Tasks:
1. **`check_api_health`** - API health check
2. **`get_coordinates_config`** - Get coordinates từ config
3. **`fetch_weather_forecast_data`** - Fetch data từ API
4. **`store_weather_data`** - Store data vào database
5. **`validate_stored_data`** - Validate data quality
6. **`cleanup_old_weather_data`** - Cleanup old data
7. **`generate_pipeline_summary`** - Generate execution summary

## Cấu hình Airflow Variables

### Required Variables:

```python
# API Configuration
weather_api_key = "your_api_key_here"
weather_api_base_url = "https://api.weather.com/v1"  # Optional, có default

# Database Configuration  
weather_db_connection_string = "postgresql://user:password@host:port/database"

# Coordinates to fetch
weather_coordinates = "1,2,3,4,5"  # Comma-separated coordinate IDs

# Optional Configuration
weather_forecast_days = "7"  # Number of days ahead to fetch
weather_data_retention_days = "30"  # Data retention period
min_data_quality_score = "80.0"  # Minimum acceptable quality score
min_recent_records = "100"  # Minimum recent records threshold
```

### Cách set variables trong Airflow:

```bash
# Via Airflow CLI
airflow variables set weather_api_key "your_api_key"
airflow variables set weather_db_connection_string "postgresql://..."

# Via Web UI: Admin -> Variables
```

## Database Schema

DAGs sử dụng schema đã được định nghĩa trong `src/weather/database/models.py`:

### Tables:
- **`coordinates`** - Coordinate definitions
- **`weather_forecasts`** - Weather forecast data

### Key Fields trong `weather_forecasts`:
- `coordinate_id`, `date`, `hour` - Composite primary key
- `tc` - Temperature (°C)
- `rh` - Relative humidity (%)
- `slp` - Sea-level pressure (hPa)
- `rain` - Rainfall (mm)
- `ws_*` - Wind speed at different levels
- `wd_*` - Wind direction at different levels
- `cloud*` - Cloud cover data

## Monitoring và Troubleshooting

### XCom Keys được sử dụng:

#### Simplified DAG:
- `processing_summary` - Fetch and store summary
- `validation_results` - Data validation results
- `cleanup_summary` - Cleanup operation results  
- `health_status` - API health status

#### Custom Operators DAG:
- `weather_forecast_data` - Raw forecast data
- `fetch_summary` - Fetch operation summary
- `storage_summary` - Storage operation summary
- `validation_results` - Validation results
- `cleanup_summary` - Cleanup summary
- `health_check_result` - API health check
- `pipeline_summary` - Complete pipeline summary

### Common Issues:

1. **API Key Issues**: Kiểm tra `weather_api_key` variable
2. **Database Connection**: Verify `weather_db_connection_string`
3. **Missing Coordinates**: Check `weather_coordinates` variable format
4. **Data Quality**: Review validation thresholds trong variables

## Ví dụ Usage

### Enable DAGs:
```bash
# Enable simplified DAG
airflow dags unpause weather_simplified_pipeline

# Enable custom operators DAG  
airflow dags unpause weather_forecast_custom_operators
```

### Manual Trigger:
```bash
airflow dags trigger weather_simplified_pipeline
airflow dags trigger weather_forecast_custom_operators
```

### Monitor Execution:
- Web UI: `http://localhost:8080`
- Logs: Check task logs trong Web UI
- XCom: View XCom data trong task instances

## Lựa chọn DAG nào?

### Chọn **Simplified DAG** khi:
- Cần solution đơn giản, dễ maintain
- Team ít experience với custom operators
- Ít requirements phức tạp

### Chọn **Custom Operators DAG** khi:
- Cần modular, reusable components
- Có requirements phức tạp về monitoring
- Muốn separation of concerns rõ ràng
- Plan tái sử dụng operators cho DAGs khác

## Extension và Customization

### Thêm coordinates mới:
```python
# Update variable
airflow variables set weather_coordinates "1,2,3,4,5,6,7"
```

### Thay đổi schedule:
```python
# Trong DAG definition
schedule_interval='0 */3 * * *'  # Every 3 hours
```

### Thêm validation rules:
- Modify `validate_weather_data()` function
- Add new validation thresholds trong variables

### Monitoring enhancements:
- Add alerting trong task failure callbacks
- Integrate với external monitoring systems
- Add custom metrics collection