# Hướng dẫn Quản lý Coordinates cho Weather DAGs

## Tổng quan

Bạn đã có hệ thống linh hoạt để quản lý coordinates cho weather data pipeline với 3 DAGs:

1. **`weather_simplified_pipeline`** - DAG chính để fetch weather data
2. **`coordinate_management`** - DAG để quản lý coordinates trong database  
3. **`weather_forecast_custom_operators`** - DAG sử dụng custom operators

## Logic Ưu tiên Coordinates

### Weather Simplified Pipeline sử dụng logic ưu tiên sau:

1. **Ưu tiên 1**: Coordinates từ Airflow Variable `weather_coordinates`
   - Nếu variable được set, sẽ chỉ fetch weather cho các coordinates này
   - Format: `"1,2,3,4,5"` (comma-separated IDs)

2. **Ưu tiên 2**: Tất cả active coordinates từ database
   - Nếu variable không được set, sẽ fetch cho TẤT CẢ coordinates có `is_active = TRUE`
   - Đây là mode "hàng ngày" bạn muốn

3. **Fallback**: Default coordinates `[1,2,3,4,5]`
   - Chỉ dùng khi cả variable và database đều thất bại

## Cách sử dụng

### 1. Setup Coordinates trong Database

#### Thêm coordinates mẫu:
```bash
# Trigger coordinate management DAG
airflow dags trigger coordinate_management --conf '{"task": "add_sample_coordinates"}'

# Hoặc chạy specific task
airflow tasks run coordinate_management add_sample_coordinates 2024-01-01
```

#### Xem tất cả coordinates:
```bash
airflow tasks run coordinate_management list_all_coordinates 2024-01-01
```

### 2. Chế độ "Hàng ngày" - Fetch tất cả active coordinates

```bash
# Đảm bảo KHÔNG có variable weather_coordinates
airflow variables delete weather_coordinates

# Chạy DAG - sẽ tự động fetch all active coordinates
airflow dags trigger weather_simplified_pipeline
```

**DAG sẽ log:**
```
INFO - Variable coordinates: Not set
INFO - Database coordinates: [1, 2, 3, 4, 5, 7]
INFO - WILL USE: [1, 2, 3, 4, 5, 7] from Database (active coordinates)
```

### 3. Chế độ "Specific List" - Fetch coordinates cụ thể

```bash
# Set specific coordinates via variable
airflow variables set weather_coordinates "1,3,5"

# Chạy DAG - chỉ fetch coordinates đã chọn
airflow dags trigger weather_simplified_pipeline
```

**DAG sẽ log:**
```
INFO - Variable coordinates: [1, 3, 5]
INFO - Database coordinates: [1, 2, 3, 4, 5, 7]
INFO - WILL USE: [1, 3, 5] from Airflow Variable 'weather_coordinates'
```

### 4. Quản lý Active/Inactive Status

#### Xem current status:
```bash
# List all coordinates với status
airflow tasks run coordinate_management list_all_coordinates 2024-01-01
```

#### Toggle status của coordinates:
```bash
# Set coordinates cần toggle
airflow variables set coordinates_to_toggle "6,7"

# Run toggle task
airflow tasks run coordinate_management toggle_coordinate_status 2024-01-01

# Xem kết quả
airflow tasks run coordinate_management list_all_coordinates 2024-01-01
```

## Sample Coordinates đã có

Coordinate Management DAG sẽ tạo các coordinates mẫu:

| ID | Location | Lat | Lon | Status |
|----|----------|-----|-----|--------|
| 1 | Hanoi | 21.0285 | 105.8542 | Active |
| 2 | Ho Chi Minh City | 10.8231 | 106.6297 | Active |
| 3 | Da Nang | 16.0678 | 108.2208 | Active |
| 4 | Hoi An | 15.8801 | 108.3380 | Active |
| 5 | Nha Trang | 12.2388 | 109.1967 | Active |
| 6 | Hai Phong | 20.8449 | 106.6881 | **Inactive** |
| 7 | Buon Ma Thuot | 14.0583 | 108.2772 | Active |

## Monitoring và Debugging

### Task mới: `check_coordinate_configuration`

Weather DAG bây giờ có task đầu tiên để check configuration:

```python
# Task này sẽ log detailed info:
=== COORDINATE CONFIGURATION ===
Variable coordinates: [1, 3, 5]
Database coordinates: [1, 2, 3, 4, 5, 7]
WILL USE: [1, 3, 5] from Airflow Variable 'weather_coordinates'
================================
```

### XCom Keys mới:

- `coordinate_config` - Configuration details
- `processing_summary` - Bao gồm `coordinate_source` và `coordinates_used`

### Workflow Complete:

```
check_coordinate_configuration >> check_api_health >> fetch_and_store_weather_data >> validate_weather_data >> cleanup_old_data
```

## Use Cases thực tế

### 1. Production Daily Run
```bash
# Xóa variable để sử dụng all active coordinates
airflow variables delete weather_coordinates

# Schedule sẽ tự động chạy mỗi 6 giờ với all active coordinates
```

### 2. Testing với subset
```bash
# Set specific coordinates cho testing
airflow variables set weather_coordinates "1,2"

# Test run
airflow dags trigger weather_simplified_pipeline
```

### 3. Temporary disable một location
```bash
# Disable coordinate 6 (Hai Phong)
airflow variables set coordinates_to_toggle "6"
airflow tasks run coordinate_management toggle_coordinate_status 2024-01-01

# Check status
airflow tasks run coordinate_management list_all_coordinates 2024-01-01
```

### 4. Emergency: chỉ fetch critical locations
```bash
# Override với emergency coordinates
airflow variables set weather_coordinates "1,2,3"  # Hanoi, HCMC, Da Nang

# Run pipeline
airflow dags trigger weather_simplified_pipeline
```

## Advanced Configuration

### Database Query cho active coordinates:
```sql
-- Xem active coordinates
SELECT id, location_name, is_active 
FROM coordinates 
WHERE is_active = TRUE 
ORDER BY id;

-- Manually update status
UPDATE coordinates 
SET is_active = FALSE 
WHERE id IN (6, 7);
```

### Airflow Variables cần thiết:

```bash
# Required
weather_api_key="your_api_key"
weather_db_connection_string="postgresql://user:pass@host:port/db"

# Optional - coordinate control
weather_coordinates="1,2,3,4,5"  # Specific list (optional)
coordinates_to_toggle="6,7"       # For toggle operation (optional)

# Other optional
weather_api_base_url="https://api.weather.com/v1"
weather_forecast_days="7"
weather_data_retention_days="30"
```

Hệ thống bây giờ rất linh hoạt và hỗ trợ đầy đủ cả use case hàng ngày (all active) và specific testing! 🎯