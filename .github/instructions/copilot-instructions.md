# AI Coding Agent Instructions for Airflow Weather Data Pipeline
---
alwaysApply: true
applyTo: "**"
---

# Python Expert - Ruff Linting Standards

You are an expert Python developer, Django and Celery. All code MUST follow Ruff linting standards and modern Python best practices.

## Core Principles

1. **Type Safety First:** Always use type hints
2. **Document Everything:** Every function needs a docstring
3. **Clean Code:** PEP 8 compliant, max 88 chars per line
4. **Modern Python:** Use Python 3.12+ features
5. **Error Handling:** Always use exception chaining

## Required Standards

### Type Annotations
```python
def process(data: dict, limit: int = 100) -> list[dict]:
    """Process data with limit."""
    return [data]
```

### Docstrings (Google Style)
```python
def fetch_data(source: str, limit: int) -> list[dict]:
    """Fetch data from source.

    Args:
        source: Data source identifier.
        limit: Maximum records to fetch.

    Returns:
        list[dict]: Retrieved records.

    Raises:
        ValueError: If source is invalid.
    """
```

### Exception Chaining
```python
try:
    result = operation()
except Exception as e:
    raise CustomError(f"Failed: {e}") from e
```

### Import Organization
```python
# 1. Standard library
import os
from datetime import datetime

# 2. Third-party
import requests
from fastapi import FastAPI

# 3. Local imports
from .utils import helper
```

### Naming Conventions
- Variables/functions: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`
- Private: `_leading_underscore`

### Line Length
- Max 88 characters
- Break long lines with parentheses
- Split long f-strings

### Complexity
- Keep functions simple (≤10 branches)
- Extract complex logic into helpers
- Use early returns to reduce nesting

## Ruff Rules to Follow

- **E/F:** PEP 8 compliance
- **I:** Sorted imports
- **D:** Docstrings required (Google style)
- **ANN:** Type hints required
- **B:** Best practices (bugbear)
- **N:** Naming conventions
- **C90:** Complexity checks
- **UP:** Modern Python syntax

## Before Committing

- [ ] All functions have type hints
- [ ] All functions have docstrings
- [ ] Lines ≤88 characters
- [ ] Imports properly sorted
- [ ] snake_case naming
- [ ] Exception chaining with `from e`
- [ ] No complexity warnings

## Auto-Check Commands

```bash
uv run ruff check          # Check linting
uv run ruff check --fix    # Auto-fix issues
uv run ruff format         # Format code
```

**Remember:** Clean, type-safe, well-documented code is mandatory, not optional.
 



## Architecture Overview

This project processes Thailand weather data using Apache Airflow with a PostgreSQL backend. The codebase follows domain-driven design with clear separation between Airflow orchestration (`dags/`) and business logic (`include/weather/`).

### Key Components
- **DAGs**: Weather data pipelines in `dags/` using TaskGroups for ETL operations
- **Weather Domain**: Core logic in `include/weather/` with database, ETL, and alert modules  
- **Reference Tables**: Thailand geographic data processors in `scripts/database/references_tbl/manual/`
- **Database Layer**: `WeatherDBManager` class abstracts PostgreSQL operations using Airflow's PostgresHook

## Critical Development Workflows

### Testing DAGs
```bash
# Run all DAG tests with uv (preferred package manager)
uv run pytest tests/unit/test_dags/ -v

# Test specific DAG
uv run pytest tests/unit/test_dags/test_wt_test_notification.py -v
```

### Testing Reference Table Processing
```bash
# Test geographic data processing
uv run pytest tests/unit/test_database/ -v
```

### Import Resolution
- DAGs import from `include.weather.*` (relative to dags directory)
- Reference table modules use relative imports: `from .func import *`
- Always import `WeatherDBManager` from `include.weather.core.database.manager`

## Project-Specific Patterns

### Database Operations
```python
# Standard pattern for DAG database connections
from include.weather.core.etl.references_tbl import create_connection
db_manager = create_connection(postgres_conn_id="ac-weather-backend")
```

### TaskGroup Structure
```python
# Group related ETL operations using TaskGroups
with TaskGroup("load_ref", tooltip="Load Reference tbl") as load_ref:
    get_ref_tbl = PythonOperator(
        task_id="get_reference_table_coordinate",
        python_callable=get_reference_table,
        op_kwargs={"db_manager": db_manager, "ref_tbl_name": "coordinate"}
    )
```

### Geographic Data Processing
- **Class-Based Architecture**: Use `TableCreator`, `MultiSourceGeocoder`, `LocationProcessor` classes
- **Fallback Strategy**: Multiple geocoding sources (Thai Community API → Nominatim → manual coordinates)
- **Backward Compatibility**: Maintain function wrappers for existing code

## Integration Points

### TMD Weather API
```python
# Pattern for fetching TMD station data
url = "http://data.tmd.go.th/api/Station/V1/"
params = {"uid": "demo", "ukey": "demokey", "format": "json"}
```

### Thai Geographic APIs
- Primary: `https://cbtthailand.dasta.or.th/webapi/getDataCommunity`
- Fallback: Nominatim with "Thailand" suffix
- Data structure: Province → District → Subdistrict hierarchy

### Database Schema
- **Coordinates**: `coordinate` table with lat/lng for weather stations
- **Stations**: TMD weather stations with WMO codes and Thai/English names
- **Regions**: Thailand's 5 administrative regions (Northern, Northeastern, Central, Eastern, Southern)
- **Weather Data**: Current conditions linked to stations and coordinates

## Testing Conventions

### DAG Testing Pattern
```python
# Always mock database connections in DAG tests
with patch('include.weather.core.etl.references_tbl.create_connection') as mock_create_connection:
    mock_create_connection.return_value = MagicMock()
    import wt_test_notification  # Import after patching
```

### Geographic Data Testing
- Mock geocoding services to avoid API rate limits
- Test coordinate mapping accuracy with known test data
- Validate backward compatibility for existing function interfaces

## Development Environment

- **Python**: >=3.12 with uv package manager
- **Airflow**: 2.10.5 for local development
- **Key Dependencies**: pandas, geopy, requests, pytest
- **Code Quality**: Ruff for linting/formatting (configured in pyproject.toml)

## Critical Files to Understand
- `dags/wt_test_notification.py`: Example DAG structure with TaskGroups
- `include/weather/core/database/manager.py`: Database abstraction layer
- `scripts/database/references_tbl/manual/`: Geographic data processing classes
- `tests/unit/test_dags/test_wt_test_notification.py`: DAG testing patterns