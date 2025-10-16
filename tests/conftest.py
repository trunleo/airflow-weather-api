"""Pytest configuration for weather tests."""

import pytest
from unittest.mock import Mock

@pytest.fixture
def mock_postgres_hook():
    """Mock PostgresHook for testing."""
    return Mock()

@pytest.fixture
def sample_weather_data():
    """Sample weather data for testing."""
    return [
        {
            "id": 1,
            "date": "2025-10-16",
            "tc": 25.5,
            "rh": 80.0,
            "rain": 5.2,
            "province_name": "Bangkok"
        }
    ]
