"""Unit tests for WeatherDBManager."""

import pytest
from unittest.mock import Mock, patch
from include.weather.core.database.manager import WeatherDBManager

class TestWeatherDBManager:
    """Test cases for WeatherDBManager."""
    
    def test_init(self):
        """Test manager initialization."""
        manager = WeatherDBManager("test_conn")
        assert manager.conn_id == "test_conn"
    
    @patch('include.weather.core.database.manager.PostgresHook')
    def test_test_connection_success(self, mock_hook_class):
        """Test successful connection test."""
        mock_hook = Mock()
        mock_hook.get_records.return_value = [(1,)]
        mock_hook_class.return_value = mock_hook
        
        manager = WeatherDBManager("test_conn")
        assert manager.test_connection() is True
    
    @patch('include.weather.core.database.manager.PostgresHook')
    def test_test_connection_failure(self, mock_hook_class):
        """Test failed connection test."""
        mock_hook = Mock()
        mock_hook.get_records.side_effect = Exception("Connection failed")
        mock_hook_class.return_value = mock_hook
        
        manager = WeatherDBManager("test_conn")
        assert manager.test_connection() is False
