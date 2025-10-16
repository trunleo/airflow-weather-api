"""Tests for weather test notification DAG."""

import pytest
import sys
import os
from datetime import datetime
from unittest.mock import patch, MagicMock

# Add the dags directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../dags'))


class TestWtTestNotificationDAG:
    """Test class for wt_test_notification DAG."""

    def test_dag_imports_successfully(self):
        """Test that the DAG file can be imported without errors."""
        with patch('include.weather.core.etl.references_tbl.create_connection') as mock_create_connection:
            mock_create_connection.return_value = MagicMock()
            
            # Import the DAG module
            import wt_test_notification
            
            # Verify the DAG exists
            assert hasattr(wt_test_notification, 'dag')
            assert wt_test_notification.dag is not None

    def test_dag_structure(self):
        """Test DAG structure and configuration."""
        with patch('include.weather.core.etl.references_tbl.create_connection') as mock_create_connection:
            mock_create_connection.return_value = MagicMock()
            
            # Import the DAG module
            import wt_test_notification
            dag = wt_test_notification.dag
            
            # Test DAG properties
            assert dag.dag_id == "wt_test_daily_pipeline"
            assert dag.description == "Test Weather daily pipeline"
            assert "weather" in dag.tags
            assert "data-engineer" in dag.tags
            
            # Test default args
            assert dag.default_args["owner"] == "trung.tran@vnsilicon.net,khai.do@vnsilicon.net"
            assert dag.default_args["retries"] == 0
            assert dag.default_args["email_on_failure"] is False
            assert dag.default_args["email_on_retry"] is False

    def test_task_count(self):
        """Test that DAG has the expected number of tasks."""
        with patch('include.weather.core.etl.references_tbl.create_connection') as mock_create_connection:
            mock_create_connection.return_value = MagicMock()
            
            # Import the DAG module
            import wt_test_notification
            dag = wt_test_notification.dag
            
            tasks = dag.tasks
            assert len(tasks) == 1

    def test_task_properties(self):
        """Test task properties."""
        with patch('include.weather.core.etl.references_tbl.create_connection') as mock_create_connection:
            mock_db_manager = MagicMock()
            mock_create_connection.return_value = mock_db_manager
            
            # Import the DAG module
            import wt_test_notification
            dag = wt_test_notification.dag
            
            # Get the task
            get_ref_tbl_task = dag.get_task("get_reference_table")
            
            # Test task properties
            assert get_ref_tbl_task is not None
            assert get_ref_tbl_task.task_id == "get_reference_table"
            
            # Test that task has no upstream dependencies (it's the first task)
            upstream_tasks = get_ref_tbl_task.upstream_task_ids
            assert len(upstream_tasks) == 0
            
            # Verify the task arguments
            assert "db_manager" in get_ref_tbl_task.op_kwargs

    def test_dag_schedule(self):
        """Test DAG scheduling configuration."""
        with patch('include.weather.core.etl.references_tbl.create_connection') as mock_create_connection:
            mock_create_connection.return_value = MagicMock()
            
            # Import the DAG module
            import wt_test_notification
            dag = wt_test_notification.dag
            
            # Test schedule interval (from the DAG - should be timedelta(days=1) as per default args)
            # Note: The DAG uses default_args["schedule_interval"] = "0 0 * * *"
            # but Airflow might convert this or use a different format
            schedule = dag.schedule_interval
            assert schedule == "0 0 * * *" or str(schedule) == "1 day, 0:00:00"
            
            # Test catchup setting - check default_args since it might not be inherited by DAG
            # In newer Airflow versions, catchup needs to be explicitly set on DAG
            if hasattr(dag, 'catchup'):
                # Either the DAG has catchup disabled or it's in default_args
                assert dag.catchup is False or dag.default_args.get("catchup") is False
            else:
                # Fallback to checking default_args
                assert dag.default_args.get("catchup") is False
            
            # Test start date - either on DAG directly or in default_args
            if dag.start_date:
                start_date_check = dag.start_date
            else:
                # Check in default_args
                start_date_check = dag.default_args.get("start_date")
            
            # Compare the date components (Airflow might convert to pendulum DateTime)
            expected_date = datetime(2025, 10, 12)
            if hasattr(start_date_check, 'date'):
                # If it's a pendulum DateTime, compare just the date part
                assert start_date_check.date() == expected_date.date()
            else:
                # Regular datetime comparison
                assert start_date_check.replace(tzinfo=None) == expected_date

    def test_connection_creation_called(self):
        """Test that create_connection function exists and can be imported."""
        # Since create_connection is called at module import time,
        # we need to patch it before importing
        with patch('include.weather.core.etl.references_tbl.create_connection') as mock_create_connection:
            mock_db_manager = MagicMock()
            mock_create_connection.return_value = mock_db_manager
            
            # Clear any existing module from cache to ensure fresh import
            import sys
            if 'wt_test_notification' in sys.modules:
                del sys.modules['wt_test_notification']
            
            # Import the DAG module (this will call create_connection)
            import wt_test_notification
            
            # Verify create_connection was called with the correct connection ID
            mock_create_connection.assert_called_once_with(postgres_conn_id="ac-weather-backend")

    def test_task_callable_function(self):
        """Test that the task uses the correct callable function."""
        with patch('include.weather.core.etl.references_tbl.create_connection') as mock_create_connection:
            mock_db_manager = MagicMock()
            mock_create_connection.return_value = mock_db_manager
            
            # Import the DAG module
            import wt_test_notification
            dag = wt_test_notification.dag
            task = dag.get_task("get_reference_table")
            
            # Test that the task uses the get_reference_table function
            # Check that the callable function name matches
            assert task.python_callable.__name__ == "get_reference_table"
