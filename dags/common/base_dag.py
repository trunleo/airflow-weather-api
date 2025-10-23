from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)

class BaseDag:
    """Base class for creating standardized DAGs"""
    
    def __init__(
        self,
        dag_id: str,
        description: str,
        schedule_interval: str,
        start_date: datetime,
        tags: Optional[List[str]] = None,
        default_args: Optional[Dict[str, Any]] = None
    ):
        self.dag_id = dag_id
        self.description = description
        self.schedule_interval = schedule_interval
        self.start_date = start_date
        self.tags = tags or []
        
        # Standard default args
        self.default_args = {
            'owner': 'data-team',
            'depends_on_past': False,
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
            'catchup': False,
            **(default_args or {})
        }
        
        self.dag = self._create_dag()
    
    def _create_dag(self) -> DAG:
        """Create the DAG object"""
        return DAG(
            dag_id=self.dag_id,
            description=self.description,
            schedule_interval=self.schedule_interval,
            start_date=self.start_date,
            default_args=self.default_args,
            tags=self.tags,
            max_active_runs=1,
            catchup=False
        )
    
    def add_task(self, task_id: str, python_callable, **kwargs) -> PythonOperator:
        """Add a Python task to the DAG"""
        task = PythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            dag=self.dag,
            **kwargs
        )
        return task
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration from Airflow Variables"""
        try:
            return Variable.get(key, default_var=default)
        except Exception as e:
            logger.warning(f"Could not get variable {key}: {e}")
            return default