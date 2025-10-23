from typing import Dict, Any, List
from datetime import datetime
from .base_dag import BaseDag
from airflow.operators.python import PythonOperator

class DagFactory:
    """Factory for creating standardized DAGs"""
    
    @staticmethod
    def create_ingestion_dag(
        dag_id: str,
        data_source: str,
        ingestion_callable,
        validation_callable,
        cleanup_callable,
        schedule_interval: str = '0 */6 * * *',
        **kwargs
    ) -> BaseDag:
        """Create a standard data ingestion DAG"""
        
        base_dag = BaseDag(
            dag_id=dag_id,
            description=f"Data ingestion from {data_source}",
            schedule_interval=schedule_interval,
            start_date=datetime(2024, 1, 1),
            tags=['ingestion', data_source],
            **kwargs
        )
        
        # Add standard tasks
        ingestion_task = base_dag.add_task(
            task_id='ingest_data',
            python_callable=ingestion_callable
        )
        
        validation_task = base_dag.add_task(
            task_id='validate_data',
            python_callable=validation_callable
        )
        
        cleanup_task = base_dag.add_task(
            task_id='cleanup_old_data',
            python_callable=cleanup_callable
        )
        
        # Set dependencies
        ingestion_task >> validation_task >> cleanup_task
        
        return base_dag