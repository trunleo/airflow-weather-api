from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="my_first_simple_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Run manually or on a specific schedule
    catchup=False,
    tags=["example"],
) as dag:
    # Define tasks
    start_task = BashOperator(
        task_id="start_message",
        bash_command="echo 'Starting the DAG!'",
    )

    process_data = BashOperator(
        task_id="process_data",
        bash_command="echo 'Processing some data...'",
    )

    end_task = BashOperator(
        task_id="end_message",
        bash_command="echo 'DAG finished successfully!'",
    )

    # Define task dependencies
    start_task >> process_data >> end_task