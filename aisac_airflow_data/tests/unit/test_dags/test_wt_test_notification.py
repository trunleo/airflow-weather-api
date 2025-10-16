from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import uv

dag = DAG('test_notification_dag', default_args={'owner': 'airflow', 'start_date': days_ago(1)})

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> end

@uv.test
def test_dag_structure():
    assert 'start' in dag.task_ids
    assert 'end' in dag.task_ids
    assert start >> end