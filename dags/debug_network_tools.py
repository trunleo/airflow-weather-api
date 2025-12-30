from datetime import datetime, timedelta
import socket
import logging
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import pandas as pd
from hooks.postgres_hook import ForecastPostgresHook


logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
}

TRINO_HOST = str(Variable.get("TRINO_HOST", default_var="trino.aisac-dev.vnsilicon.cloud"))
print(TRINO_HOST)
TRINO_PORT = int(Variable.get("TRINO_PORT", default_var=443))
print(TRINO_PORT)
TRINO_USER = str(Variable.get("TRINO_USER", default_var=""))
print(TRINO_USER)
TRINO_PASSWORD = str(Variable.get("TRINO_PASSWORD", default_var=""))
print(TRINO_PASSWORD)
TRINO_CATALOG = str(Variable.get("TRINO_CATALOG", default_var=""))
print(TRINO_CATALOG)
TRINO_SCHEMA = str(Variable.get("TRINO_SCHEMA", default_var=""))
print(TRINO_SCHEMA)

def check_dns():
    logger.info(f"Checking DNS resolution for {TRINO_HOST}")
    try:
        ip_address = socket.gethostbyname(TRINO_HOST)
        logger.info(f"Successfully resolved {TRINO_HOST} to {ip_address}")
    except socket.gaierror as e:
        logger.error(f"DNS resolution failed for {TRINO_HOST}: {e}")
        raise

def check_tcp_connection():
    logger.info(f"Checking TCP connection to {TRINO_HOST}:{TRINO_PORT}")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        result = sock.connect_ex((TRINO_HOST, TRINO_PORT))
        if result == 0:
            logger.info(f"Successfully connected to {TRINO_HOST}:{TRINO_PORT}")
        else:
            logger.error(f"Failed to connect to {TRINO_HOST}:{TRINO_PORT}, error code: {result}")
            raise Exception(f"TCP connection failed with error code {result}")
    except Exception as e:
        logger.error(f"Socket connection error: {e}")
        raise
    finally:
        sock.close()

def check_curl():
    logger.info(f"Checking connectivity using curl to https://{TRINO_HOST}:{TRINO_PORT}")
    # -I for headers only to avoid downloading body, -v for verbose to see handshake
    cmd = ["curl", "-I", "-v", f"https://{TRINO_HOST}:{TRINO_PORT}"]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
        logger.info(f"Curl standard output:\n{result.stdout}")
        logger.info(f"Curl standard error (verbose info):\n{result.stderr}")
        if result.returncode != 0:
            raise Exception(f"Curl command failed with return code {result.returncode}")
    except subprocess.TimeoutExpired:
        logger.error("Curl command timed out")
        raise
    except Exception as e:
        logger.error(f"Curl command failed: {e}")
        raise
def check_trino_connection():
    conn = connect(
            host=str(TRINO_HOST),
            port=TRINO_PORT,
            user=str(TRINO_USER),
            auth=BasicAuthentication(str(TRINO_USER), str(TRINO_PASSWORD)),
            catalog=str(TRINO_CATALOG),
            schema=str(TRINO_SCHEMA),
            request_timeout=10,
        )
    df = pd.read_sql_query("SELECT 1", conn)
    logger.info("Connected to Trino")

def check_pg_connection():
    pg_hook = ForecastPostgresHook(postgres_conn_id="weather_db_connection_id")
    pg_hook.get_conn()
    logger.info("Connected to Postgres")


with DAG(
    "debug_network_tools",
    default_args=default_args,
    description="Diagnostic DAG for network connectivity",
    schedule_interval=None,
    tags=["debug", "network"],
) as dag:

    dns_task = PythonOperator(
        task_id="check_dns",
        python_callable=check_dns,
    )

    tcp_task = PythonOperator(
        task_id="check_tcp",
        python_callable=check_tcp_connection,
    )

    curl_task = PythonOperator(
        task_id="check_curl",
        python_callable=check_curl,
    )

    trino_task = PythonOperator(
            task_id="check_trino",
        python_callable=check_trino_connection,
    )

    pg_task = PythonOperator(
            task_id="check_pg",
        python_callable=check_pg_connection,
    )

    dns_task >> tcp_task >> curl_task >> trino_task >> pg_task
