#!/bin/bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Default config (TESTING)
helm install airflow apache-airflow/airflow \
    --namespace airflow \
    --create-namespace \
    --debug \
    --timeout 10m01s
