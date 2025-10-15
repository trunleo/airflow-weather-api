#!/bin/bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# # Default config (TESTING)
# helm install airflow apache-airflow/airflow \
#     --namespace airflow \
#     --create-namespace \
#     --debug \
#     --timeout 10m01s

# Create secret from existing file
./setup-credentials.sh

helm upgrade --install airflow apache-airflow/airflow \
    --namespace airflow \
    --create-namespace \
    --debug \
    --timeout 10m01s \
    --version 1.16.0 \
    -f values.yaml
# Set up gitsync for airflow
