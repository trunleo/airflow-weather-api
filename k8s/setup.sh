#!/bin/bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Default config (TESTING)
helm install airflow apache-airflow/airflow \
    --namespace airflow \
    --create-namespace \
    --debug \
    --timeout 10m01s

# Set up gitsync for airflow
echo -n "github-username" | base64
echo -n "github-token" | base64

# Create secret
kubectl apply -f k8s/secret.yaml --namespace airflow

# Show values file
helm show values airflow -n airflow > k8s/values.yaml

helm upgrade --install airflow apache-airflow/airflow \
    -n airflow -f k8s/values.yaml \
    --debug \
    --timeout 10m02s
