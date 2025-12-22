FROM apache/airflow:2.10.5

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

COPY pyproject.toml /opt/airflow/pyproject.toml

RUN pip install --no-cache-dir ".[dev]"
