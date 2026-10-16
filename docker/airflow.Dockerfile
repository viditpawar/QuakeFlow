# Apache Airflow 3.3.2 on Python 3.11, with the Docker provider so that tasks can run the platform's own
# containers (the Spark image). The provider is installed against the constraints file that the Airflow
# project publishes for this exact release, so every package version is one that was tested together.
# That file pins the Docker provider at 4.5.9, which is the version used here.
FROM apache/airflow:3.3.2-python3.11

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.11
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-docker==4.5.9" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
