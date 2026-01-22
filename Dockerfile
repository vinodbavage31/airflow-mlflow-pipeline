FROM python:3.11-slim

LABEL maintainer="ETL Pipeline"
LABEL description="ETL Pipeline for Corporate Registry with Apache Airflow"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    AIRFLOW_HOME=/opt/airflow \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags \
    AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins \
    AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True

WORKDIR /opt/airflow

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    curl \
    openjdk-21-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    pip install --no-cache-dir psycopg2-binary && \
    rm /tmp/requirements.txt

COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh

COPY . /opt/airflow/

RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins && \
    chmod -R 755 /opt/airflow

EXPOSE 8080

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
