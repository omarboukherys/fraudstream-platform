# Airflow + Java 17 + PySpark : un seul conteneur pour la démo (en prod : KubernetesPodOperator / Dataproc Serverless)
FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
