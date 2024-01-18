FROM apache/airflow:latest

USER root
RUN apt-get update && \
    apt-get clean

USER airflow