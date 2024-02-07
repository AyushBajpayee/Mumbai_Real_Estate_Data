FROM apache/airflow
FROM python:3.10-slim
COPY requirments_propreturns.txt .
RUN pip install --upgrade pip
RUN pip install -r requirments_propreturns.txt