ENV AIRFLOW_VERSION=2.9.2
FROM apache/airflow:2.9.2
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt