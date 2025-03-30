FROM apache/airflow:2.10.4-python3.10

ENV AIRFLOW_HOME=/opt/airflow

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME
COPY ./google /opt/airflow/google