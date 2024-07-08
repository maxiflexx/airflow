FROM apache/airflow:2.9.0

RUN pip install minio==7.2.5
RUN pip install pytest
