FROM apache/airflow:2.11.2
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt