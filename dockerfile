FROM apache/airflow:2.7.3

# Install additional Python packages
RUN pip install --no-cache-dir \
    pandas \
    google-cloud-bigquery \
    db-dtypes \
    pyarrow