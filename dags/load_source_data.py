"""
Load Source Data DAG

Extracts data from SQLite and loads to BigQuery.
Run manually (not scheduled) to seed the source tables.
"""
from datetime import datetime
import sqlite3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

# Config
PROJECT_ID = "otto-pipeline-practice"
DATASET_ID = "marketing"
SQLITE_PATH = "/opt/airflow/data/product_sales.db"  # Mounted from host

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
}


def load_products():
    """Extract products from SQLite, load to BigQuery."""
    # Read from SQLite
    conn = sqlite3.connect(SQLITE_PATH)
    df = pd.read_sql("SELECT sku_id, sku_description, price FROM product", conn)
    conn.close()

    print(f"Loaded {len(df)} products from SQLite")

    # Load to BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.product"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Replace existing data
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion

    print(f"Loaded {len(df)} products to {table_id}")


def load_sales():
    """Extract sales from SQLite, load to BigQuery."""
    conn = sqlite3.connect(SQLITE_PATH)
    df = pd.read_sql("""
        SELECT order_id, sku_id, orderdate_utc, sales
        FROM sales
    """, conn)
    conn.close()

    # Convert to timestamp
    df['orderdate_utc'] = pd.to_datetime(df['orderdate_utc'])

    print(f"Loaded {len(df)} sales from SQLite")

    # Load to BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.sales"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(df)} sales to {table_id}")


with DAG(
    dag_id="load_source_data",
    default_args=default_args,
    description="Load product and sales data from SQLite to BigQuery",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ingestion", "source"],
) as dag:

    load_products_task = PythonOperator(
        task_id="load_products",
        python_callable=load_products,
    )

    load_sales_task = PythonOperator(
        task_id="load_sales",
        python_callable=load_sales,
    )

    # Run in parallel (no dependency)
    [load_products_task, load_sales_task]
