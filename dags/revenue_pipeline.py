"""
Revenue Pipeline DAG

Calculates daily revenue for all products and loads to BigQuery.
Runs daily, processing the previous day's data.
"""
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)

# Config
PROJECT_ID = "otto-pipeline-practice"
DATASET_ID = "marketing"
SQL_PATH = Path(__file__).parent.parent / "sql"


def load_sql(filename: str) -> str:
    """Load SQL from file and substitute variables."""
    sql = (SQL_PATH / filename).read_text()
    return sql.format(project_id=PROJECT_ID, dataset_id=DATASET_ID)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="revenue_pipeline",
    default_args=default_args,
    description="Calculate daily product revenue",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["revenue", "marketing"],
) as dag:

    # Task 1: Build revenue data from SQL file
    build_revenue = BigQueryInsertJobOperator(
        task_id="build_revenue",
        configuration={
            "query": {
                "query": load_sql("build_revenue.sql"),
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location="EU",
    )

    # Task 2: Validate row count
    validate_revenue = BigQueryCheckOperator(
        task_id="validate_revenue",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.{DATASET_ID}.revenue`
        """,
        use_legacy_sql=False,
        location="EU",
        gcp_conn_id="google_cloud_default",
    )

    # Define task order
    build_revenue >> validate_revenue