"""
DAG Validation Tests

Tests to ensure DAGs are valid and can be imported without errors.
"""
import pytest
from pathlib import Path


class TestDAGIntegrity:
    """Test DAG files can be loaded without errors."""

    def test_dag_import_no_errors(self):
        """All DAGs should import without errors."""
        from airflow.models import DagBag
        
        dag_folder = Path(__file__).parent.parent / "dags"
        dag_bag = DagBag(dag_folder=str(dag_folder), include_examples=False)
        
        assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

    def test_dag_count(self):
        """Should have expected number of DAGs."""
        from airflow.models import DagBag
        
        dag_folder = Path(__file__).parent.parent / "dags"
        dag_bag = DagBag(dag_folder=str(dag_folder), include_examples=False)
        
        assert len(dag_bag.dags) == 2, f"Expected 2 DAGs, found {len(dag_bag.dags)}"

    def test_revenue_pipeline_tasks(self):
        """Revenue pipeline should have expected tasks."""
        from airflow.models import DagBag
        
        dag_folder = Path(__file__).parent.parent / "dags"
        dag_bag = DagBag(dag_folder=str(dag_folder), include_examples=False)
        
        dag = dag_bag.dags.get("revenue_pipeline")
        assert dag is not None, "revenue_pipeline DAG not found"
        
        task_ids = [task.task_id for task in dag.tasks]
        assert "build_revenue" in task_ids
        assert "validate_revenue" in task_ids

    def test_load_source_data_tasks(self):
        """Load source data should have expected tasks."""
        from airflow.models import DagBag
        
        dag_folder = Path(__file__).parent.parent / "dags"
        dag_bag = DagBag(dag_folder=str(dag_folder), include_examples=False)
        
        dag = dag_bag.dags.get("load_source_data")
        assert dag is not None, "load_source_data DAG not found"
        
        task_ids = [task.task_id for task in dag.tasks]
        assert "load_products" in task_ids
        assert "load_sales" in task_ids


class TestSQLFiles:
    """Test SQL files exist and are valid."""

    def test_build_revenue_sql_exists(self):
        """build_revenue.sql should exist."""
        sql_path = Path(__file__).parent.parent / "sql" / "build_revenue.sql"
        assert sql_path.exists(), "build_revenue.sql not found"

    def test_build_revenue_sql_has_content(self):
        """build_revenue.sql should have content."""
        sql_path = Path(__file__).parent.parent / "sql" / "build_revenue.sql"
        content = sql_path.read_text()
        
        assert len(content) > 100, "SQL file seems too short"
        assert "CREATE OR REPLACE TABLE" in content
        assert "{project_id}" in content
        assert "{dataset_id}" in content
