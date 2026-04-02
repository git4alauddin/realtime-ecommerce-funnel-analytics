from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

try:
    from airflow.models import DagBag
except ModuleNotFoundError:
    pytest.skip("apache-airflow is not installed", allow_module_level=True)


def test_airflow_dag_loads_with_expected_tasks():
    dags_path = Path(__file__).resolve().parents[2] / "airflow" / "dags"
    dag_bag = DagBag(dag_folder=str(dags_path), include_examples=False)

    assert "build_daily_analytics_marts" in dag_bag.dags
    dag = dag_bag.dags["build_daily_analytics_marts"]
    assert {task.task_id for task in dag.tasks} == {
        "start_daily_pipeline",
        "build_daily_analytics",
        "run_quality_checks",
        "finish_daily_pipeline",
    }
