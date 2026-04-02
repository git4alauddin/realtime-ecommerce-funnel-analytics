from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow import DAG

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="build_daily_analytics_marts",
    description="Build daily analytics marts from Silver lake data and run warehouse checks.",
    start_date=datetime(2025, 1, 1),
    schedule=os.getenv("DE_PIPELINE_AIRFLOW_SCHEDULE", "0 2 * * *"),
    catchup=False,
    default_args=default_args,
    tags=["data-engineering", "analytics"],
) as dag:
    start = EmptyOperator(task_id="start_daily_pipeline")

    build_daily_marts = BashOperator(
        task_id="build_daily_analytics",
        cwd="/opt/project",
        bash_command="python -m jobs.batch.build_daily_analytics",
    )

    run_quality_checks = BashOperator(
        task_id="run_quality_checks",
        cwd="/opt/project",
        bash_command="python -m jobs.batch.run_quality_checks",
    )

    finish = EmptyOperator(task_id="finish_daily_pipeline")

    start >> build_daily_marts >> run_quality_checks >> finish
