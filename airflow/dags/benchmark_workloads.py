from __future__ import annotations

import random
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "benchmark",
    "depends_on_past": False,
}


def mock_rpc(step_name: str, latency_ms: int = 0, payload_bytes: int = 0, failure_rate_percent: int = 0) -> dict:
    if latency_ms > 0:
        time.sleep(latency_ms / 1000)

    if failure_rate_percent > 0 and random.random() * 100 < failure_rate_percent:
        raise RuntimeError(f"Injected transient failure in {step_name}")

    return {
        "stepName": step_name,
        "latencyMs": latency_ms,
        "payloadBytes": payload_bytes,
    }


def wait_seconds(step_name: str, wait_ms: int) -> dict:
    time.sleep(max(0, wait_ms) / 1000)
    return {
        "stepName": step_name,
        "waitMs": wait_ms,
    }


def dag_for(dag_id: str) -> DAG:
    return DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=64,
        tags=["benchmark"],
    )


with dag_for("benchmark_low_latency") as benchmark_low_latency:
    previous = None
    for index in range(5):
        task = PythonOperator(
            task_id=f"low_latency_{index}",
            python_callable=mock_rpc,
            op_kwargs={"step_name": f"low_latency_{index}", "latency_ms": 10, "payload_bytes": 256},
        )
        if previous:
            previous >> task
        previous = task


with dag_for("benchmark_high_throughput") as benchmark_high_throughput:
    start = EmptyOperator(task_id="start")
    fanout_tasks = [
        PythonOperator(
            task_id=f"fanout_{index}",
            python_callable=mock_rpc,
            op_kwargs={"step_name": f"fanout_{index}", "latency_ms": 5, "payload_bytes": 128},
        )
        for index in range(100)
    ]
    aggregate = PythonOperator(
        task_id="aggregate_results",
        python_callable=mock_rpc,
        op_kwargs={"step_name": "aggregate_results", "latency_ms": 5, "payload_bytes": 128},
    )
    start >> fanout_tasks >> aggregate


with dag_for("benchmark_deep_sequential") as benchmark_deep_sequential:
    previous = None
    for index in range(50):
        task = PythonOperator(
            task_id=f"deep_step_{index}",
            python_callable=mock_rpc,
            op_kwargs={"step_name": f"deep_step_{index}", "latency_ms": 2, "payload_bytes": 128},
        )
        if previous:
            previous >> task
        previous = task


with dag_for("benchmark_long_running") as benchmark_long_running:
    long_pre_0 = PythonOperator(task_id="long_pre_0", python_callable=mock_rpc, op_kwargs={"step_name": "long_pre_0", "latency_ms": 5})
    long_pre_1 = PythonOperator(task_id="long_pre_1", python_callable=mock_rpc, op_kwargs={"step_name": "long_pre_1", "latency_ms": 5})
    long_wait = PythonOperator(task_id="long_wait", python_callable=wait_seconds, op_kwargs={"step_name": "long_wait", "wait_ms": 5000})
    long_post_0 = PythonOperator(task_id="long_post_0", python_callable=mock_rpc, op_kwargs={"step_name": "long_post_0", "latency_ms": 5})
    long_post_1 = PythonOperator(task_id="long_post_1", python_callable=mock_rpc, op_kwargs={"step_name": "long_post_1", "latency_ms": 5})
    long_pre_0 >> long_pre_1 >> long_wait >> long_post_0 >> long_post_1


with dag_for("benchmark_retry_heavy") as benchmark_retry_heavy:
    previous = None
    for index in range(5):
        task = PythonOperator(
            task_id=f"retry_step_{index}",
            python_callable=mock_rpc,
            op_kwargs={"step_name": f"retry_step_{index}", "latency_ms": 5, "failure_rate_percent": 20},
            retries=4,
            retry_delay=timedelta(seconds=1),
        )
        if previous:
            previous >> task
        previous = task


with dag_for("benchmark_timer_intensive") as benchmark_timer_intensive:
    previous = None
    for index in range(10):
        task = PythonOperator(
            task_id=f"timer_{index}",
            python_callable=wait_seconds,
            op_kwargs={"step_name": f"timer_{index}", "wait_ms": 1000},
        )
        if previous:
            previous >> task
        previous = task


with dag_for("benchmark_failure_recovery") as benchmark_failure_recovery:
    recovery_before_0 = PythonOperator(task_id="recovery_before_0", python_callable=mock_rpc, op_kwargs={"step_name": "recovery_before_0", "latency_ms": 10})
    recovery_before_1 = PythonOperator(task_id="recovery_before_1", python_callable=mock_rpc, op_kwargs={"step_name": "recovery_before_1", "latency_ms": 10})
    recovery_wait = PythonOperator(task_id="recovery_wait", python_callable=wait_seconds, op_kwargs={"step_name": "recovery_wait", "wait_ms": 30000})
    recovery_after_0 = PythonOperator(task_id="recovery_after_0", python_callable=mock_rpc, op_kwargs={"step_name": "recovery_after_0", "latency_ms": 10})
    recovery_after_1 = PythonOperator(task_id="recovery_after_1", python_callable=mock_rpc, op_kwargs={"step_name": "recovery_after_1", "latency_ms": 10})
    recovery_after_2 = PythonOperator(task_id="recovery_after_2", python_callable=mock_rpc, op_kwargs={"step_name": "recovery_after_2", "latency_ms": 10})
    recovery_before_0 >> recovery_before_1 >> recovery_wait >> recovery_after_0 >> recovery_after_1 >> recovery_after_2
