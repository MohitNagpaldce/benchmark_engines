# Airflow Adapter

The Airflow adapter runs the same benchmark workload names through Apache Airflow DAGs.

Airflow is not a request/response workflow engine in the same way Temporal is. It is a DAG scheduler and task executor, so these tests mainly measure DAG run scheduling, task execution, fan-out scheduling, retry behavior, and wait/sleep behavior.

## Local Setup

Start Airflow:

```bash
npm run airflow:up
```

Endpoints:

- Airflow UI: `http://localhost:8081`
- Airflow stable REST API: `http://localhost:8081/api/v1`

Default credentials:

- Username: `airflow`
- Password: `airflow`

Run a benchmark:

```bash
npm run airflow:bench -- --problem low-latency --total 5 --concurrency 5
```

Stop Airflow:

```bash
npm run airflow:down
```

## Implementation

- DAGs live in `airflow/dags/benchmark_workloads.py`.
- REST API calls live in `src/engines/airflow/client.ts`.
- DAG ID/result mapping lives in `src/engines/airflow/definitions.ts`.
- The shared runner supports Airflow with `--engine airflow`.

## Model Differences

- Airflow DAG structure is parsed by the scheduler rather than defined dynamically per run.
- Timer-style tests currently use Python sleep tasks, which occupy worker slots. This is not identical to Temporal durable timers or Conductor WAIT tasks.
- The failure-recovery workload is a baseline long-wait shape and does not yet kill/restart Airflow scheduler or workers.
