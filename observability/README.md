# Observability

This directory contains Prometheus and Grafana configuration for comparing workflow orchestration benchmark runs.

## Services

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

## Dashboard

Grafana automatically provisions the `Workflow Benchmark Overview` dashboard.

Panels currently include:

- Workflow throughput
- P50/P95/P99 workflow latency
- Workflow completions by success/failure
- Mock activity calls
- Workflow timers

## Scrape Targets

Prometheus is configured to scrape:

- `temporal:8000`: Temporal server metrics
- `host.docker.internal:9464`: benchmark runner metrics
- `host.docker.internal:9465`: Temporal TypeScript worker SDK metrics

The `host.docker.internal` targets assume Docker Desktop on macOS or Windows. On Linux, replace those targets with the host gateway address or run the worker/runner inside Docker.

## Run Sequence

```bash
npm run stack:up
npm run temporal:worker
npm run temporal:bench -- --problem low-latency --total 100 --concurrency 10 --metrics-hold-ms 60000
```

Then open Grafana at `http://localhost:3000`.
