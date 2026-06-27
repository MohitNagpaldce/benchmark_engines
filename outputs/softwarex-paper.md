# A Reproducible Benchmark Harness for Comparing Workflow Orchestration Engines: Temporal, Conductor, and Apache Airflow

**Mohit Nagpal**
Independent Researcher
mht.nagpal@gmail.com

---

## Code Metadata

| Nr. | Code metadata description | Value |
|---|---|---|
| C1 | Current code version | v1.0.0 |
| C2 | Permanent link to code/repository | https://github.com/MohitNagpaldce/benchmark_engines |
| C3 | Permanent link to reproducible capsule | https://doi.org/10.5281/zenodo.2097859 |
| C4 | Legal code license | GPL-3.0-only |
| C5 | Code versioning system used | git |
| C6 | Software code languages, tools, and services used | TypeScript 5, Node.js ≥ 22, Python 3.11, Docker Compose v2, PostgreSQL 16 |
| C7 | Compilation requirements, operating environments and dependencies | Node.js ≥ 20, Docker Desktop or Colima (macOS), Docker Compose; `npm ci` installs all TypeScript dependencies including `@temporalio/client`, `@temporalio/worker`, `conductor-javascript`, `apache-airflow-client`, `commander`, `prom-client` |
| C8 | Link to developer documentation / manual | https://github.com/MohitNagpaldce/benchmark_engines/blob/main/README.md |
| C9 | Support email for questions | mht.nagpal@gmail.com |

---

## Abstract

We present an open-source benchmark harness for evaluating workflow orchestration engines under representative distributed-system workload patterns. The framework supports Temporal, Conductor OSS, and Apache Airflow through engine-specific adapters sharing a common TypeScript runner, normalized result schema, and seven parameterized workload definitions. Each workload captures a distinct orchestration pattern: low-latency request execution, high-throughput fan-out, deep sequential task chains, long-running persisted waits, retry-heavy execution, timer-intensive scheduling, and failure-recovery baselines. Docker Compose stacks with PostgreSQL backends provide reproducible infrastructure for all three engines. Benchmark results are written as structured JSON, and Prometheus metrics are exposed for live monitoring. The harness enables workload-specific engine comparison without requiring access to production infrastructure.

**Keywords:** workflow orchestration, benchmark, Temporal, Apache Airflow, Conductor, distributed systems, reproducibility

---

## 1. Motivation and Significance

Workflow orchestration engines are a foundational layer of modern distributed systems, used to coordinate services, maintain durable execution state, manage retries and timers, and recover from partial failures. Temporal, Conductor, and Apache Airflow are among the most widely deployed open-source options, but they differ substantially in their execution models: Temporal uses event-sourced deterministic replay with SDK-defined workflow code; Conductor uses externally polled task workers and JSON workflow definitions; Airflow uses a DAG scheduler that forks subprocess executors for each task.

Despite this diversity, engineers choosing an engine typically rely on documentation, blog posts, and anecdotal comparisons. No publicly available benchmark framework provides reproducible, workload-specific measurements across all three systems from a shared codebase.

The harness presented here fills this gap. It provides:

- A single CLI runner that dispatches to all three engines through clean adapter interfaces.
- Seven parameterized workload definitions covering the principal orchestration patterns that distinguish engine behavior.
- Containerized, version-pinned infrastructure for each engine using Docker Compose and PostgreSQL.
- Structured JSON result files and Prometheus metrics for post-run analysis.
- A resource-scaling protocol (three CPU/memory configurations) enabling performance characterization across capacity levels.

The framework is designed so that researchers can add new engines by implementing a small adapter interface, and practitioners can run the full suite against a local or cloud environment without modifying workload definitions.

---

## 2. Software Description

### 2.1 Architecture

The harness is organized into three layers:

**Workload layer** (`src/problems.ts`): Seven `ProblemDefinition` objects specify workload parameters — number of steps, task latency, fan-out degree, timer durations, failure injection rate, and wait durations. These parameters are engine-agnostic and drive all three adapters.

**Runner layer** (`src/runner.ts`): A Commander CLI accepts `--engine`, `--problem`, `--total`, `--concurrency`, and `--results-dir` flags. The `runWithConcurrency()` function maintains a sliding window of in-flight workflow executions, records per-completion latency samples, and computes P50/P95/P99 percentiles after all runs complete. Results are written as `BenchmarkResultFile` JSON objects under `results/`.

**Adapter layer** (`src/engines/`): Three adapters translate workload parameters into engine-specific API calls:

- `temporal/` — TypeScript Temporal SDK (`@temporalio/client`, `@temporalio/worker`); workflow code in `workflows.ts` uses `proxyActivities` and `workflow.sleep()` for durable timers.
- `conductor/` — REST API client; `definitions.ts` assembles inline JSON workflow definitions using `SIMPLE`, `FORK_JOIN`, `JOIN`, and `WAIT` task types; `worker.ts` runs an HTTP polling loop.
- `airflow/` — REST client (`triggerDagRun`, `getDagRun`, `unpauseDag`); DAG definitions in `airflow/dags/benchmark_workloads.py`; polling-based completion detection.

**Metrics layer** (`src/metrics.ts`): Exposes a `benchmark_workflow_latency_ms` Prometheus histogram, a `benchmark_workflows_completed_total` counter, and a `benchmark_run_info` gauge on a configurable HTTP port. Compatible with Grafana.

### 2.2 Infrastructure

Each engine runs in a self-contained Docker Compose stack with a pinned PostgreSQL 16 backend:

- `docker-compose.temporal.yml` — `temporalio/auto-setup:latest` server with `DB=postgres12`; separate TypeScript worker container built from the included `Dockerfile`.
- `docker-compose.conductor.yml` — `conductoross/conductor:latest`; `CONDUCTOR_INDEXING_ENABLED=false` eliminates the Elasticsearch dependency.
- `docker-compose.airflow.yml` — `apache/airflow:2.10.4` with `LocalExecutor`; separate init, webserver, and scheduler services; `ulimits: nofile: 65536` on the scheduler container to support high-parallelism workloads.

All three stacks can be started and torn down independently. Resource limits (CPU, memory) are set per service and can be scaled to test performance across capacity configurations.

### 2.3 Workload Definitions

| Workload | Pattern | Steps | Latency config | Timer/wait |
|---|---|---|---|---|
| low-latency | Sequential | 5 | 10 ms/step | None |
| high-throughput | Fan-out/fan-in | 100 parallel + 1 join | 5 ms/step | None |
| deep-sequential | Long chain | 50 sequential | 2 ms/step | None |
| long-running | Wait + resume | 2 + wait + 2 | 10 ms/step | 5 s |
| retry-heavy | Fault injection | 5 sequential | 10 ms/step | 20% failure rate |
| timer-intensive | Repeated timers | 10 wait steps | — | 1 s/step |
| failure-recovery | Long wait + resume | 2 + wait + 3 | 10 ms/step | 30 s |

### 2.4 Result Schema

Each completed benchmark run produces a JSON file conforming to the `BenchmarkResultFile` interface (`src/common/types.ts`):

```json
{
  "engine": "temporal",
  "problem": { "id": "low-latency", "steps": 5, "latencyMs": 10 },
  "config": { "total": 100, "concurrency": 10 },
  "summary": {
    "total": 100,
    "succeeded": 100,
    "failed": 0,
    "durationMs": 16566,
    "workflowsPerSecond": 6.04,
    "p50LatencyMs": 1658,
    "p95LatencyMs": 3027,
    "p99LatencyMs": 3418
  },
  "samples": [ ... ]
}
```

Raw per-workflow `samples` arrays enable post-hoc statistical analysis independent of the runner's summary computation.

---

## 3. Illustrative Examples

### 3.1 Running a single benchmark

```bash
# Start the Temporal stack
docker compose -f docker-compose.temporal.yml up -d

# Run low-latency benchmark: 100 workflows, 10 concurrent
npx tsx src/runner.ts --engine temporal --problem low-latency --total 100 --concurrency 10

# Results written to results/temporal-low-latency-<uuid>.json
```

### 3.2 Running all seven workloads across all three engines

```bash
for ENGINE in temporal conductor airflow; do
  docker compose -f docker-compose.${ENGINE}.yml up -d
  for PROBLEM in low-latency high-throughput deep-sequential long-running retry-heavy timer-intensive failure-recovery; do
    npx tsx src/runner.ts --engine $ENGINE --problem $PROBLEM --total 100 --concurrency 10 \
      --results-dir results/config1
  done
  docker compose -f docker-compose.${ENGINE}.yml down
done
```

### 3.3 Observed results (illustrative, local single-node — three resource configurations)

All results were obtained on a local development machine (Apple M-series, 36 GB host RAM, Colima Docker VM: 6 vCPU / 12 GB). Three resource configurations were tested sequentially per engine:

- **Config 1 (baseline):** 2 vCPU / 2 GB per engine service; 200 max concurrent workers
- **Config 2 (2×):** 4 vCPU / 4 GB per engine service; 400 max concurrent workers
- **Config 3 (4×):** 6 vCPU / 8 GB per engine service; 800 max concurrent workers

**Temporal — P50 latency (ms):**

| Workload | Config 1 | Config 2 | Config 3 | Δ C1→C3 |
|---|---:|---:|---:|---:|
| low-latency | 1,658 | 615 | 554 | −67% |
| high-throughput | 5,022 | 2,094 | 1,636 | −67% |
| deep-sequential | 5,244 | 5,037 | 5,046 | −4% |
| long-running | 5,339 | 5,448 | 5,236 | −2% |
| retry-heavy | 1,388 | 1,362 | 1,444 | +4% |
| timer-intensive | 10,318 | 10,714 | 10,346 | +0.3% |
| failure-recovery | 31,177 | 30,740 | 30,593 | −2% |

**Conductor — P50 latency (ms):**

| Workload | Config 1 | Config 2 | Config 3 | Δ C1→C3 |
|---|---:|---:|---:|---:|
| low-latency | 10,937 | 10,781 | 10,660 | −3% |
| high-throughput | 30,089 | 17,148 | 9,215 | −69% |
| deep-sequential | 105,774 | 105,131 | 105,303 | −0.4% |
| long-running | 13,562 | 15,648 | 14,460 | +7% |
| retry-heavy | 11,377 | 12,741 | 12,785 | +12% |
| timer-intensive | 61,081 | 23,944 | 31,126 | −49% |
| failure-recovery | 41,414 | 42,000 | 43,138 | +4% |

**Airflow — P50 latency (ms):**

| Workload | Config 1 | Config 2 | Config 3 | Δ C1→C3 |
|---|---:|---:|---:|---:|
| low-latency | 19,992 | 10,147 | 10,272 | −49% |
| high-throughput | OOM crash | 161,017 | degraded† | — |
| deep-sequential | 88,856 | 64,122 | 64,665 | −27% |
| long-running | 24,485 | 16,081 | 14,537 | −41% |
| retry-heavy | 24,816 | 12,756 | 11,045 | −56% |
| timer-intensive | 115,713 | 65,614 | 67,497 | −42% |
| failure-recovery | 89,690 | 72,408 | 61,369 | −32% |

† Config 3 Airflow high-throughput ran after 6 prior workloads in the same session; only 4/20 runs succeeded due to accumulated scheduler state. Re-run in an isolated session recommended.

![Figure 1 — P50 Latency by Engine and Workload, Config 1 baseline](../figures/fig1_p50_latency_config1.png)

![Figure 2 — Throughput (workflows/second) by Engine and Workload, Config 1](../figures/fig2_throughput_config1.png)

![Figure 3 — P50 Latency Scaling Across Configs C1, C2, C3 per Engine](../figures/fig3_scaling_p50_configs.png)

![Figure 4 — Success Rate Heatmap across Engine × Config × Workload](../figures/fig4_success_heatmap.png)

![Figure 5 — Temporal P50/P95/P99 Latency Spread by Workload](../figures/fig5_temporal_latency_spread.png)

![Figure 6 — Timer-Intensive Throughput Scaling Across Configs](../figures/fig6_timer_intensive_scaling.png)

These results demonstrate the harness's ability to surface both resource-sensitive and structurally-bounded behaviors. Temporal scales cleanly with CPU for latency-sensitive workloads (low-latency: −67% P50 from C1→C3). Conductor's deep-sequential workload remains flat across all configs (105,774 ms → 105,303 ms), confirming the bottleneck is sequential HTTP poll round-trips, not compute. Airflow's high-throughput workload crashed the scheduler OOM in Config 1 (LocalExecutor subprocess fan-out exhausted memory) but completed at Config 2 with a 10 GB scheduler allocation.

---

## 4. Impact

The harness is intended for three audiences:

**Researchers** gain a reproducible baseline for comparing orchestration systems under controlled workload conditions. The normalized result schema and raw sample arrays enable statistical comparison, bootstrap simulation, and cross-paper result reproduction.

**Engineers and architects** can run the framework in their own environment to obtain local or cloud-specific measurements before committing to an engine. The Docker Compose stacks eliminate the setup cost of standing up three orchestration systems independently.

**Engine maintainers and contributors** can use the workload taxonomy to identify performance regressions across releases. The seven workloads correspond to patterns (fan-out, deep chains, durable timers, fault injection) that are difficult to capture with microbenchmarks but common in production deployments.

The benchmark design intentionally separates workload parameters from engine-specific implementations, so new engines (e.g., Zeebe/Camunda 8, AWS Step Functions via LocalStack, Prefect) can be added by implementing the adapter interface in `src/engines/`.

---

## 5. Conclusions

We have described an open-source benchmark harness that enables reproducible, workload-specific comparison of workflow orchestration engines. The framework provides normalized workload definitions, engine-specific adapters, containerized infrastructure, and structured result output for Temporal, Conductor, and Apache Airflow. Illustrative results on a local environment demonstrate that the framework surfaces meaningful behavioral differences across engines and resource configurations. The code, Docker Compose stacks, and raw result files are publicly available at https://github.com/MohitNagpaldce/benchmark_engines under the GPL-3.0 license.

---

## Conflict of Interest

The author declares no conflict of interest.

---

## References

1. Temporal Technologies. *Temporal Documentation*. https://docs.temporal.io/ (accessed June 2026).
2. Conductor OSS. *Conductor Documentation*. https://conductor-oss.github.io/conductor/ (accessed June 2026).
3. Apache Software Foundation. *Apache Airflow Documentation*. https://airflow.apache.org/docs/ (accessed June 2026).
4. Temporal TypeScript SDK. https://typescript.temporal.io/ (accessed June 2026).
5. Apache Airflow Stable REST API. https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html (accessed June 2026).
6. Conductor Task API. https://conductor-oss.github.io/conductor/documentation/api/task.html (accessed June 2026).
