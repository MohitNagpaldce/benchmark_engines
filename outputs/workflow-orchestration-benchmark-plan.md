# Workflow Orchestration Benchmark Project Plan

## Project Goal

Build a reproducible benchmarking framework for comparing open-source workflow orchestration engines under realistic workload patterns. The initial engine scope is:

- Temporal
- Netflix Conductor
- Apache Airflow

The benchmark should produce quantitative data and architectural observations that help engineers choose an orchestration system for specific workload profiles.

## Key Questions

- Which engine has the lowest orchestration overhead for latency-sensitive workflows?
- Which engine handles the highest task and workflow throughput on a single local benchmark environment?
- How does each engine behave as workflow depth, timers, retries, and long-running state increase?
- What operational bottlenecks appear under worker crashes, restarts, and backend persistence pressure?
- Which engine is best suited for each workload category rather than overall in the abstract?

## Benchmark Workloads

### 1. Low-Latency Workflow Execution

Simulate user-facing request orchestration such as checkout, payment authorization, ride booking, or signup flows.

Workload shape:

- Short sequential workflow
- 3 to 5 service-like tasks
- Mock RPC calls with controlled latency
- Moderate total volume, targeting 100K to 1M workflow executions if feasible locally

Primary metrics:

- Workflow start latency
- Task scheduling latency
- End-to-end completion latency
- P50, P95, P99 latency

### 2. High-Throughput Parallel Workflows

Simulate fan-out workloads such as notification campaigns, content processing, or event processing pipelines.

Workload shape:

- One parent workflow
- 1K to 10K child tasks or child workflows, adjusted to what one machine can sustain
- Mock RPC endpoint calls
- Aggregation step at the end

Primary metrics:

- Workflows per second
- Tasks per second
- Worker utilization
- Queue backlog
- Failure/error rate under load

### 3. Deep Multi-Step Workflows

Simulate enterprise workflows such as loan approval, KYC, insurance claims, or staged document approval.

Workload shape:

- Sequential workflow with 50 or more steps
- Each step performs a small deterministic unit of work or mock service call
- Step count should be parameterized, for example 50, 100, and 250 steps

Primary metrics:

- Scheduler overhead per step
- End-to-end workflow latency
- Event history growth
- Reliability at increased workflow depth

### 4. Long-Running Workflows

Simulate workflows that persist state and resume after delays, such as customer disputes, approvals, onboarding, and contract review.

Workload shape:

- Workflow with durable waits
- Use compressed benchmark timers instead of real multi-day waits
- Resume after timer or external signal
- Run enough concurrent workflows to observe persistence and replay cost

Primary metrics:

- Storage overhead per workflow
- Event history size growth
- Resume/replay latency
- Database read/write load

### 5. Retry-Heavy Workflows

Simulate transient failures in external services such as payment gateways, shipping APIs, fraud checks, and vendor integrations.

Workload shape:

- Task failure rates configured at 1%, 5%, 20%, and 50%
- Exponential backoff retry policy
- Optional retry-storm scenario with temporary 100% failure window

Primary metrics:

- Retry latency
- Retry queue buildup
- Duplicate execution risk
- System stability under repeated failures
- Final workflow success/failure rate

### 6. Timer-Intensive Workflows

Simulate delayed actions such as subscription reminders, payment deadlines, order cancellation, SLA monitoring, and scheduled follow-ups.

Workload shape:

- Large number of workflows with timers
- Timer durations compressed to seconds or minutes
- Test increasing timer counts until local system saturation

Primary metrics:

- Timer scheduling accuracy
- Timer fire delay
- Timer queue scalability
- Persistence/storage overhead

### 7. Failure Recovery and Resilience

Simulate infrastructure failure while workflows are active.

Workload shape:

- Start a fixed number of active workflows, for example 10K where feasible
- Kill worker containers
- Restart workers
- Optionally restart persistence components in a controlled scenario

Primary metrics:

- Recovery time
- Replay overhead
- Duplicate task execution risk
- Workflow correctness after recovery
- Lost, stuck, or corrupted workflow count

## Benchmark Framework Architecture

### Components

- `benchmark-runner`: CLI that starts benchmark scenarios, passes parameters, and records run metadata.
- `engine-adapters`: One adapter per workflow engine with a common interface for deploy, start workflow, query status, collect engine-specific stats, and teardown.
- `workload-definitions`: Shared workload specifications independent of engine implementation.
- `mock-services`: Local HTTP/gRPC services used to simulate RPC latency, failures, and payload sizes.
- `metrics-collector`: Collects application metrics, engine metrics, container metrics, and benchmark timestamps.
- `results-store`: Writes normalized results to files or a database for analysis.
- `report-generator`: Produces charts, tables, and observations for each engine/workload combination.

### Suggested Technology Choices

- Docker Compose for local reproducible setup.
- Prometheus for metrics collection.
- Grafana dashboards for live inspection.
- JSON or Parquet for benchmark result output.
- Python, Go, or Java for the benchmark runner; choose the language based on team familiarity and engine SDK support.
- OpenTelemetry where possible for consistent traces and timing.

## Normalized Metrics Model

Capture these fields for every run:

- Engine name and version
- Engine configuration
- Workload type
- Workload parameters
- Machine profile
- Container CPU and memory limits
- Worker count
- Task concurrency
- Start and end timestamps
- Total workflows started
- Total workflows completed
- Total workflows failed
- Total tasks completed
- Throughput
- P50, P95, P99 latency
- CPU utilization
- Memory utilization
- Persistence read/write volume
- Queue depth or backlog
- Error count
- Retry count
- Timer delay
- Recovery duration, where applicable

## Execution Phases

### Phase 0: Benchmark Design

Deliverables:

- Final workload definitions
- Metric dictionary
- Engine comparison criteria
- Local machine and container resource profile

Exit criteria:

- All workloads have clear parameters, expected behavior, and success conditions.
- All engines can be compared using the same top-level metrics.

### Phase 1: Local Engine Setup

Deliverables:

- Docker Compose setup for Temporal
- Docker Compose setup for Netflix Conductor
- Docker Compose setup for Apache Airflow
- Basic smoke test workflow for each engine

Exit criteria:

- Each engine can run locally from a clean checkout.
- A trivial workflow can be started, completed, and observed.

### Phase 2: Common Benchmark Harness

Deliverables:

- Benchmark CLI
- Shared workload config format
- Run metadata capture
- Result file schema
- Mock service for latency/failure simulation

Exit criteria:

- Same benchmark command shape can run against all three engines.
- Results are written in a normalized format.

### Phase 3: Engine Adapters

Deliverables:

- Temporal adapter
- Conductor adapter
- Airflow adapter
- Engine-specific workflow implementations for each benchmark category

Exit criteria:

- Each engine supports the same workload categories where architecturally reasonable.
- Any mismatch is explicitly documented.

### Phase 4: Metrics and Observability

Deliverables:

- Prometheus scrape configuration
- Container CPU/memory collection
- Engine-specific metric mapping
- Dashboard for live benchmark monitoring

Exit criteria:

- Benchmark output includes both application-level and system-level metrics.
- Metrics can be traced back to a specific benchmark run ID.

### Phase 5: Benchmark Execution

Deliverables:

- Baseline benchmark runs
- Parameter sweep runs
- Saturation tests
- Failure recovery tests

Exit criteria:

- Each workload has comparable runs across all engines.
- Runs are repeated enough to reduce noise.
- Outliers and failed runs are labeled, not silently removed.

### Phase 6: Analysis and Report

Deliverables:

- Benchmark report
- Charts and tables
- Architectural observations
- Recommendations by workload pattern

Exit criteria:

- Report identifies strengths, weaknesses, and bottlenecks for each engine.
- Results are reproducible from committed configs and run commands.

## Suggested Milestones

### Milestone 1: Benchmark Spec Complete

Duration: 1 week

Output:

- Workload definitions
- Metric dictionary
- Comparison methodology

### Milestone 2: All Engines Running Locally

Duration: 1 to 2 weeks

Output:

- Containerized setup
- Smoke test workflows

### Milestone 3: Harness and Metrics MVP

Duration: 2 weeks

Output:

- CLI runner
- Mock services
- Normalized result output
- Prometheus metrics collection

### Milestone 4: First Comparable Benchmark

Duration: 2 weeks

Output:

- Low-latency and high-throughput workloads implemented across all engines
- First comparative charts

### Milestone 5: Full Workload Coverage

Duration: 3 to 4 weeks

Output:

- All seven workload categories implemented
- Failure/recovery scenarios validated

### Milestone 6: Final Report

Duration: 1 to 2 weeks

Output:

- Final benchmark report
- Reproducibility instructions
- Recommendations by workload category

Estimated total duration: 10 to 13 weeks.

## Important Design Decisions

- Use one fixed local machine profile for primary comparisons.
- Run all engines with explicit CPU and memory limits.
- Separate cold-start results from steady-state results.
- Use repeated runs and report variance.
- Avoid declaring a universal winner; compare engines by workload category.
- Document places where Airflow's DAG-oriented model does not map cleanly to request/response workflow patterns.
- Treat engine-specific best practices fairly, while keeping workload semantics consistent.

## Risks and Mitigations

### Risk: Apples-to-oranges workflow implementations

Mitigation:

- Define workload semantics first.
- Keep engine-specific code as thin as possible.
- Document unavoidable architectural differences.

### Risk: Local machine limits dominate results

Mitigation:

- Capture machine and container resource profiles.
- Report saturation point separately from efficiency metrics.
- Use parameter sweeps instead of one fixed load.

### Risk: Airflow may not fit low-latency orchestration patterns

Mitigation:

- Include Airflow for comparison, but explicitly flag model mismatch.
- Evaluate it more heavily in batch, deep-step, and scheduled workflow scenarios.

### Risk: Metrics differ across engines

Mitigation:

- Define normalized metrics that the harness measures directly.
- Use engine-native metrics only as supporting diagnostics.

### Risk: Benchmark results are noisy

Mitigation:

- Warm up each engine.
- Repeat runs.
- Separate setup time from execution time.
- Record variance and confidence ranges.

## Recommended First Sprint

1. Finalize workload parameters for the first two scenarios: low-latency and high-throughput.
2. Build Docker Compose setup for Temporal, Conductor, and Airflow.
3. Implement one smoke-test workflow per engine.
4. Build the mock RPC service with configurable latency and failure rate.
5. Define the normalized result schema.
6. Implement the first version of the benchmark runner.
7. Run a tiny comparison, such as 100 workflows per engine, to validate instrumentation before scaling load.

## Final Deliverables

- Reproducible benchmark repository
- Engine setup scripts and Docker Compose files
- Workload specification
- Metrics schema
- Benchmark runner
- Raw result files
- Generated charts
- Final benchmark report with recommendations

