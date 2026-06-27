#!/usr/bin/env bash
# run-cloud-benchmarks.sh
# Run the full 3-engine × 3-config × 7-workload benchmark suite.
# Usage: bash run-cloud-benchmarks.sh
# Expected environment: Ubuntu 24.04, Docker installed, npm ci already run.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="$REPO_DIR/bench-run.log"
ENGINES=(temporal conductor airflow)
PROBLEMS=(low-latency high-throughput deep-sequential long-running retry-heavy timer-intensive failure-recovery)

# Concurrency and run counts per problem (match local runs for comparability)
declare -A TOTAL=(
  [low-latency]=100 [high-throughput]=20 [deep-sequential]=20
  [long-running]=50 [retry-heavy]=50 [timer-intensive]=100 [failure-recovery]=100
)
declare -A CONCURRENCY=(
  [low-latency]=10 [high-throughput]=5 [deep-sequential]=5
  [long-running]=10 [retry-heavy]=10 [timer-intensive]=10 [failure-recovery]=10
)

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }

kill_metrics_port() {
  lsof -ti :9464 | xargs -r kill -9 2>/dev/null || true
}

run_engine() {
  local engine=$1 config=$2
  local compose_file="$REPO_DIR/docker-compose.${engine}.yml"
  local results_dir="$REPO_DIR/results/${config}"
  mkdir -p "$results_dir"

  log "=== $engine / $config START ==="
  docker compose -f "$compose_file" down -v --remove-orphans 2>>"$LOG" || true
  docker compose -f "$compose_file" up -d 2>>"$LOG"

  # Wait for engine to be healthy
  log "Waiting for $engine to be ready..."
  sleep 30

  for problem in "${PROBLEMS[@]}"; do
    local total="${TOTAL[$problem]}"
    local concurrency="${CONCURRENCY[$problem]}"
    kill_metrics_port

    log "  Running: $engine / $problem (n=$total c=$concurrency)"
    if npx tsx "$REPO_DIR/src/runner.ts" \
        --engine "$engine" \
        --problem "$problem" \
        --total "$total" \
        --concurrency "$concurrency" \
        --results-dir "$results_dir" >> "$LOG" 2>&1; then
      log "  OK: $engine / $problem"
    else
      log "  FAILED: $engine / $problem (exit $?)"
    fi
    sleep 5
  done

  log "=== $engine / $config DONE ==="
  docker compose -f "$compose_file" down -v 2>>"$LOG"
  sleep 15
}

# ── Config 1: baseline (docker-compose files as-is — 2 CPU / 2g workers) ─────
log "====== CONFIG 1 (baseline) ======"
for engine in "${ENGINES[@]}"; do
  run_engine "$engine" "config1"
done

# ── Config 2: patch docker-compose files to 4 CPU / 4g, double concurrency ───
log "====== CONFIG 2 (2× resources) ======"
for file in docker-compose.temporal.yml docker-compose.conductor.yml docker-compose.airflow.yml; do
  sed -i "s/cpus: '2'/cpus: '4'/g; s/cpus: '4'/cpus: '4'/g; \
          s/mem_limit: 2g/mem_limit: 4g/g; \
          s/TEMPORAL_MAX_ACTIVITIES=200/TEMPORAL_MAX_ACTIVITIES=400/g; \
          s/TEMPORAL_MAX_WORKFLOWS=200/TEMPORAL_MAX_WORKFLOWS=400/g; \
          s/CONDUCTOR_POLL_COUNT=20/CONDUCTOR_POLL_COUNT=40/g; \
          s/PARALLELISM.*\"200\"/PARALLELISM: \"400\"/g" "$REPO_DIR/$file"
done
for engine in "${ENGINES[@]}"; do
  run_engine "$engine" "config2"
done

# ── Config 3: patch to 6 CPU / 8g, double concurrency again ──────────────────
log "====== CONFIG 3 (4× resources) ======"
for file in docker-compose.temporal.yml docker-compose.conductor.yml docker-compose.airflow.yml; do
  sed -i "s/cpus: '4'/cpus: '6'/g; \
          s/mem_limit: 4g/mem_limit: 8g/g; \
          s/mem_limit: 2g/mem_limit: 4g/g; \
          s/TEMPORAL_MAX_ACTIVITIES=400/TEMPORAL_MAX_ACTIVITIES=800/g; \
          s/TEMPORAL_MAX_WORKFLOWS=400/TEMPORAL_MAX_WORKFLOWS=800/g; \
          s/CONDUCTOR_POLL_COUNT=40/CONDUCTOR_POLL_COUNT=80/g; \
          s/PARALLELISM.*\"400\"/PARALLELISM: \"800\"/g" "$REPO_DIR/$file"
done
for engine in "${ENGINES[@]}"; do
  run_engine "$engine" "config3"
done

log "====== ALL CONFIGS COMPLETE ======"
log "Results in: $REPO_DIR/results/"
find "$REPO_DIR/results" -name "*.json" | wc -l | xargs -I{} log "Total result files: {}"
