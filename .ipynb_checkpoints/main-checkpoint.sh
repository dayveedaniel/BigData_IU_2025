#!/bin/bash
set -euo pipefail

# Check if the first argument is "true" to decide whether to run the benchmark
# Default to "false" if no argument is provided or if it's not "true"
RUN_BENCHMARK_FLAG="${1:-false}"

echo "Starting data pipeline..."

bash scripts/01_data_collection.sh
bash scripts/02_data_storage.sh


# Optionally run the benchmark script
if [[ "${RUN_BENCHMARK_FLAG}" == "true" ]]; then
  echo "Running benchmark ..."
  if [[ -f "scripts/run_benchmark.sh" ]]; then
    bash "scripts/run_benchmark.sh"
    echo "Benchmark completed."
  else
    echo "WARNING: Benchmark script scripts/run_benchmark.sh not found. Skipping." >&2
  fi
else
  echo "Skipping benchmark."
fi


bash scripts/03_stage2.sh
bash scripts/04_stage3.sh

echo "Data pipeline completed successfully!"