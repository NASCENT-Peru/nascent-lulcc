#!/bin/bash
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: bash scripts/verify_phase3_smoke.sh <job_id> [budget_mb]" >&2
    exit 2
fi

JOB_ID="$1"
BUDGET_MB="${2:-16384}"
JOB_STDOUT="logs/lulc-allocation-smoke-${JOB_ID}.out"
JOB_STDERR="logs/lulc-allocation-smoke-${JOB_ID}.err"

if [ ! -f "$JOB_STDOUT" ]; then
    echo "ERROR: expected stdout log not found: $JOB_STDOUT" >&2
    exit 1
fi

SACCT_LINE=$(sacct -j "$JOB_ID" --format=State,ExitCode --noheader | awk 'NF {print; exit}')
JOB_STATE=$(echo "$SACCT_LINE" | awk '{print $1}')
EXIT_CODE=$(echo "$SACCT_LINE" | awk '{print $2}')
if [ "$JOB_STATE" != "COMPLETED" ] || [ "$EXIT_CODE" != "0:0" ]; then
    echo "ERROR: sacct reported state=$JOB_STATE exit_code=$EXIT_CODE" >&2
    exit 1
fi

for pattern in \
    "Parallel: strategy=multicore" \
    "future.globals.onReference = 'error' enabled" \
    "ALLOCATION_REGION_FILTER=" \
    "ALLOCATION_YEAR_POST_FILTER="
do
    if ! grep -q "$pattern" "$JOB_STDOUT"; then
        echo "ERROR: missing stdout marker '$pattern'" >&2
        exit 1
    fi
done

for forbidden in "oom-kill" "MulticoreFuture interrupted"; do
    if grep -q "$forbidden" "$JOB_STDOUT" "$JOB_STDERR"; then
        echo "ERROR: found forbidden marker '$forbidden' in job output" >&2
        exit 1
    fi
done

SCENARIO=$(awk -F= '/ALLOCATION_PROFILE_SCENARIO=/{print $2; exit}' "$JOB_STDOUT")
REGION_LABEL=$(awk -F= '/ALLOCATION_REGION_FILTER=/{print $2; exit}' "$JOB_STDOUT")
YEAR_POST=$(awk -F= '/ALLOCATION_YEAR_POST_FILTER=/{print $2; exit}' "$JOB_STDOUT")
REGION_SUFFIX=$(echo "$REGION_LABEL" | tr '[:upper:]' '[:lower:]' | tr ' ' '_')

readarray -t PATH_INFO < <(Rscript --vanilla -e "setwd('.'); source('src/setup.r'); cfg <- get_config(); cat(file.path(cfg[['simulation_output_dir']], '$SCENARIO', '$YEAR_POST', paste0('posterior_', '$YEAR_POST', '.tif')), '\n'); cat(file.path(cfg[['simulation_output_dir']], '$SCENARIO', '$YEAR_POST', paste0('region_', '$REGION_SUFFIX'), 'worker_logs'), '\n')")
POSTERIOR_TIF="${PATH_INFO[0]}"
REGION_LOG_DIR="${PATH_INFO[1]}"

if [ ! -d "$REGION_LOG_DIR" ]; then
    echo "ERROR: region log directory not found: $REGION_LOG_DIR" >&2
    exit 1
fi

REGION_LOG=$(ls -1t "$REGION_LOG_DIR"/*.log 2>/dev/null | head -n1 || true)
if [ -z "$REGION_LOG" ]; then
    echo "ERROR: no worker log found under $REGION_LOG_DIR" >&2
    exit 1
fi

for pattern in \
    "stage=pin_native_threads_to_one" \
    "stage=preload_models" \
    "stage=nhood_precompute" \
    "stage=parent_baseline" \
    "CGROUP"
do
    if ! grep -q "$pattern" "$REGION_LOG"; then
        echo "ERROR: missing region-log marker '$pattern' in $REGION_LOG" >&2
        exit 1
    fi
done

for forbidden in "oom-kill" "MulticoreFuture interrupted" "SENTINEL reason=incomplete"; do
    if grep -q "$forbidden" "$JOB_STDOUT" "$JOB_STDERR" "$REGION_LOG"; then
        echo "ERROR: found forbidden marker '$forbidden'" >&2
        exit 1
    fi
done

MAX_PEAK_RSS=$(grep -o 'peak_rss=[0-9.]*MB' "$REGION_LOG" | sed 's/peak_rss=//; s/MB//' | sort -nr | head -n1 || true)
if [ -z "$MAX_PEAK_RSS" ]; then
    echo "ERROR: no peak_rss markers found in $REGION_LOG" >&2
    exit 1
fi

if ! awk -v observed="$MAX_PEAK_RSS" -v budget="$BUDGET_MB" 'BEGIN { exit !(observed <= budget) }'; then
    echo "ERROR: peak_rss ${MAX_PEAK_RSS}MB exceeds budget ${BUDGET_MB}MB" >&2
    exit 1
fi

if [ ! -f "$POSTERIOR_TIF" ]; then
    echo "ERROR: posterior raster not found: $POSTERIOR_TIF" >&2
    exit 1
fi

Rscript --vanilla -e "terra::rast('$POSTERIOR_TIF'); cat('posterior-ok\n')"

echo "PASS strategy=multicore state=$JOB_STATE peak_rss_mb=$MAX_PEAK_RSS output=$POSTERIOR_TIF"
