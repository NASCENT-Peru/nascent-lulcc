#!/bin/bash
#SBATCH --job-name=lulc-allocation-smoke
# Routed to highmem: 4 x 32G = 128G exceeds the 95000 MB compute cap.
#SBATCH --partition=highmem
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=32G
#SBATCH --output=logs/lulc-allocation-smoke-%j.out
#SBATCH --error=logs/lulc-allocation-smoke-%j.err
#SBATCH --profile=task

# NOTE: The #SBATCH directives above apply ONLY when this script is launched via
# `sbatch`. On the Rundeck-allocated HPC, you reserve an exclusive node, SSH in,
# and run this script directly (`bash scripts/submit_allocation_smoke.sh`); the
# #SBATCH lines are then inert. The allocation run is effectively single-threaded
# (ALLOCATION_PARALLEL_STRATEGY=sequential + native threads pinned to 1), so on an
# exclusive node you get the whole node's RAM regardless of the cpu/mem directives.
# Pick the node by region size: forest-dominated regions (cuenca_del_amazonas,
# selva_andina) preload a ~80GB predictor table and peak >128GB on the big
# forest->* transition — run those on a fat/highmem node, not a ~93GB compute node.

if [ -n "$SLURM_SUBMIT_DIR" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR/scripts"
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
source "$SCRIPT_DIR/hpc_common.sh"

# Project root: prefer SLURM_SUBMIT_DIR (sbatch), else the parent of scripts/
# (direct run on a Rundeck-allocated node, where SLURM_* vars are unset).
if [ -n "$SLURM_SUBMIT_DIR" ]; then
    PROJECT_ROOT="$SLURM_SUBMIT_DIR"
else
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
fi
export PROJECT_ROOT
cd "$PROJECT_ROOT" || { echo "ERROR: cannot cd to PROJECT_ROOT: $PROJECT_ROOT"; exit 1; }

ENV_NAME="allocation_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

echo "========================================="
echo "Job: Phase 3 Allocation Smoke Run"
echo "========================================="
echo "Environment: $ENV_NAME"
echo "Path: $ENV_PATH"
echo

setup_common_env
activate_env "$ENV_PATH"
echo

RSCRIPT_BIN=$(verify_rscript "$ENV_PATH")
if [ $? -ne 0 ]; then
    exit 1
fi
echo

R_SCRIPT="$PROJECT_ROOT/scripts/run_allocation.r"
if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_allocation.r not found at: $R_SCRIPT"
    exit 1
fi

DEFAULT_YEAR_POST=$("$RSCRIPT_BIN" --vanilla -e "setwd(Sys.getenv('PROJECT_ROOT')); source('src/setup.r'); cfg <- get_config(); cat(cfg[['simulation_start_year']] + cfg[['step_length']])" 2>/dev/null | grep -E '^[0-9]+$' | tail -1)
if [ -z "${ALLOCATION_YEAR_POST_FILTER:-}" ]; then
    export ALLOCATION_YEAR_POST_FILTER="$DEFAULT_YEAR_POST"
fi

export ALLOCATION_PROFILE=TRUE
export ALLOCATION_PARALLEL_STRATEGY=sequential
export ALLOCATION_DEV_STRICT_GLOBALS=TRUE
export ALLOCATION_NUM_WORKERS=${SLURM_CPUS_PER_TASK:-4}
export ALLOCATION_PROFILE_SCENARIO=${ALLOCATION_PROFILE_SCENARIO:-BAU}
export ALLOCATION_REGION_FILTER=${ALLOCATION_REGION_FILTER:-costa_peruana}
export ALLOCATION_WORKER_RSS_BUDGET_MB=${ALLOCATION_WORKER_RSS_BUDGET_MB:-16384}
# Optional memory-bounded prediction (Phase 3.4). Unset = original single-shot
# prediction. Set (e.g. ALLOCATION_PREDICT_BATCH_ROWS=5000000) to predict large
# "from"-class transitions in row-batches and cap prediction-time peak RSS — needed
# for cuenca_del_amazonas / selva_andina on memory-constrained nodes. Passed through
# from the environment; left unexported here so the default stays single-shot.

echo "ALLOCATION_PROFILE=$ALLOCATION_PROFILE"
echo "ALLOCATION_PARALLEL_STRATEGY=$ALLOCATION_PARALLEL_STRATEGY"
echo "ALLOCATION_DEV_STRICT_GLOBALS=$ALLOCATION_DEV_STRICT_GLOBALS"
echo "ALLOCATION_NUM_WORKERS=$ALLOCATION_NUM_WORKERS"
echo "ALLOCATION_PROFILE_SCENARIO=$ALLOCATION_PROFILE_SCENARIO"
echo "ALLOCATION_REGION_FILTER=$ALLOCATION_REGION_FILTER"
echo "ALLOCATION_YEAR_POST_FILTER=$ALLOCATION_YEAR_POST_FILTER"
echo "ALLOCATION_WORKER_RSS_BUDGET_MB=$ALLOCATION_WORKER_RSS_BUDGET_MB"
echo "ALLOCATION_PREDICT_BATCH_ROWS=${ALLOCATION_PREDICT_BATCH_ROWS:-<unset: single-shot>}"
echo

RUN_ID="${SLURM_JOB_ID:-$(date +%Y%m%d-%H%M%S)-${ALLOCATION_REGION_FILTER:-allregions}}"

if [ -z "${SLURM_JOB_ID:-}" ]; then
    # Direct run on a Rundeck-allocated node: no scheduler captures stdout, so
    # tee the run to a log alongside where #SBATCH --output would have written.
    mkdir -p logs
    NODE_LOG="logs/lulc-allocation-smoke-${RUN_ID}.out"
    echo "Direct run (no SLURM): teeing stdout+stderr to $NODE_LOG"
    set -o pipefail
    "$RSCRIPT_BIN" --vanilla "$R_SCRIPT" 2>&1 | tee "$NODE_LOG"
    EXIT_CODE=${PIPESTATUS[0]}
else
    "$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
    EXIT_CODE=$?
fi

echo
echo "Rscript exit code: $EXIT_CODE"
echo "Verification: bash scripts/verify_phase3_smoke.sh ${SLURM_JOB_ID:-$RUN_ID} ${ALLOCATION_WORKER_RSS_BUDGET_MB}"
exit $EXIT_CODE
