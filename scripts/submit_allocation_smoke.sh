#!/bin/bash
#SBATCH --job-name=lulc-allocation-smoke
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=8G
#SBATCH --output=logs/lulc-allocation-smoke-%j.out
#SBATCH --error=logs/lulc-allocation-smoke-%j.err
#SBATCH --profile=task

if [ -n "$SLURM_SUBMIT_DIR" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR/scripts"
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
source "$SCRIPT_DIR/hpc_common.sh"

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

R_SCRIPT="$SLURM_SUBMIT_DIR/scripts/run_allocation.r"
if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_allocation.r not found at: $R_SCRIPT"
    exit 1
fi

DEFAULT_YEAR_POST=$("$RSCRIPT_BIN" --vanilla -e "setwd(Sys.getenv('SLURM_SUBMIT_DIR')); source('src/setup.r'); cfg <- get_config(); cat(cfg[['simulation_start_year']] + cfg[['step_length']])")
if [ -z "${ALLOCATION_YEAR_POST_FILTER:-}" ]; then
    export ALLOCATION_YEAR_POST_FILTER="$DEFAULT_YEAR_POST"
fi

export ALLOCATION_PROFILE=TRUE
export ALLOCATION_PARALLEL_STRATEGY=multicore
export ALLOCATION_DEV_STRICT_GLOBALS=TRUE
export ALLOCATION_NUM_WORKERS=${SLURM_CPUS_PER_TASK:-4}
export ALLOCATION_PROFILE_SCENARIO=${ALLOCATION_PROFILE_SCENARIO:-BAU}
export ALLOCATION_REGION_FILTER=${ALLOCATION_REGION_FILTER:-Coast}
export ALLOCATION_WORKER_RSS_BUDGET_MB=${ALLOCATION_WORKER_RSS_BUDGET_MB:-16384}

echo "ALLOCATION_PROFILE=$ALLOCATION_PROFILE"
echo "ALLOCATION_PARALLEL_STRATEGY=$ALLOCATION_PARALLEL_STRATEGY"
echo "ALLOCATION_DEV_STRICT_GLOBALS=$ALLOCATION_DEV_STRICT_GLOBALS"
echo "ALLOCATION_NUM_WORKERS=$ALLOCATION_NUM_WORKERS"
echo "ALLOCATION_PROFILE_SCENARIO=$ALLOCATION_PROFILE_SCENARIO"
echo "ALLOCATION_REGION_FILTER=$ALLOCATION_REGION_FILTER"
echo "ALLOCATION_YEAR_POST_FILTER=$ALLOCATION_YEAR_POST_FILTER"
echo "ALLOCATION_WORKER_RSS_BUDGET_MB=$ALLOCATION_WORKER_RSS_BUDGET_MB"
echo

"$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
echo "Verification: bash scripts/verify_phase3_smoke.sh $SLURM_JOB_ID ${ALLOCATION_WORKER_RSS_BUDGET_MB}"
exit $EXIT_CODE
