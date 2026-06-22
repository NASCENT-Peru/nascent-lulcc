#!/bin/bash
#SBATCH --job-name=lulc-allocation
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=8G
#SBATCH --output=logs/lulc-allocation-%j.out
#SBATCH --error=logs/lulc-allocation-%j.err
#SBATCH --profile=task

# ----------------------------------------------------------
# Load common HPC functions
# ----------------------------------------------------------
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

# ----------------------------------------------------------
# Setup environment
# ----------------------------------------------------------
# Stage 7 canonical environment per MEM-06. The package contract lives in
# environments/allocation_env.yml; pre-flight (later Phase 1 plan) validates
# this same env, so the submit script MUST activate it rather than a drifted
# training env (Phase 1 RESEARCH Pitfall 2).
ENV_NAME="allocation_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

echo "========================================="
echo "Job: LULC Allocation Simulations"
echo "========================================="
echo "Environment: $ENV_NAME"
echo "Path: $ENV_PATH"
echo

# Setup common environment variables
setup_common_env

# Activate environment
activate_env "$ENV_PATH"
echo

# Verify Rscript
RSCRIPT_BIN=$(verify_rscript "$ENV_PATH")
if [ $? -ne 0 ]; then
    exit 1
fi
echo

# Set number of parallel workers for region processing
export ALLOCATION_NUM_WORKERS=${SLURM_CPUS_PER_TASK:-4}
echo "Parallel workers: $ALLOCATION_NUM_WORKERS"

# ----------------------------------------------------------
# Run allocation simulations
# ----------------------------------------------------------
R_SCRIPT="$PROJECT_ROOT/scripts/run_allocation.r"

if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_allocation.r not found at: $R_SCRIPT"
    exit 1
fi

echo "✓ Running allocation simulations: $R_SCRIPT"
if [ -z "${SLURM_JOB_ID:-}" ]; then
    # Direct run on a Rundeck-allocated node: no scheduler captures stdout, so
    # tee the run to a log (the #SBATCH --output directive is inert here).
    mkdir -p logs
    NODE_LOG="logs/lulc-allocation-$(date +%Y%m%d-%H%M%S).out"
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
exit $EXIT_CODE
