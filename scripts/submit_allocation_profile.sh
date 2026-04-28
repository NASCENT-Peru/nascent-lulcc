#!/bin/bash
#SBATCH --job-name=lulc-allocation-profile
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=8G
#SBATCH --output=logs/lulc-allocation-profile-%j.out
#SBATCH --error=logs/lulc-allocation-profile-%j.err
#SBATCH --profile=task

# ----------------------------------------------------------
# Load common HPC functions
# ----------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/hpc_common.sh"

# ----------------------------------------------------------
# Setup environment
# ----------------------------------------------------------
ENV_NAME="transition_model_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

echo "========================================="
echo "Job: LULC Allocation Profiling"
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

export ALLOCATION_PROFILE=TRUE
export ALLOCATION_PROFILE_SCENARIO=${ALLOCATION_PROFILE_SCENARIO:-BAU}
export ALLOCATION_PROFILE_TIMESTEP_INDEX=${ALLOCATION_PROFILE_TIMESTEP_INDEX:-1}

echo "Profile scenario: $ALLOCATION_PROFILE_SCENARIO"
echo "Profile timestep index: $ALLOCATION_PROFILE_TIMESTEP_INDEX"
echo "Allocated CPUs: ${SLURM_CPUS_PER_TASK:-8}"
echo

print_slurm_mem_snapshot() {
    local label=$1
    echo "----- SLURM memory snapshot ($label) -----"
    if command -v sstat >/dev/null 2>&1; then
        echo "[sstat — running step]"
        sstat -j "$SLURM_JOB_ID" \
            --format=JobID,MaxRSS,AveRSS,MaxVMSize \
            --units=M 2>&1 || true
    fi
    if command -v sacct >/dev/null 2>&1; then
        echo "[sacct — completed steps]"
        sacct -j "$SLURM_JOB_ID" \
            --format=JobID,JobName,MaxRSS,AveRSS,MaxVMSize,Elapsed,State \
            --units=M 2>&1 || true
    fi
    echo "------------------------------------------"
}

run_profile_pass() {
    local run_label=$1
    local worker_count=$2

    export ALLOCATION_PROFILE_RUN_LABEL="$run_label"
    export ALLOCATION_NUM_WORKERS="$worker_count"

    echo "-----------------------------------------"
    echo "Starting profile pass: $run_label"
    echo "ALLOCATION_NUM_WORKERS=$ALLOCATION_NUM_WORKERS"
    echo "-----------------------------------------"

    "$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
    local exit_code=$?

    echo
    echo "Profile pass '$run_label' exit code: $exit_code"
    print_slurm_mem_snapshot "after pass=$run_label"
    echo

    return $exit_code
}

run_profile_pass serial 1
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    run_profile_pass parallel "${SLURM_CPUS_PER_TASK:-8}"
    EXIT_CODE=$?
fi

print_slurm_mem_snapshot "final"

exit $EXIT_CODE
