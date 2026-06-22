#!/bin/bash
#SBATCH --job-name=feat-select
# Reserve the whole Rundeck node: --exclusive + --mem=0 give the job all cores and
# all RAM on the reserved node. No --partition: the Rundeck reservation places the
# job. Reserve highmem-exclusive (188GB) or larger.
#SBATCH --time=72:00:00
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --cpus-per-task=4
#SBATCH --output=logs/feat-select-%j.out
#SBATCH --error=logs/feat-select-%j.err
#SBATCH --profile=task
# ----------------------------------------------------------
# Load common HPC functions
# ----------------------------------------------------------
SCRIPT_DIR="$SLURM_SUBMIT_DIR/scripts"
source "$SCRIPT_DIR/hpc_common.sh"

# ----------------------------------------------------------
# Setup environment
# ----------------------------------------------------------
ENV_NAME="feat_select_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

echo "========================================="
echo "Job: feat_select_env"
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
# ----------------------------------------------------------
# un your R pipeline script
# ----------------------------------------------------------
R_SCRIPT="$SLURM_SUBMIT_DIR/scripts/run_feature_selection.r"

if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_feature_selection.r not found at: $R_SCRIPT"
    exit 1
fi

echo "? Running pipeline: $R_SCRIPT"
"$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
exit $EXIT_CODE
