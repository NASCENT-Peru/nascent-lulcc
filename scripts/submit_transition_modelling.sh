#!/bin/bash
#SBATCH --job-name=trans-model
# Reserve the whole Rundeck node: --exclusive + --mem=0 give the job all cores and
# all RAM on the reserved node. No --partition: the Rundeck reservation places the
# job. Reserve highmem-exclusive (188GB) or larger.
#SBATCH --time=72:00:00
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --cpus-per-task=3
#SBATCH --output=logs/trans-model-%j.out
#SBATCH --error=logs/trans-model-%j.err
#SBATCH --profile=task

# ----------------------------------------------------------
# Load common HPC functions
# ----------------------------------------------------------
SCRIPT_DIR="$SLURM_SUBMIT_DIR/scripts"
source "$SCRIPT_DIR/hpc_common.sh"

# ----------------------------------------------------------
# Setup environment
# ----------------------------------------------------------
ENV_NAME="transition_model_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

echo "========================================="
echo "Job: transition_model_env"
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
# un transition modelling pipeline
# ----------------------------------------------------------
R_SCRIPT="$SLURM_SUBMIT_DIR/scripts/run_transition_modelling.r"

if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_transition_modelling.r not found in scripts directory"
    exit 1
fi

echo "✓ Running transition modelling pipeline: $R_SCRIPT"
"$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
exit $EXIT_CODE