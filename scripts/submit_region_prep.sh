#!/bin/bash
#SBATCH --job-name=region-prep
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=16G
#SBATCH --output=logs/region-prep-%j.out
#SBATCH --error=logs/region-prep-%j.err
#SBATCH --profile=task

# ----------------------------------------------------------
# Load common HPC functions
# ----------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/hpc_common.sh"

# ----------------------------------------------------------
# Setup environment
# ----------------------------------------------------------
ENV_NAME="data_prep_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

echo "========================================="
echo "Job: Region Preparation"
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
# un region preparation
# ----------------------------------------------------------
R_SCRIPT="$SLURM_SUBMIT_DIR/run_region_prep.r"

if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_region_prep.r not found at: $R_SCRIPT"
    exit 1
fi

echo "✓ Running region preparation: $R_SCRIPT"
"$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
exit $EXIT_CODE