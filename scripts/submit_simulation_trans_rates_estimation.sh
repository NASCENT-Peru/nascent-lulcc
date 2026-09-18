#!/bin/bash
#SBATCH --job-name=sim-trans-rates-prep
# Reserve the whole Rundeck node: --exclusive + --mem=0 give the job all cores and
# all RAM on the reserved node. No --partition: the Rundeck reservation places the job.
#SBATCH --time=04:00:00
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --cpus-per-task=6
#SBATCH --output=logs/sim-trans-rates-prep-%j.out
#SBATCH --error=logs/sim-trans-rates-prep-%j.err
#SBATCH --profile=task

# ----------------------------------------------------------
# Load common HPC functions
# ----------------------------------------------------------
SCRIPT_DIR="$SLURM_SUBMIT_DIR/scripts"
source "$SCRIPT_DIR/hpc_common.sh"

# ----------------------------------------------------------
# Setup environment
# ----------------------------------------------------------
ENV_NAME="trans_rate_estimation_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

echo "========================================="
echo "Simulation Transition Rates Preparation"
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
# Run simulation transition rates preparation
# ----------------------------------------------------------
R_SCRIPT="$SCRIPT_DIR/run_simulation_trans_rates_prep.r"

if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_simulation_trans_rates_prep.r not found at: $R_SCRIPT"
    exit 1
fi

echo "✓ Running simulation transition rates preparation: $R_SCRIPT"
"$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
exit $EXIT_CODE