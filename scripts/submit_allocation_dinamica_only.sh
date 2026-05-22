#!/bin/bash
#SBATCH --job-name=lulc-dinamica-only
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=8G
#SBATCH --output=logs/lulc-dinamica-only-%j.out
#SBATCH --error=logs/lulc-dinamica-only-%j.err

# Smoke test for the Dinamica allocation step only.
# Reuses the probability maps from a prior full run (job 364249 by default).
# Set ALLOCATION_WORK_DIR before submitting to point at a different run.
#
# Usage:
#   sbatch scripts/submit_allocation_dinamica_only.sh
#
# Prerequisites:
#   DINAMICA_EGO_8_HOME must be set in your environment or .env before submitting:
#     export DINAMICA_EGO_8_HOME=/path/to/dinamica.sif
#     sbatch scripts/submit_allocation_dinamica_only.sh

if [ -n "$SLURM_SUBMIT_DIR" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR/scripts"
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
source "$SCRIPT_DIR/hpc_common.sh"

ENV_NAME="allocation_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

echo "========================================="
echo "Job: Dinamica-only allocation smoke test"
echo "========================================="

setup_common_env
activate_env "$ENV_PATH"

RSCRIPT_BIN=$(verify_rscript "$ENV_PATH")
if [ $? -ne 0 ]; then exit 1; fi

# DINAMICA_EGO_8_HOME must be set by the operator (see .env.template).
if [ -z "${DINAMICA_EGO_8_HOME:-}" ]; then
    echo "ERROR: DINAMICA_EGO_8_HOME is not set."
    echo "       Set it to the absolute path of the Dinamica .sif image before submitting."
    exit 1
fi

# Default work_dir: costa_peruana region from job 364249.
# Override by setting ALLOCATION_WORK_DIR in the environment before sbatch.
DEFAULT_WORK_DIR="$HPC_SCRATCH_ROOT/outputs/simulations/BAU/2026/region_costa_peruana"
export ALLOCATION_WORK_DIR="${ALLOCATION_WORK_DIR:-$DEFAULT_WORK_DIR}"
export DINAMICA_BACKEND="${DINAMICA_BACKEND:-hpc}"

echo "DINAMICA_EGO_8_HOME : $DINAMICA_EGO_8_HOME"
echo "DINAMICA_BACKEND    : $DINAMICA_BACKEND"
echo "ALLOCATION_WORK_DIR : $ALLOCATION_WORK_DIR"
echo

"$RSCRIPT_BIN" --vanilla "$SLURM_SUBMIT_DIR/scripts/run_allocation_dinamica_only.r"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
exit $EXIT_CODE
