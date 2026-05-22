#!/bin/bash
#SBATCH --job-name=lulc-dinamica-only
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem-per-cpu=4G
#SBATCH --output=logs/lulc-dinamica-only-%j.out
#SBATCH --error=logs/lulc-dinamica-only-%j.err

# Smoke test for the Dinamica allocation step only.
# Reuses the probability maps from a prior full run (job 364249 by default).
# Set ALLOCATION_WORK_DIR before submitting to point at a different run.
#
# CPU/memory sizing: Dinamica auto-detects available cores for its parallel map
# thread pool. With --cpus-per-task=16 Euler's cpuset limits Dinamica to 16
# workers (16 × 4 GB = 64 GB total). Increase --cpus-per-task proportionally
# if you observe memory pressure in the SLURM accounting.
#
# Time: the Create Cube of Probability Maps step takes ~20s per transition
# per CPU (28 transitions × ~20s / 16 CPUs ≈ 35 min). 4 h gives headroom.
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

# Expose allocated core count to thread-aware libraries inside the container.
# Dinamica reads available CPUs from the OS; SLURM's cpuset limits visibility
# to SLURM_CPUS_PER_TASK cores so Dinamica auto-detects the right count.
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-16}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK:-16}

echo "DINAMICA_EGO_8_HOME : $DINAMICA_EGO_8_HOME"
echo "DINAMICA_BACKEND    : $DINAMICA_BACKEND"
echo "ALLOCATION_WORK_DIR : $ALLOCATION_WORK_DIR"
echo "SLURM_CPUS_PER_TASK : ${SLURM_CPUS_PER_TASK:-16}"
echo

"$RSCRIPT_BIN" --vanilla "$SLURM_SUBMIT_DIR/scripts/run_allocation_dinamica_only.r"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
exit $EXIT_CODE
