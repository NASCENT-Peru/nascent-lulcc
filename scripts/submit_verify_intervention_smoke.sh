#!/bin/bash
#SBATCH --job-name=iv-verify
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=48G
#SBATCH --output=logs/iv-verify-%j.out
#SBATCH --error=logs/iv-verify-%j.err

# Batch wrapper for scripts/verify_intervention_smoke.r.
#
# WHY THIS EXISTS: docs/README_HPC.md shows the verifier (and
# scripts/validate_intervention_masks.r) being run as a bare interactive
# `Rscript` call on login02. That guidance is outdated — the verifier opens
# every per-transition probability map in a region and crops the national masks
# against them, which is real compute, and the HPC manager sends warning emails
# for work run on the login node. Submit it instead.
#
# Do NOT hand-roll this as `sbatch --wrap="... micromamba activate ..."`: the
# micromamba binary is not at a fixed path on this cluster, and hpc_common.sh's
# find_micromamba() is the only thing that locates it reliably.
#
# Standard launch, from the repo root on login02:
#
#   source .env
#   sbatch scripts/submit_verify_intervention_smoke.sh \
#     --scenario NAT --region andes --year 2028 \
#     --extra-log logs/lulc-allocation-smoke-<job_id>.out
#
# Every argument after the script name is forwarded verbatim to the R script.
#
# No --partition: 48G / 2 cpus fits the ordinary `compute` partition (93GB).
# The verifier has no predictor preload, so it does not need highmem or fat
# even for andes or cuenca_del_amazonas.
#
# `source .env` is required before submitting: setup_common_env() gates on the
# Stage 7 path contract (HPC_SCRATCH_ROOT, TERRA_TEMP, HPC_TMP_ROOT, ...) and
# refuses to run with partial paths, so an unsourced shell fails in seconds.
#
# Exit status is the verifier's own: 0 PASS, 1 FAIL, 2 usage or sourcing error.

if [ -n "${SLURM_SUBMIT_DIR:-}" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR/scripts"
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
source "$SCRIPT_DIR/hpc_common.sh"

if [ -n "${SLURM_SUBMIT_DIR:-}" ]; then
    PROJECT_ROOT="$SLURM_SUBMIT_DIR"
else
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
fi
export PROJECT_ROOT
cd "$PROJECT_ROOT" || { echo "ERROR: cannot cd to PROJECT_ROOT: $PROJECT_ROOT"; exit 1; }

ENV_NAME="allocation_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

echo "========================================="
echo "Job: Intervention smoke verifier"
echo "========================================="
echo "Environment: $ENV_NAME"
echo "Path: $ENV_PATH"
echo "Args: $*"
echo

setup_common_env
activate_env "$ENV_PATH"
echo

RSCRIPT_BIN=$(verify_rscript "$ENV_PATH")
if [ $? -ne 0 ]; then
    exit 1
fi
echo

R_SCRIPT="$PROJECT_ROOT/scripts/verify_intervention_smoke.r"
if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: verify_intervention_smoke.r not found at: $R_SCRIPT"
    exit 1
fi

# The verifier is I/O bound; keep native pools single-threaded on a shared node.
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export GDAL_NUM_THREADS=1

"$RSCRIPT_BIN" --vanilla "$R_SCRIPT" "$@"
EXIT_CODE=$?

echo
echo "Verifier exit code: $EXIT_CODE  (0 = PASS, 1 = FAIL, 2 = usage/sourcing error)"
exit $EXIT_CODE
