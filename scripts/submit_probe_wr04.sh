#!/bin/bash
#SBATCH --job-name=wr04-probe
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=32G
#SBATCH --output=logs/wr04-probe-%j.out
#SBATCH --error=logs/wr04-probe-%j.err

# Batch wrapper for scripts/probe_wr04_info_path.r (phase 05.1 UAT item 1).
#
# The probe is read-only — it opens one donor run's per-transition probability
# maps plus the national intervention masks and does a handful of terra::global()
# reductions. That is small next to an allocation run, but it is still real I/O
# and real compute, so it belongs in a batch job and not on login02.
#
# Standard launch, from the repo root on login02:
#
#   source .env
#   sbatch scripts/submit_probe_wr04.sh --donor-region andes --donor-year 2032 \
#     --probe-years 2024,2028,2032,2036
#
# Every argument after the script name is forwarded verbatim to the R script,
# so `--help` and all of its flags work unchanged.
#
# No --partition: the defaults above (32G, 2 cpus, 30 min) fit the ordinary
# `compute` partition comfortably. Unlike the allocation smoke run this job has
# no 80GB predictor preload, so it does NOT need highmem or fat.
#
# `source .env` is still required before submitting: setup_common_env() gates on
# the Stage 7 path contract (HPC_SCRATCH_ROOT, TERRA_TEMP, HPC_TMP_ROOT, ...)
# and refuses to run with partial paths, so an unsourced shell fails in seconds.

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
echo "Job: WR-04 INFO-path probe (read-only)"
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

R_SCRIPT="$PROJECT_ROOT/scripts/probe_wr04_info_path.r"
if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: probe_wr04_info_path.r not found at: $R_SCRIPT"
    exit 1
fi

# Keep every native pool single-threaded: the probe is I/O bound and
# oversubscribing GDAL on a shared node is what the batch policy exists to stop.
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export GDAL_NUM_THREADS=1

"$RSCRIPT_BIN" --vanilla "$R_SCRIPT" "$@"
EXIT_CODE=$?

echo
echo "Probe exit code: $EXIT_CODE  (0 = a firing combination was found, 1 = none, 2 = usage error)"
exit $EXIT_CODE
