#!/bin/bash
#SBATCH --job-name=wr04-probe
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=48G
#SBATCH --output=logs/wr04-probe-%j.out
#SBATCH --error=logs/wr04-probe-%j.err

# Batch wrapper for scripts/probe_wr04_info_path.r (phase 05.1 UAT item 1).
#
# The probe is read-only — it streams existing per-transition probability maps
# against the national intervention masks and reduces each pair to two counts.
# That is small next to an allocation run, but it is still real I/O and real
# compute, so it belongs in a batch job and never on login02.
#
# Standard launch, from the repo root on login02:
#
#   source .env
#   # cheapest region, every scenario and year it has, stop at the first hit:
#   sbatch scripts/submit_probe_wr04.sh --donor-region costa_peruana
#
#   # widen to everything (hours; ordered cheapest region first, early exit):
#   sbatch scripts/submit_probe_wr04.sh
#
#   # bound the cost explicitly:
#   sbatch scripts/submit_probe_wr04.sh --max-donors 12
#
# Every argument after the script name is forwarded verbatim to the R script,
# so `--help` and all of its flags work unchanged.
#
# No --partition: the defaults above (48G, 2 cpus, 6h) fit the ordinary
# `compute` partition (93GB). Unlike the allocation smoke run this job has no
# 80GB predictor preload, so it does NOT need highmem or fat — not even to
# sweep andes or cuenca_del_amazonas, because memory is bounded by chunk size
# rather than by region extent.
#
# Walltime is the real budget here. The sweep orders donors cheapest region
# first and stops at the first firing combination, so a hit in costa_peruana
# costs minutes; a full --census over every donor is what fills 6 hours.
#
# Sizing history: the first version of the probe used terra::ifel() and chained
# boolean raster algebra, which materialises several full-extent logical rasters
# at once; it was OOM-killed at 32G on andes. The probe now crops each mask once
# to a temp file and counts through a fixed-size row window, so peak memory no
# longer scales with region size. 48G is headroom, not a requirement, and the
# walltime is raised because streaming trades memory for I/O.
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
