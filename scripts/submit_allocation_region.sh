#!/bin/bash
#SBATCH --job-name=alloc-region
#SBATCH --partition=fat
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --cpus-per-task=160
#SBATCH --time=24:00:00
#SBATCH --output=logs/alloc-region-%j.out
#SBATCH --error=logs/alloc-region-%j.err
#SBATCH --profile=task

# Phase 3.6 single-region allocation job body (plan 03.6-02, D-01/D-02).
#
# Runs ONE region's full serial timestep chain (2022->2060) under
# ALLOCATION_REGION_FILTER. This is the per-region unit the launcher
# (scripts/submit_allocation_scenario.sh) submits once per region; regions run
# concurrently (one region per node), timesteps stay serial within a region.
#
# Why fat + 160 cores (D-02): andes peaks ~206-216GB (the 26M-cell from-class
# predict) and OOMs compute(93GB)/highmem(188GB) — it runs clean only on fat(~1.5TB);
# a bare `sbatch` also defaults to the ~93GB compute partition and OOMs. So reserve a
# whole fat node: --partition=fat --exclusive --mem=0 --cpus-per-task=160.
# This is a SINGLE-region job (one region, timesteps serial), so there is NO
# region-level parallelism to spread across furrr workers. ALLOCATION_PARALLEL_STRATEGY
# =sequential (set below) collapses to one worker and hands ALL cores to the ranger
# predict (the dominant cost) — matching the smoke script's fast ~160-thread predict.
# Without sequential, the default multicore plan forks NUM_WORKERS workers for the
# lone region (rest idle) AND predict threads = cores/NUM_WORKERS = 1 (single-threaded
# predict). Override on submit to run elsewhere (e.g. --partition=highmem
# --cpus-per-task=80, optionally with ALLOCATION_PREDICT_BATCH_ROWS to bound peak RAM).
#
# Why this is safe to run concurrently with sibling regions (Plan 01 D-05): a
# single-region run (length(region_inputs)==1) chains on its OWN
# region_<suffix>/posterior.tif and skips the shared national mosaic write, so
# concurrent per-region jobs never clobber posterior_<year>.tif. The national
# mosaic is produced post-hoc by the afterok assembly job (Plan 03).
#
# Inputs (set by the launcher via --export=ALL,...):
#   ALLOC_REGION              region label (-> ALLOCATION_REGION_FILTER). REQUIRED.
#   ALLOC_SCENARIO            scenario (default BAU; -> ALLOCATION_PROFILE_SCENARIO).
#   ALLOCATION_YEAR_POST_FILTER  OPTIONAL single-timestep SMOKE filter (one posterior
#                             year). NOT a resume control — resume is driver-side and
#                             automatic (run_allocation_for_scenario self-resumes from
#                             the first gap). Normally unset for production runs.
# Pass-throughs (same defaults as submit_allocation_smoke.sh):
#   ALLOCATION_PREDICT_BATCH_ROWS  batches the 26M-cell from-class predict (andes
#                             needs it). ALLOCATION_PREDICTOR_LAZY,
#                             ALLOCATION_PREDICT_NUM_THREADS likewise pass through.

set -uo pipefail

if [ -n "${SLURM_SUBMIT_DIR:-}" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR/scripts"
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
source "$SCRIPT_DIR/hpc_common.sh"

# Project root: prefer SLURM_SUBMIT_DIR (sbatch), else the parent of scripts/.
if [ -n "${SLURM_SUBMIT_DIR:-}" ]; then
    PROJECT_ROOT="$SLURM_SUBMIT_DIR"
else
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
fi
export PROJECT_ROOT
cd "$PROJECT_ROOT" || { echo "ERROR: cannot cd to PROJECT_ROOT: $PROJECT_ROOT"; exit 1; }

ENV_NAME="allocation_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

# Resolve the region: ALLOC_REGION is set by the launcher. Fail closed if absent
# (a bare sbatch with no region would silently run ALL regions in one job, which
# defeats the per-region decomposition and would OOM).
ALLOC_REGION="${ALLOC_REGION:-}"
if [ -z "$ALLOC_REGION" ]; then
    echo "ERROR: ALLOC_REGION is unset. This job body runs ONE region's chain and"
    echo "       must be submitted with --export=ALL,ALLOC_REGION=<label> (the"
    echo "       launcher scripts/submit_allocation_scenario.sh does this per region)."
    exit 2
fi
ALLOC_SCENARIO="${ALLOC_SCENARIO:-BAU}"

echo "========================================="
echo "Job: Phase 3.6 Single-Region Allocation"
echo "========================================="
echo "Region: $ALLOC_REGION"
echo "Scenario: $ALLOC_SCENARIO"
echo "Environment: $ENV_NAME"
echo "Path: $ENV_PATH"
echo "Node: $(hostname -s 2>/dev/null || echo '?')  cpus-per-task=${SLURM_CPUS_PER_TASK:-?}"
echo

# WR-04: fail closed if env setup/activation fails (these run under `set +e`, so an
# unchecked failure would otherwise let verify_rscript pick a stale on-PATH Rscript
# and run the job against the wrong interpreter).
setup_common_env || { echo "ERROR: setup_common_env failed" >&2; exit 1; }
activate_env "$ENV_PATH" || { echo "ERROR: activate_env failed for $ENV_PATH" >&2; exit 1; }
echo

RSCRIPT_BIN=$(verify_rscript "$ENV_PATH")
if [ $? -ne 0 ]; then
    exit 1
fi
echo

R_SCRIPT="$PROJECT_ROOT/scripts/run_allocation.r"
if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_allocation.r not found at: $R_SCRIPT"
    exit 1
fi

# Scope the run to this single region (D-01). With ALLOCATION_REGION_FILTER set, the
# driver self-resumes (D-09 contiguous resume in run_allocation_for_scenario). Do NOT
# hardcode ALLOCATION_YEAR_POST_FILTER here: the launcher no longer sets it for
# resume; if an operator exports it for a SMOKE test it passes through unchanged.
export ALLOCATION_REGION_FILTER="$ALLOC_REGION"
export ALLOCATION_PROFILE_SCENARIO="$ALLOC_SCENARIO"
export ALLOCATION_PROFILE=TRUE
# Single region per job => sequential strategy: one furrr worker (no NUM_WORKERS-way
# fork for a lone region) and effective_workers=1 in get_allocation_predict_num_threads,
# so the ranger predict gets ALL cores (SLURM_CPUS_PER_TASK). Without it the default
# multicore plan makes predict threads = cores/NUM_WORKERS = 1 (single-threaded).
export ALLOCATION_PARALLEL_STRATEGY=${ALLOCATION_PARALLEL_STRATEGY:-sequential}
export ALLOCATION_NUM_WORKERS=${SLURM_CPUS_PER_TASK:-160}
# Pass-throughs (left as whatever the launcher/operator exported; same defaults
# as submit_allocation_smoke.sh). ALLOCATION_PREDICT_BATCH_ROWS is needed on
# andes for the 26M-cell from-class predict.
export ALLOCATION_PREDICT_BATCH_ROWS=${ALLOCATION_PREDICT_BATCH_ROWS:-}
export ALLOCATION_PREDICTOR_LAZY=${ALLOCATION_PREDICTOR_LAZY:-}
export ALLOCATION_PREDICT_NUM_THREADS=${ALLOCATION_PREDICT_NUM_THREADS:-}

echo "ALLOCATION_REGION_FILTER=$ALLOCATION_REGION_FILTER"
echo "ALLOCATION_PROFILE_SCENARIO=$ALLOCATION_PROFILE_SCENARIO"
echo "ALLOCATION_PROFILE=$ALLOCATION_PROFILE"
echo "ALLOCATION_PARALLEL_STRATEGY=$ALLOCATION_PARALLEL_STRATEGY"
echo "ALLOCATION_NUM_WORKERS=$ALLOCATION_NUM_WORKERS  (moot under sequential; predict uses all $((${SLURM_CPUS_PER_TASK:-160})) cores)"
echo "ALLOCATION_YEAR_POST_FILTER=${ALLOCATION_YEAR_POST_FILTER:-<unset: driver self-resumes>}"
echo "ALLOCATION_PREDICT_BATCH_ROWS=${ALLOCATION_PREDICT_BATCH_ROWS:-<unset: single-shot>}"
echo "ALLOCATION_PREDICTOR_LAZY=${ALLOCATION_PREDICTOR_LAZY:-<unset: lazy per-from-class ON>}"
echo "ALLOCATION_PREDICT_NUM_THREADS=${ALLOCATION_PREDICT_NUM_THREADS:-<unset: auto cores/workers>}"
echo

"$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
echo "Region $ALLOC_REGION (scenario $ALLOC_SCENARIO) chain complete (exit=$EXIT_CODE)."
exit $EXIT_CODE
