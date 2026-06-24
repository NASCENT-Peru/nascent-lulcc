#!/bin/bash
#SBATCH --job-name=andes-validation
#SBATCH --partition=highmem
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --cpus-per-task=40
#SBATCH --time=24:00:00
#SBATCH --output=logs/andes-validation-%j.out
#SBATCH --error=logs/andes-validation-%j.err
#SBATCH --profile=task

# Phase 3.5 validation driver (plan 03.5-03).
#
# Runs the three Andes smoke configurations SEQUENTIALLY on ONE highmem node:
#   1. lazy-1thr   : ALLOCATION_PREDICTOR_LAZY=1  ALLOCATION_PREDICT_NUM_THREADS=1
#   2. lazy-threads: ALLOCATION_PREDICTOR_LAZY=1  ALLOCATION_PREDICT_NUM_THREADS=40
#   3. eager-1thr  : ALLOCATION_PREDICTOR_LAZY=0  ALLOCATION_PREDICT_NUM_THREADS=1
#
# Why a driver instead of three bare `sbatch` calls:
#   * Sequential by construction. Each run writes the SAME beegfs paths with
#     overwrite=TRUE (region_<suffix>/anterior.tif, neighbourhood TIFs, and
#     region_<suffix>/probability_map_dir/*). Running them concurrently corrupts
#     those shared files (job 573350/573351). One node + serial = no collision.
#   * One highmem (188 GB) node. The eager control (run 3) reproduces the ~80 GB
#     predictor preload + ~45 GB models and OOMs on a ~93 GB compute node
#     (job 573352). --partition=highmem --exclusive --mem=0 gives the whole node.
#   * Identical hardware. The run-2-vs-run-1 wall-time delta is the threading
#     proof; it is only meaningful on the same node.
#   * Output archiving. Each run overwrites the previous run's probability maps,
#     so we copy them to validation_outputs/<jobid>/<tag>/ after every run for the
#     bit-for-bit / <1e-6 equivalence check.
#
# Submit inside the 15-min Rundeck window, then disconnect (sbatch survives the
# SSH drop; the job keeps running):
#   sbatch scripts/run_andes_validation.sh
#
# Override the region label if regions.json uses a different one (e.g. the label
# the `regions.json $label` one-liner prints):
#   sbatch --export=ALL,VAL_REGION=selva_andina scripts/run_andes_validation.sh
#
# Override the scenario / threads / output base if needed:
#   sbatch --export=ALL,VAL_SCENARIO=BAU,VAL_THREADS=40,VAL_SIM_OUT=/beegfs/black/nascent-lulcc/outputs/simulations \
#     scripts/run_andes_validation.sh

set -uo pipefail   # NOT -e: a single run's failure must not abort the chain

if [ -n "${SLURM_SUBMIT_DIR:-}" ]; then
    cd "$SLURM_SUBMIT_DIR" || { echo "ERROR: cannot cd to SLURM_SUBMIT_DIR=$SLURM_SUBMIT_DIR"; exit 1; }
fi
PROJECT_ROOT="$(pwd)"
mkdir -p logs validation_outputs

REGION="${VAL_REGION:-andes}"
SCEN="${VAL_SCENARIO:-BAU}"
THREADS="${VAL_THREADS:-40}"
# Which configs to run (space-separated). Default = all three. Override to skip
# runs already done, e.g. after fixing the lazy read with eager still valid:
#   sbatch --export=ALL,VAL_RUNS="lazy-1thr lazy-threads" scripts/run_andes_validation.sh
RUNS="${VAL_RUNS:-lazy-1thr lazy-threads eager-1thr}"
# Phase 3.4 batched prediction. The 26M-cell from-class (natural_grasslands,
# class 102) makes a single-shot ranger predict spike peak_rss to ~178GB and
# OOM even the 188GB node. Batching caps that transient. Default 5M rows;
# set VAL_BATCH_ROWS=0 to disable (single-shot, will OOM on this region).
BATCH_ROWS="${VAL_BATCH_ROWS:-5000000}"
if [ -n "$BATCH_ROWS" ] && [ "$BATCH_ROWS" != "0" ]; then
    export ALLOCATION_PREDICT_BATCH_ROWS="$BATCH_ROWS"
fi
TAG="${SLURM_JOB_ID:-$(date +%Y%m%d-%H%M%S)}"
# Scenario output root = config[["simulation_output_dir"]]. Override via VAL_SIM_OUT
# if your config differs from the observed beegfs path.
SIM_OUT="${VAL_SIM_OUT:-/beegfs/black/nascent-lulcc/outputs/simulations}"
REGION_SUFFIX="$(echo "$REGION" | tr ' ' '_' | tr '[:upper:]' '[:lower:]')"

echo "================================================================"
echo "Andes Phase 3.5 validation chain"
echo "  region=$REGION (suffix=$REGION_SUFFIX)  scenario=$SCEN  threads(run2)=$THREADS"
echo "  runs=$RUNS"
echo "  predict_batch_rows=${ALLOCATION_PREDICT_BATCH_ROWS:-<single-shot>}"
echo "  driver job=$TAG  node=$(hostname -s)  cpus-per-task=${SLURM_CPUS_PER_TASK:-?}"
echo "  sim_out=$SIM_OUT"
echo "================================================================"

archive_run () {
    local label="$1"
    local dest="validation_outputs/${TAG}/${label}"
    mkdir -p "$dest"
    local found=0
    while IFS= read -r d; do
        [ -z "$d" ] && continue
        found=1
        echo "  archiving $d -> $dest/"
        cp -r "$d" "$dest/" || echo "  WARN: archive copy failed for $d"
    done < <(find "$SIM_OUT/$SCEN" -type d -name probability_map_dir -path "*region_${REGION_SUFFIX}*" 2>/dev/null)
    [ "$found" -eq 0 ] && echo "  (no probability_map_dir found to archive for $label — run may have failed before map generation)"
}

run_one () {
    local label="$1"; shift
    local logf="logs/andes-${label}-${TAG}.out"
    echo
    echo "================ RUN ${label} START $(date -Is) ================"
    echo "  knobs: $* ; per-run log: $logf"
    env ALLOCATION_REGION_FILTER="$REGION" ALLOCATION_PROFILE_SCENARIO="$SCEN" "$@" \
        bash scripts/submit_allocation_smoke.sh > "$logf" 2>&1
    local rc=$?
    echo "================ RUN ${label} EXIT ${rc} $(date -Is) ================"
    grep -E "PROFILE .*stage=(predictor_preload|predictor_load|predict|setup_inputs)|peak_rss=|resolved.*thread|num\.threads|[0-9]+ cells" "$logf" | tail -40 || true
    archive_run "$label"
    return 0
}

for r in $RUNS; do
    case "$r" in
        lazy-1thr)    run_one lazy-1thr    ALLOCATION_PREDICTOR_LAZY=1 ALLOCATION_PREDICT_NUM_THREADS=1 ;;
        lazy-threads) run_one lazy-threads ALLOCATION_PREDICTOR_LAZY=1 ALLOCATION_PREDICT_NUM_THREADS="$THREADS" ;;
        eager-1thr)   run_one eager-1thr   ALLOCATION_PREDICTOR_LAZY=0 ALLOCATION_PREDICT_NUM_THREADS=1 ;;
        eager-threads) run_one eager-threads ALLOCATION_PREDICTOR_LAZY=0 ALLOCATION_PREDICT_NUM_THREADS="$THREADS" ;;
        *) echo "WARN: unknown run '$r' — skipping (valid: lazy-1thr lazy-threads eager-1thr eager-threads)" ;;
    esac
done

echo
echo "================================================================"
echo "All three runs attempted (failures do not abort the chain)."
echo "  Per-run logs : logs/andes-{lazy-1thr,lazy-threads,eager-1thr}-${TAG}.out"
echo "  Archived maps: validation_outputs/${TAG}/{lazy-1thr,lazy-threads,eager-1thr}/"
echo "================================================================"
