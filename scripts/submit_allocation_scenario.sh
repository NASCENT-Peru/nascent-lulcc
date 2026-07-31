#!/bin/bash
# submit_allocation_scenario.sh
# Phase 3.6 capstone launcher (plan 03.6-02, D-04/D-09).
#
# This is NOT itself an SBATCH job — run it from the project root on a login/submit
# node; it CALLS sbatch. It fans out one per-region fat-node job
# (scripts/submit_allocation_region.sh) per region (D-01/D-02), implements
# timestep-level resume (D-09), and queues a dependent (afterok) national-mosaic
# assembly job (scripts/submit_assemble_mosaic.sh, the Plan 03 target) that runs
# only after every region job succeeds.
#
# Usage (defaults to NAT; scenario-parameterised for the BAU/NAT/CUL/SOC sweep):
#   bash scripts/submit_allocation_scenario.sh
#   ALLOC_SCENARIO=BAU bash scripts/submit_allocation_scenario.sh
#
# Region list, the timestep schedule (simulation_year_steps), and the output dir
# are all resolved at run time from get_config() / regions.json — nothing is
# hardcoded. regions.json is empty in the repo checkout and authoritative only on
# HPC scratch (config[["reg_dir"]]); the launcher fails closed if the probe
# returns zero regions.
#
# Resume (D-09): resume is DRIVER-SIDE. The launcher only skips a region whose
# posteriors are ALL already present (so no node is wasted); any region with at least
# one missing posterior is submitted with NO year filter, and the driver
# (run_allocation_for_scenario) scans that region's posteriors, resumes at the first
# gap, and runs the remaining timesteps through 2060 (overwriting any stale
# downstream posterior). The launcher does NOT set ALLOCATION_YEAR_POST_FILTER for
# resume — that variable is the single-timestep SMOKE filter, and overloading it made
# a resumed region run exactly ONE step per submission (03.6-REVIEW.md CR-01). Plan 01
# D-10 atomic writes guarantee the scan only ever sees fully-written posteriors.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export PROJECT_ROOT
cd "$PROJECT_ROOT" || { echo "ERROR: cannot cd to PROJECT_ROOT: $PROJECT_ROOT"; exit 1; }

source "$SCRIPT_DIR/hpc_common.sh"

ENV_NAME="allocation_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

SCENARIO="${ALLOC_SCENARIO:-NAT}"
REGION_JOB="$SCRIPT_DIR/submit_allocation_region.sh"
MOSAIC_JOB="$SCRIPT_DIR/submit_assemble_mosaic.sh"   # Plan 03 target (co-delivered this phase)

# Per-region SLURM partition + core map (D-02). The region->tier assignment
# lives in config (hpc_config.yaml: allocation_fat_regions /
# allocation_highmem_regions, keyed by region suffix, sized from measured peak
# RSS) and is probed below alongside the schedule — so adding or renaming a
# region in regions.json is routed by editing config, not this script. A region
# in NEITHER list routes to fat with a warning: the fail-safe default (a small
# region on fat merely queues; a big region on highmem OOMs). cpus-per-task
# tracks the node so the sequential-strategy predict uses the whole node:
# fat=160, highmem=80 (only two fat nodes exist; highmem nodes have 80 vCores).
# Force one partition for every region with ALLOC_PARTITION (and ALLOC_CPUS);
# tune per-tier with ALLOC_FAT_PARTITION/ALLOC_HIGHMEM_PARTITION and
# ALLOC_FAT_CPUS/ALLOC_HIGHMEM_CPUS.
FAT_PARTITION="${ALLOC_FAT_PARTITION:-fat}"
HIGHMEM_PARTITION="${ALLOC_HIGHMEM_PARTITION:-highmem}"
FAT_CPUS="${ALLOC_FAT_CPUS:-160}"
HIGHMEM_CPUS="${ALLOC_HIGHMEM_CPUS:-80}"
region_in_list() {  # <needle> <items...>
    local needle="$1" item
    shift
    for item in "$@"; do
        if [ "$item" = "$needle" ]; then return 0; fi
    done
    return 1
}
partition_for_region() {
    if [ -n "${ALLOC_PARTITION:-}" ]; then echo "$ALLOC_PARTITION"; return; fi
    if region_in_list "$1" ${HIGHMEM_REGIONS[@]+"${HIGHMEM_REGIONS[@]}"}; then
        echo "$HIGHMEM_PARTITION"
    else
        echo "$FAT_PARTITION"
    fi
}
cpus_for_region() {
    if [ -n "${ALLOC_CPUS:-}" ]; then echo "$ALLOC_CPUS"; return; fi
    if region_in_list "$1" ${HIGHMEM_REGIONS[@]+"${HIGHMEM_REGIONS[@]}"}; then
        echo "$HIGHMEM_CPUS"
    else
        echo "$FAT_CPUS"
    fi
}

echo "========================================="
echo "Phase 3.6 allocation launcher"
echo "========================================="
echo "Scenario: $SCENARIO"
echo "Project root: $PROJECT_ROOT"
echo "Region job body: $REGION_JOB"
echo "Mosaic assembly job (afterok target): $MOSAIC_JOB"
echo

# WR-04: fail closed if env setup/activation fails (these run under `set +e`, so an
# unchecked failure would otherwise let verify_rscript pick a stale on-PATH Rscript
# and probe regions/schedule from a different environment than the jobs run in).
setup_common_env || { echo "ERROR: setup_common_env failed" >&2; exit 1; }
activate_env "$ENV_PATH" || { echo "ERROR: activate_env failed for $ENV_PATH" >&2; exit 1; }
echo
RSCRIPT_BIN=$(verify_rscript "$ENV_PATH")
if [ $? -ne 0 ]; then
    exit 1
fi
echo

# --- Resolve region labels from regions.json (config[["reg_dir"]]) ----------
# Shared trusted probe (hpc_common.sh): region labels come ONLY from
# regions.json via get_config(), never free operator text — T-036-04 mitigation.
readarray -t REGIONS < <(probe_region_labels "$RSCRIPT_BIN")

# Fail closed (T-036-04): an empty region list means regions.json is missing/empty
# (the repo checkout) — the schedule is authoritative only on HPC scratch.
if [ "${#REGIONS[@]}" -eq 0 ]; then
    echo "ERROR: region probe returned zero regions." >&2
    echo "       regions.json under config[['reg_dir']] is missing or empty." >&2
    echo "       It is authoritative only on HPC scratch — run this launcher there." >&2
    exit 1
fi

# --- Resolve the timestep schedule + output dir + partition routing ---------
readarray -t SCHEDULE_INFO < <("$RSCRIPT_BIN" --vanilla -e "
  setwd(Sys.getenv('PROJECT_ROOT')); source('src/setup.r'); cfg <- get_config();
  ys <- cfg[['simulation_year_steps']];
  cat(cfg[['simulation_output_dir']], '\n');
  cat(paste(ys, collapse = ' '), '\n');
  cat(paste(cfg[['allocation_fat_regions']], collapse = ' '), '\n');
  cat(paste(cfg[['allocation_highmem_regions']], collapse = ' '), '\n')
" 2>/dev/null)
SIM_OUT="${SCHEDULE_INFO[0]:-}"
SIM_OUT="$(echo "$SIM_OUT" | xargs)"   # trim
read -r -a YEAR_STEPS <<< "${SCHEDULE_INFO[1]:-}"
read -r -a FAT_REGIONS <<< "${SCHEDULE_INFO[2]:-}"
read -r -a HIGHMEM_REGIONS <<< "${SCHEDULE_INFO[3]:-}"

if [ -z "$SIM_OUT" ] || [ "${#YEAR_STEPS[@]}" -lt 2 ]; then
    echo "ERROR: could not resolve simulation_output_dir / simulation_year_steps from get_config()." >&2
    exit 1
fi

# Posterior years are the TAIL of the schedule (each step writes posterior_<year_end>).
POSTERIOR_YEARS=("${YEAR_STEPS[@]:1}")

echo "Regions (${#REGIONS[@]}): ${REGIONS[*]}"
echo "Schedule (${#YEAR_STEPS[@]} boundaries): ${YEAR_STEPS[*]}"
echo "Posterior years (${#POSTERIOR_YEARS[@]}): ${POSTERIOR_YEARS[*]}"
echo "Output dir: $SIM_OUT"
echo "Routing (config): fat=[${FAT_REGIONS[*]:-}] highmem=[${HIGHMEM_REGIONS[*]:-}] (unlisted -> fat)"
echo

mkdir -p logs

# --- Fan out: one job per region -------------------------------------------
ID_LIST=""              # colon-joined region job ids for the afterok dependency
declare -a SUBMIT_SUMMARY=()

for REGION in "${REGIONS[@]}"; do
    # region_suffix rule: gsub(" ","_",tolower(label)) — must match src/allocation.r
    # (load_allocation_models / prepare_region_worker_inputs) and run_manifest.r.
    REGION_SUFFIX="$(echo "$REGION" | tr ' ' '_' | tr '[:upper:]' '[:lower:]')"

    # Skip-complete optimisation (file-presence, D-09): if EVERY posterior year
    # already exists for this region there is nothing to do — don't allocate a node
    # just to have the driver exit. Otherwise submit the region job and let the
    # driver self-resume (contiguous resume in run_allocation_for_scenario: it scans
    # this region's posteriors, resumes at the first gap, and runs the tail to 2060).
    # Atomic writes (Plan 01 D-10) mean an existing posterior.tif is always complete.
    # We intentionally do NOT pass ALLOCATION_YEAR_POST_FILTER — that is the
    # single-timestep SMOKE filter, not a resume control (overloading it ran exactly
    # one step per submission; 03.6-REVIEW.md CR-01).
    INCOMPLETE=0
    for YEAR in "${POSTERIOR_YEARS[@]}"; do
        if [ ! -f "$SIM_OUT/$SCENARIO/$YEAR/region_${REGION_SUFFIX}/posterior.tif" ]; then
            INCOMPLETE=1
            break
        fi
    done

    if [ "$INCOMPLETE" -eq 0 ]; then
        # Every posterior year already exists for this region — nothing to do.
        echo "  [$REGION] all ${#POSTERIOR_YEARS[@]} posteriors present — skipping (already complete)."
        SUBMIT_SUMMARY+=("$REGION -> skipped(complete)")
        continue
    fi

    if ! region_in_list "$REGION_SUFFIX" \
        ${FAT_REGIONS[@]+"${FAT_REGIONS[@]}"} \
        ${HIGHMEM_REGIONS[@]+"${HIGHMEM_REGIONS[@]}"}; then
        echo "  WARNING: [$REGION] suffix '$REGION_SUFFIX' is in neither" \
             "allocation_fat_regions nor allocation_highmem_regions (hpc_config.yaml)" \
             "— defaulting to the fat tier. Add it to config to route it explicitly." >&2
    fi
    PARTITION="$(partition_for_region "$REGION_SUFFIX")"
    CPUS="$(cpus_for_region "$REGION_SUFFIX")"

    # Driver self-resumes; do NOT set ALLOCATION_YEAR_POST_FILTER (smoke filter).
    # --cpus-per-task is set per-region here so it tracks the node tier (fat=160,
    # highmem=80) and overrides the region job's #SBATCH default; the sequential
    # predict then uses the whole node.
    EXPORT="ALL,ALLOC_REGION=$REGION,ALLOC_SCENARIO=$SCENARIO"

    JOB_ID=$(sbatch --parsable \
        --partition="$PARTITION" \
        --cpus-per-task="$CPUS" \
        --export="$EXPORT" \
        "$REGION_JOB")
    RC=$?
    if [ $RC -ne 0 ] || [ -z "$JOB_ID" ]; then
        echo "ERROR: failed to submit region job for $REGION (rc=$RC)." >&2
        exit 1
    fi

    echo "  [$REGION] submitted job $JOB_ID on partition $PARTITION (${CPUS} cores; driver self-resumes from first gap)."
    SUBMIT_SUMMARY+=("$REGION -> $JOB_ID (partition=$PARTITION cpus=$CPUS)")
    if [ -z "$ID_LIST" ]; then ID_LIST="$JOB_ID"; else ID_LIST="$ID_LIST:$JOB_ID"; fi
done

# --- Queue the national-mosaic assembly job (D-03) --------------------------
# WR-02: assembly submission is INDEPENDENT of whether region jobs were submitted.
# If every region was already complete (ID_LIST empty) the national mosaics may
# still be missing (e.g. a prior afterok mosaic job failed, was cancelled, or its
# dependency was released on a partial failure) — so submit the assembler WITHOUT a
# dependency to fill them. Otherwise gate it afterok on all region ids. The assembler
# skips any year whose region posteriors are incomplete, so an unconditional submit
# is safe.
MOSAIC_ID=""
if [ -z "$ID_LIST" ]; then
    echo
    echo "All regions already complete — submitting mosaic assembly (no dependency) to fill any missing national mosaics."
    MOSAIC_ID=$(sbatch --parsable \
        --export=ALL,ALLOC_SCENARIO="$SCENARIO" \
        "$MOSAIC_JOB")
    RC=$?
    if [ $RC -ne 0 ] || [ -z "$MOSAIC_ID" ]; then
        echo "ERROR: failed to submit mosaic-assembly job (no dependency, rc=$RC)." >&2
        exit 1
    fi
else
    # afterok on ALL region ids (colon-joined). The mosaic job runs only if every
    # region succeeds; it is the sole writer of the national posterior_<year>.tif.
    MOSAIC_ID=$(sbatch --parsable \
        --dependency=afterok:"$ID_LIST" \
        --export=ALL,ALLOC_SCENARIO="$SCENARIO" \
        "$MOSAIC_JOB")
    RC=$?
    if [ $RC -ne 0 ] || [ -z "$MOSAIC_ID" ]; then
        echo "ERROR: failed to submit mosaic-assembly job (afterok:$ID_LIST, rc=$RC)." >&2
        exit 1
    fi
fi

# --- Structured summary ------------------------------------------------------
echo
echo "========================================="
echo "Submission summary (scenario=$SCENARIO)"
echo "========================================="
for line in "${SUBMIT_SUMMARY[@]}"; do
    echo "  $line"
done
echo "  region-ids: ${ID_LIST:-<none>}"
echo "  mosaic-assembly job: ${MOSAIC_ID:-<none>} (dependency afterok:${ID_LIST:-<none — submitted unconditionally>})"
echo
echo "STATE launcher=allocation_scenario scenario=$SCENARIO regions=${#REGIONS[@]} region_ids=${ID_LIST:-none} mosaic_id=${MOSAIC_ID:-none}"
