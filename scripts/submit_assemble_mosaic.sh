#!/bin/bash
#SBATCH --job-name=assemble-mosaic
#SBATCH --partition=highmem
#SBATCH --cpus-per-task=4
#SBATCH --time=04:00:00
#SBATCH --output=logs/assemble-mosaic-%j.out
#SBATCH --error=logs/assemble-mosaic-%j.err

# Phase 3.6 national-mosaic assembly job body (plan 03.6-03, D-03/D-04).
#
# Thin SBATCH wrapper around scripts/assemble_national_mosaic.r. It is the
# `afterok` target the launcher (scripts/submit_allocation_scenario.sh) queues
# AFTER all per-region run_allocation jobs (scripts/submit_allocation_region.sh
# -> scripts/run_allocation.r) finish:
#   sbatch --dependency=afterok:<id1>:<id2>:... --export=ALL,ALLOC_SCENARIO=<scenario> \
#          scripts/submit_assemble_mosaic.sh
# so it runs only when every region succeeded. The assembler is the SOLE writer
# of the national posterior_<year>.tif under per-region parallel jobs (D-05).
#
# Modest sizing: terra::merge over a handful of per-region posteriors is far
# lighter than allocation itself (no predictor preload, no Dinamica) — a single
# highmem node with --cpus-per-task=4 / --time=04:00:00 is ample. The operator
# can override --partition on submit if highmem is unavailable.
#
# Inputs (set by the launcher via --export=ALL,...):
#   ALLOC_SCENARIO   scenario to assemble (default NAT), matching the launcher's
#                    --export. Passed positionally to the assembler.

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

# Scenario: ALLOC_SCENARIO is set by the launcher's --export; default BAU.
ALLOC_SCENARIO="${ALLOC_SCENARIO:-NAT}"

echo "========================================="
echo "Job: Phase 3.6 National Mosaic Assembly"
echo "========================================="
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

R_SCRIPT="$PROJECT_ROOT/scripts/assemble_national_mosaic.r"
if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: assemble_national_mosaic.r not found at: $R_SCRIPT"
    exit 1
fi

echo "Assembling national mosaics for scenario: $ALLOC_SCENARIO"
echo

"$RSCRIPT_BIN" --vanilla scripts/assemble_national_mosaic.r "$ALLOC_SCENARIO"
exit $?
