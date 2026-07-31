#!/bin/bash
# submit_allocation_all_scenarios.sh
# Full scenario-sweep launcher (Phase 4 / ROADMAP SC7: per-scenario jobs, S2).
#
# This is NOT itself an SBATCH job — run it from the project root on a login/submit
# node. It fans out one submit_allocation_scenario.sh invocation per scenario, so
# every scenario inherits that launcher's machinery unchanged:
#   - per-region partition/core routing from config (hpc_config.yaml:
#     allocation_fat_regions / allocation_highmem_regions; fat=160, highmem=80),
#   - skip-complete region filtering + driver-side resume (D-09),
#   - the afterok national-mosaic assembly job per scenario.
#
# Node usage: with the current routing each scenario submits 3 fat-node jobs +
# 1 highmem job. Only two fat nodes exist, so a 4-scenario sweep queues 12 fat
# jobs; SLURM runs them as nodes
# free up (scenarios interleave — there is no cross-scenario dependency, and none
# is needed: regions/scenarios are fully independent and each mosaic job is gated
# afterok on its OWN scenario's region jobs only). Re-running this launcher is
# idempotent: completed regions are skipped, partial regions resume at the first
# missing posterior.
#
# Usage:
#   bash scripts/submit_allocation_all_scenarios.sh
#   ALLOC_SCENARIOS="BAU CUL" bash scripts/submit_allocation_all_scenarios.sh
#
# All submit_allocation_scenario.sh tuning knobs pass through per scenario
# (ALLOC_PARTITION, ALLOC_CPUS, ALLOC_FAT_*/ALLOC_HIGHMEM_*,
# ALLOCATION_PREDICT_BATCH_ROWS, ...). ALLOC_SCENARIO itself is ignored here —
# use ALLOC_SCENARIOS (plural) or the single-scenario launcher directly.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT" || { echo "ERROR: cannot cd to PROJECT_ROOT: $PROJECT_ROOT"; exit 1; }

SCENARIO_LAUNCHER="$SCRIPT_DIR/submit_allocation_scenario.sh"
if [ ! -f "$SCENARIO_LAUNCHER" ]; then
    echo "ERROR: per-scenario launcher not found: $SCENARIO_LAUNCHER" >&2
    exit 1
fi

read -r -a SCENARIOS <<< "${ALLOC_SCENARIOS:-BAU NAT CUL SOC}"
if [ "${#SCENARIOS[@]}" -eq 0 ]; then
    echo "ERROR: ALLOC_SCENARIOS is set but empty." >&2
    exit 1
fi

if [ -n "${ALLOC_SCENARIO:-}" ]; then
    echo "WARNING: ALLOC_SCENARIO='$ALLOC_SCENARIO' is ignored by the sweep launcher."
    echo "         Use ALLOC_SCENARIOS=\"...\" here, or submit_allocation_scenario.sh directly."
    unset ALLOC_SCENARIO
fi

echo "========================================="
echo "Full scenario-sweep allocation launcher"
echo "========================================="
echo "Scenarios (${#SCENARIOS[@]}): ${SCENARIOS[*]}"
echo "Per-scenario launcher: $SCENARIO_LAUNCHER"
echo

mkdir -p logs

declare -a SWEEP_SUMMARY=()

for SCENARIO in "${SCENARIOS[@]}"; do
    echo "-----------------------------------------"
    echo ">>> Submitting scenario: $SCENARIO"
    echo "-----------------------------------------"

    # Capture output so the per-scenario STATE line can be lifted into the sweep
    # summary, while still streaming everything to the operator's terminal.
    SCENARIO_LOG="logs/submit-all-scenarios-${SCENARIO}-$(date +%Y%m%d-%H%M%S).log"
    ALLOC_SCENARIO="$SCENARIO" bash "$SCENARIO_LAUNCHER" 2>&1 | tee "$SCENARIO_LOG"
    RC=${PIPESTATUS[0]}

    if [ "$RC" -ne 0 ]; then
        # Fail fast: a per-scenario launcher failure (env activation, region probe,
        # sbatch error) would almost certainly repeat for the remaining scenarios.
        # Already-submitted scenarios keep running; re-run this launcher after
        # fixing the cause — skip-complete + driver resume make that safe.
        echo >&2
        echo "ERROR: scenario $SCENARIO submission failed (rc=$RC). Stopping the sweep." >&2
        echo "       Submitted so far:" >&2
        for line in "${SWEEP_SUMMARY[@]}"; do echo "         $line" >&2; done
        echo "       Fix the cause and re-run; completed/queued work is not resubmitted." >&2
        exit "$RC"
    fi

    STATE_LINE="$(grep '^STATE launcher=allocation_scenario' "$SCENARIO_LOG" | tail -n 1)"
    SWEEP_SUMMARY+=("$SCENARIO -> ${STATE_LINE:-submitted (no STATE line captured)}")
    echo
done

echo "========================================="
echo "Sweep submission summary"
echo "========================================="
for line in "${SWEEP_SUMMARY[@]}"; do
    echo "  $line"
done
echo
echo "STATE launcher=allocation_all_scenarios scenarios=${#SCENARIOS[@]} list=${SCENARIOS[*]}"
