#!/bin/bash
# verify_allocation_run.sh
# Phase 3.6 capstone verifier (plan 03.6-04, D-08).
#
# Thin OPERATOR-facing entry that runs the run manifest (scripts/run_manifest.r)
# and surfaces its PASS / INCOMPLETE verdict. Run it from the project root on a
# login / Rundeck node (it is NOT an SBATCH job — it does not call sbatch).
#
# It does NOT re-implement the region x timestep matrix checks in shell: the R
# manifest owns all the logic (existence, valid-GeoTIFF, posterior-vs-prior change,
# national mosaics, saturation roll-up, plausibility). This wrapper only
#   1. invokes scripts/run_manifest.r with the scenario,
#   2. tees the manifest's `STATE manifest verdict=...` line to the operator,
#   3. prints the path to run_manifest.csv for review,
#   4. echoes a single PASS / INCOMPLETE line mirroring the manifest verdict, and
#   5. exits with the manifest's exit code (0 = PASS, non-zero = INCOMPLETE) so
#      CI / operator gating works.
#
# Usage (BAU now; scenario-parameterised for Phase 4 S2 reuse):
#   bash scripts/verify_allocation_run.sh
#   bash scripts/verify_allocation_run.sh NAT
#   ALLOC_SCENARIO=BAU bash scripts/verify_allocation_run.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export PROJECT_ROOT
cd "$PROJECT_ROOT" || { echo "ERROR: cannot cd to PROJECT_ROOT: $PROJECT_ROOT" >&2; exit 1; }

# Scenario: positional arg wins, else ALLOC_SCENARIO, else BAU.
SCENARIO="${1:-${ALLOC_SCENARIO:-BAU}}"

R_SCRIPT="$PROJECT_ROOT/scripts/run_manifest.r"
if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_manifest.r not found at: $R_SCRIPT" >&2
    exit 2
fi

# --- Resolve an Rscript binary ----------------------------------------------
# Prefer the project's allocation_env (the same way submit_* scripts do via
# hpc_common.sh), but fall back to a plain `Rscript` on PATH when the env is
# already active or the HPC scratch contract is not configured (e.g. an operator
# running this on a login node with the env loaded). Modelled on
# scripts/verify_phase3_smoke.sh, which calls Rscript directly.
RSCRIPT_BIN=""
if [ -f "$SCRIPT_DIR/hpc_common.sh" ]; then
    # shellcheck source=/dev/null
    if source "$SCRIPT_DIR/hpc_common.sh" 2>/dev/null \
        && [ -n "${ENV_BASE_PATH:-}" ] \
        && [ -x "$ENV_BASE_PATH/allocation_env/bin/Rscript" ]; then
        RSCRIPT_BIN="$ENV_BASE_PATH/allocation_env/bin/Rscript"
    fi
fi
if [ -z "$RSCRIPT_BIN" ]; then
    if command -v Rscript >/dev/null 2>&1; then
        RSCRIPT_BIN="$(command -v Rscript)"
    else
        echo "ERROR: no Rscript found (allocation_env not present and Rscript not on PATH)." >&2
        echo "       Activate the allocation_env or add Rscript to PATH, then re-run." >&2
        exit 2
    fi
fi

echo "========================================="
echo "Phase 3.6 allocation run verifier"
echo "========================================="
echo "Scenario:     $SCENARIO"
echo "Project root: $PROJECT_ROOT"
echo "Rscript:      $RSCRIPT_BIN"
echo "Manifest:     scripts/run_manifest.r"
echo

# --- Run the manifest; capture its exit code without tripping set -e ----------
# Tee the manifest output so the operator sees the per-cell AUDIT lines and the
# final STATE verdict line live, while we still capture stdout for the summary.
MANIFEST_LOG="$(mktemp 2>/dev/null || echo "$PROJECT_ROOT/.run_manifest.$$.log")"
set +e
"$RSCRIPT_BIN" --vanilla scripts/run_manifest.r "$SCENARIO" 2>&1 | tee "$MANIFEST_LOG"
MANIFEST_RC=${PIPESTATUS[0]}
set -e

# Surface the single structured verdict line the manifest emitted.
VERDICT_LINE="$(grep -E '^STATE manifest verdict=' "$MANIFEST_LOG" | tail -n1 || true)"

# Resolve the manifest CSV path for operator review (config-derived). IN-04: grep the
# machine-stable `STATE manifest csv=<path>` key the manifest emits, not the prose.
MANIFEST_CSV="$(grep -E '^STATE manifest csv=' "$MANIFEST_LOG" \
    | tail -n1 | sed -E 's/^STATE manifest csv=//' || true)"

rm -f "$MANIFEST_LOG"

echo
echo "========================================="
echo "Verdict (scenario=$SCENARIO)"
echo "========================================="
if [ -n "$VERDICT_LINE" ]; then
    echo "  $VERDICT_LINE"
fi
if [ -n "$MANIFEST_CSV" ]; then
    echo "  manifest CSV: $MANIFEST_CSV"
fi

# Echo a single PASS / INCOMPLETE line that mirrors the manifest verdict and
# propagate the manifest's exit code (0 = PASS, non-zero = INCOMPLETE).
if [ "$MANIFEST_RC" -eq 0 ]; then
    echo "PASS scenario=$SCENARIO manifest=run_manifest.r"
else
    echo "INCOMPLETE scenario=$SCENARIO manifest=run_manifest.r rc=$MANIFEST_RC" >&2
fi

exit "$MANIFEST_RC"
