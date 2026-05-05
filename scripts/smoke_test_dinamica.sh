#!/usr/bin/env bash
# scripts/smoke_test_dinamica.sh
#
# Phase 1 Plan 04 Task 2 — Operator-facing Euler smoke test for the unified
# Dinamica launch contract (INFRA-01, D-09, D-10, D-11).
#
# This script proves the HPC Dinamica wiring before any real Stage 7 run.
# It mirrors the contract that lives inside `src/dinamica_utils.r`:
#
#   - On Euler, $DINAMICA_EGO_8_HOME is the absolute path to the external
#     Dinamica `.sif` image (D-10 — image stays out of the repo).
#   - The container runtime is probed in the order `apptainer` first,
#     `singularity` second.
#   - The live launch is exactly:
#         apptainer  exec "$DINAMICA_EGO_8_HOME" DinamicaConsole "$EGO_MODEL"
#       (or the singularity equivalent if apptainer is unavailable).
#   - On success, a timestamped logfile is written under `logs/` named
#     `dinamica-smoke-<UTC-timestamp>.log`. The live mode requires a non-zero
#     exit if the Dinamica subprocess fails OR if the expected log artifact
#     is missing under the chosen log root.
#
# Usage:
#   # Dry-run (no runtime probe required; safe on operator workstations):
#   scripts/smoke_test_dinamica.sh \
#       --dry-run \
#       --runtime apptainer \
#       --artifact /tmp/dinamica.sif \
#       --ego dinamica/dinamica_model/allocation.ego-decoded
#
#   # Live Euler run (after sourcing .env so DINAMICA_EGO_8_HOME is set):
#   scripts/smoke_test_dinamica.sh \
#       --runtime auto \
#       --artifact "$DINAMICA_EGO_8_HOME" \
#       --ego dinamica/dinamica_model/allocation.ego-decoded \
#       --live \
#       --require-log-under logs
#
# Exit codes:
#   0   success (dry-run plan printed OR live Dinamica completed AND log written)
#   1   usage / argument validation error
#   2   dry-run resolution failed (artifact missing, runtime not on PATH, etc.)
#   3   live Dinamica subprocess returned a non-zero exit code
#   4   live Dinamica subprocess succeeded but no `dinamica-smoke-*.log` was
#       written under the requested log root (D-11 contract violation)

set -euo pipefail

PROG_NAME="$(basename "$0")"

# ----------------------------------------------------------------------------
# Defaults (all overridable via flags below)
# ----------------------------------------------------------------------------
RUNTIME=""              # "auto", "apptainer", "singularity"
ARTIFACT=""             # absolute path to .sif image (== DINAMICA_EGO_8_HOME on HPC)
EGO_MODEL=""            # path to .ego-decoded smoke model
MODE="dry-run"          # "dry-run" or "live"
LOG_ROOT="logs"         # where dinamica-smoke-*.log is required to land
ALLOW_NO_PROBE=1        # dry-run never probes PATH; live always probes

usage() {
    cat <<EOF
Usage: $PROG_NAME [OPTIONS]

Validates the Phase 1 Dinamica-on-Euler launch contract before a real Stage 7
allocation run.

Required:
  --runtime auto|apptainer|singularity
       Container runtime to use. "auto" probes apptainer first, singularity
       second. In dry-run mode, an explicit name is recommended so resolution
       works on hosts that lack both runtimes.
  --artifact PATH
       Absolute path to the external Dinamica `.sif` image. On Euler this MUST
       equal \$DINAMICA_EGO_8_HOME. Treated as external per D-10 (the repo
       does not ship the built image).
  --ego PATH
       Path to the Dinamica model used for the smoke test (relative paths are
       resolved against the current working directory). Phase 1 ships
       dinamica/dinamica_model/allocation.ego-decoded for this purpose.

Mode (mutually exclusive — defaults to --dry-run):
  --dry-run            Resolve and print the launch plan; do not invoke
                       Dinamica. Exits 0 if the plan is well-formed.
  --live               Actually invoke Dinamica through the chosen runtime.
                       Exits 0 only if Dinamica returns 0 AND a timestamped
                       \`dinamica-smoke-*.log\` file lands under the chosen
                       log root.

Optional:
  --require-log-under DIR
       Directory (relative to the current working directory or absolute) where
       the live mode must find the produced \`dinamica-smoke-*.log\`. Defaults
       to \`logs\`.
  -h, --help           Show this help and exit.

Examples:
  Dry-run (workstation, no apptainer/singularity required):
    $PROG_NAME --dry-run --runtime apptainer \\
               --artifact /tmp/dinamica.sif \\
               --ego dinamica/dinamica_model/allocation.ego-decoded

  Live Euler smoke test (requires DINAMICA_EGO_8_HOME pointing to the .sif):
    $PROG_NAME --live --runtime auto \\
               --artifact "\$DINAMICA_EGO_8_HOME" \\
               --ego dinamica/dinamica_model/allocation.ego-decoded \\
               --require-log-under logs
EOF
}

# ----------------------------------------------------------------------------
# Argument parsing
# ----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --runtime)
            RUNTIME="${2:-}"
            shift 2 || { echo "ERROR: --runtime requires a value" >&2; exit 1; }
            ;;
        --artifact)
            ARTIFACT="${2:-}"
            shift 2 || { echo "ERROR: --artifact requires a value" >&2; exit 1; }
            ;;
        --ego)
            EGO_MODEL="${2:-}"
            shift 2 || { echo "ERROR: --ego requires a value" >&2; exit 1; }
            ;;
        --require-log-under)
            LOG_ROOT="${2:-}"
            shift 2 || { echo "ERROR: --require-log-under requires a value" >&2; exit 1; }
            ;;
        --dry-run)
            MODE="dry-run"
            shift
            ;;
        --live)
            MODE="live"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

# ----------------------------------------------------------------------------
# Validate arguments
# ----------------------------------------------------------------------------
if [[ -z "$RUNTIME" ]]; then
    echo "ERROR: --runtime is required (auto|apptainer|singularity)" >&2
    exit 1
fi
case "$RUNTIME" in
    auto|apptainer|singularity) : ;;
    *)
        echo "ERROR: --runtime must be one of: auto, apptainer, singularity (got '$RUNTIME')" >&2
        exit 1
        ;;
esac

if [[ -z "$ARTIFACT" ]]; then
    echo "ERROR: --artifact is required (absolute path to the external Dinamica .sif)" >&2
    exit 1
fi

if [[ -z "$EGO_MODEL" ]]; then
    echo "ERROR: --ego is required (path to the Dinamica model used for the smoke test)" >&2
    exit 1
fi

# ----------------------------------------------------------------------------
# Probe the runtime
# Dry-run: never call Sys.which / command -v ; just trust the operator-supplied
#          name so the smoke test can validate the launch plan on a workstation
#          where apptainer/singularity are not installed.
# Live   : always probe PATH. "auto" tries apptainer first, then singularity.
# ----------------------------------------------------------------------------
RESOLVED_RUNTIME=""
if [[ "$MODE" == "live" ]]; then
    if [[ "$RUNTIME" == "auto" ]]; then
        if command -v apptainer >/dev/null 2>&1; then
            RESOLVED_RUNTIME="apptainer"
        elif command -v singularity >/dev/null 2>&1; then
            RESOLVED_RUNTIME="singularity"
        else
            echo "ERROR: --runtime auto: neither apptainer nor singularity found on PATH" >&2
            echo "       Install apptainer (preferred) or singularity, or pass an explicit --runtime." >&2
            exit 2
        fi
    else
        if ! command -v "$RUNTIME" >/dev/null 2>&1; then
            echo "ERROR: requested runtime '$RUNTIME' not found on PATH" >&2
            exit 2
        fi
        RESOLVED_RUNTIME="$RUNTIME"
    fi
else
    # dry-run: take the operator's word
    if [[ "$RUNTIME" == "auto" ]]; then
        # auto + dry-run: prefer apptainer for the printed plan
        RESOLVED_RUNTIME="apptainer"
    else
        RESOLVED_RUNTIME="$RUNTIME"
    fi
fi

# ----------------------------------------------------------------------------
# Resolve the .sif and ego paths
# Dry-run: tolerate missing paths so workstation runs succeed without staging
#          the artifact.
# Live   : both the .sif image and the .ego-decoded model must exist.
# ----------------------------------------------------------------------------
if [[ "$MODE" == "live" ]]; then
    if [[ ! -f "$ARTIFACT" ]]; then
        echo "ERROR: artifact not found at $ARTIFACT" >&2
        echo "       On Euler, set DINAMICA_EGO_8_HOME to the absolute .sif path and pass it" >&2
        echo "       via --artifact \"\$DINAMICA_EGO_8_HOME\"." >&2
        exit 2
    fi
    if [[ ! -f "$EGO_MODEL" ]]; then
        echo "ERROR: ego model not found at $EGO_MODEL" >&2
        exit 2
    fi
fi

# Build the timestamp + log file path used for both modes.
TIMESTAMP="$(date -u +"%Y-%m-%dT%H-%M-%SZ")"
mkdir -p "$LOG_ROOT" 2>/dev/null || true
LOG_FILE="$LOG_ROOT/dinamica-smoke-${TIMESTAMP}.log"

# ----------------------------------------------------------------------------
# Print the resolved plan (used by both dry-run and live for traceability)
# ----------------------------------------------------------------------------
cat <<PLAN
Dinamica smoke test plan
========================
mode               : $MODE
runtime            : $RESOLVED_RUNTIME
DINAMICA_EGO_8_HOME: ${DINAMICA_EGO_8_HOME:-<unset; using --artifact only>}
artifact (.sif)    : $ARTIFACT
ego model          : $EGO_MODEL
log root           : $LOG_ROOT
log file           : $LOG_FILE
PLAN

# Resolved launch command line — printed verbatim and used by --live below.
# This is the same shape exec_dinamica() builds: <runtime> exec <sif> DinamicaConsole <model>
LAUNCH_CMD=("$RESOLVED_RUNTIME" "exec" "$ARTIFACT" "DinamicaConsole" "$EGO_MODEL")

echo "resolved command   : ${LAUNCH_CMD[*]}"

# ----------------------------------------------------------------------------
# Dry-run path
# ----------------------------------------------------------------------------
if [[ "$MODE" == "dry-run" ]]; then
    cat <<DRY
[dry-run] Would invoke:
  ${LAUNCH_CMD[*]}
[dry-run] Would tee stdout/stderr to:
  $LOG_FILE
[dry-run] No Dinamica subprocess was spawned.
DRY
    exit 0
fi

# ----------------------------------------------------------------------------
# Live path
# Run the resolved command and tee its combined output into the smoke logfile.
# ----------------------------------------------------------------------------
echo "[live] Launching Dinamica via $RESOLVED_RUNTIME ..."
echo "[live] Output is teed to $LOG_FILE"

# Use pipefail-aware bash to preserve the Dinamica subprocess exit code through
# `tee`. Without pipefail, `tee` would mask a non-zero exit. We already set
# `set -o pipefail` at the top of this script, so PIPESTATUS[0] is the truth
# source for the launcher's exit code.
set +e
"${LAUNCH_CMD[@]}" 2>&1 | tee "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}
set -e

if [[ "$EXIT_CODE" -ne 0 ]]; then
    echo "[live] FAIL: Dinamica subprocess exited with status $EXIT_CODE" >&2
    echo "[live] See $LOG_FILE for output." >&2
    exit 3
fi

# Contract: a timestamped dinamica-smoke-*.log MUST exist under $LOG_ROOT.
LATEST_LOG="$(ls -1t "$LOG_ROOT"/dinamica-smoke-*.log 2>/dev/null | head -n 1 || true)"
if [[ -z "$LATEST_LOG" || ! -s "$LATEST_LOG" ]]; then
    echo "[live] FAIL: no non-empty dinamica-smoke-*.log under $LOG_ROOT" >&2
    exit 4
fi

echo "[live] SUCCESS: Dinamica completed and wrote $LATEST_LOG"
echo "[live] DINAMICA_EGO_8_HOME contract verified for runtime '$RESOLVED_RUNTIME'."
exit 0
