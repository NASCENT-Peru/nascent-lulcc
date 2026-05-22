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
#   5   live Dinamica subprocess returned 0 BUT printed an error string under
#       grep -E 'Dinamica EGO exited with an error|terminate called after throwing|std::exception'
#       (D-107 — Dinamica exits 0 even on std::exception; do not trust the
#       subprocess exit code as the success signal).

# ----------------------------------------------------------------------------
# Dinamica launch contract (Phase 1.1 — D-104) — keep in sync with
#   src/dinamica_utils.r:resolve_dinamica_launch()
# See .planning/phases/01.1-fix-dinamica-launch-contract/01.1-PATTERNS.md
# ----------------------------------------------------------------------------

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
# D-105 — In --live mode, fail fast if HPC_SCRATCH_ROOT is unset so the
# operator gets the named-signal message BEFORE the runtime probe (which
# would otherwise mask this with a generic "runtime not found" exit 2).
# ----------------------------------------------------------------------------
if [[ "$MODE" == "live" && -z "${HPC_SCRATCH_ROOT:-}" ]]; then
    echo "ERROR: --live requires HPC_SCRATCH_ROOT (D-105). Source the project .env first." >&2
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

# ----------------------------------------------------------------------------
# D-105 — Resolve staged-home and staged-tmp paths under HPC_SCRATCH_ROOT.
# --live mode already exited above if HPC_SCRATCH_ROOT was unset; here we
# tolerate a missing HPC_SCRATCH_ROOT in --dry-run by using a tempdir so
# workstation operators can still print the launch plan.
# ----------------------------------------------------------------------------
if [[ -n "${HPC_SCRATCH_ROOT:-}" ]]; then
    STAGED_HOME="$HPC_SCRATCH_ROOT/dinamica-home"
    STAGED_TMP="$HPC_SCRATCH_ROOT/dinamica-tmp"
else
    # Dry-run only — --live with unset HPC_SCRATCH_ROOT already exited 1 above.
    STAGED_HOME="${TMPDIR:-/tmp}/nascent-dinamica-home-dryrun"
    STAGED_TMP="${TMPDIR:-/tmp}/nascent-dinamica-tmp-dryrun"
fi

# In --live mode, create staged-home and staged-tmp; seed conf if missing.
if [[ "$MODE" == "live" ]]; then
    mkdir -p "$STAGED_HOME" "$STAGED_TMP"
    if [[ ! -f "$STAGED_HOME/.dinamica_ego_8.conf" ]]; then
        cat > "$STAGED_HOME/.dinamica_ego_8.conf" <<'CONF'
AlternativePathForR = "/usr/local/bin/Rscript"
ClConfig = "0"
MemoryAllocationPolicy = "1"
RCranMirror = "https://cloud.r-project.org/"
CONF
    fi
    # Dinamica requires the system conf to exist even if empty (Plan 07 finding).
    touch "$STAGED_HOME/.dinamica_ego_8_system.conf"
fi

# D-106 — absolute model path
ABS_EGO_MODEL="$(readlink -f "$EGO_MODEL" 2>/dev/null || python3 -c "import os,sys; print(os.path.abspath(sys.argv[1]))" "$EGO_MODEL" 2>/dev/null || true)"
if [[ -z "$ABS_EGO_MODEL" ]]; then
    # Final fallback: leave as-is and warn.
    echo "WARN: could not resolve absolute path for $EGO_MODEL; passing literal." >&2
    ABS_EGO_MODEL="$EGO_MODEL"
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
staged home        : $STAGED_HOME
staged tmp         : $STAGED_TMP
abs ego model      : $ABS_EGO_MODEL
PLAN

# Resolved launch command line — printed verbatim and used by --live below.
# Dinamica launch contract (Phase 1.1 — D-104) — keep in sync with
#   src/dinamica_utils.r:resolve_dinamica_launch()
LAUNCH_CMD=(
    "$RESOLVED_RUNTIME" "exec"
    "--home" "$STAGED_HOME"
    "--bind" "${HPC_SCRATCH_ROOT:-$STAGED_TMP}:${HPC_SCRATCH_ROOT:-$STAGED_TMP}"
    "--env" "DINAMICA_EGO_8_TEMP_DIR=$STAGED_TMP"
    "$ARTIFACT"
    "bash" "-c"
    "cd /opt/dinamica/usr && bin/DinamicaEGO.sh $(printf '%q' "$ABS_EGO_MODEL")"
)

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

# D-107 — Dinamica exits 0 even on std::exception. Grep the log for error
# strings; non-zero exit if any match. Keep in sync with
#   src/dinamica_utils.r:DINAMICA_ERROR_PATTERNS
if grep -qE 'Dinamica EGO exited with an error|terminate called after throwing|std::exception' "$LOG_FILE"; then
    echo "[live] FAIL: Dinamica printed an error string despite exit 0" >&2
    echo "[live] Matching lines:" >&2
    grep -nE 'Dinamica EGO exited with an error|terminate called after throwing|std::exception' "$LOG_FILE" >&2
    exit 5
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
