#!/bin/bash
# hpc_common.sh
# Common functions and variables for HPC job scripts.
# Source this file in your submit scripts: source "$(dirname "$0")/hpc_common.sh"
#
# Stage 7 contract (Phase 1 plans 01-01 + 01-02, decisions D-12..D-15, PIPE-04):
#   - All host-specific paths come from `$USER` or one of the contract env
#     vars below. There must be NO active hardcoded user-specific literals
#     (e.g., `/home/<someone>/...`) here.
#   - Required HPC variables (validated by --check-stage7-contract):
#       HPC_SCRATCH_ROOT  (data + env install root on scratch)
#       HPC_TMP_ROOT      (per-job tmp root; backs $TMPDIR)
#       TERRA_TEMP        (terra::terraOptions tempdir; defaults to
#                          $HPC_SCRATCH_ROOT/terra_temp on HPC if unset)
#   - Variables documented (but not validated) here:
#       MAMBA_EXE_CUSTOM, DINAMICA_EGO_8_HOME, DINAMICA_BACKEND
# The same names are documented operator-facing in `.env.template` and consumed
# from R via `src/setup.r:get_stage7_runtime_paths()`.

# ----------------------------------------------------------
# Stage 7 contract validation
# ----------------------------------------------------------
# Required HPC contract variables. Pre-flight (later Phase 1 plan) and
# `--check-stage7-contract` reject jobs that proceed with any of these unset
# rather than silently constructing hidden defaults (D-15, T-01-04).
HPC_STAGE7_REQUIRED_VARS=(
    "HPC_SCRATCH_ROOT"
    "HPC_TMP_ROOT"
    "TERRA_TEMP"
)

# Validate the Stage 7 path contract.
# Returns 0 if all required variables are set and non-empty.
# Returns 1 with a human-readable list naming each missing variable otherwise.
check_stage7_contract() {
    local missing=()
    local var
    for var in "${HPC_STAGE7_REQUIRED_VARS[@]}"; do
        if [ -z "${!var:-}" ]; then
            missing+=("$var")
        fi
    done

    if [ ${#missing[@]} -gt 0 ]; then
        echo "ERROR: Stage 7 path contract incomplete." >&2
        echo "The following required variables are unset or empty:" >&2
        for var in "${missing[@]}"; do
            echo "  - $var" >&2
        done
        echo >&2
        echo "Source the project .env file or export these variables before submitting" >&2
        echo "the Stage 7 batch job. See .env.template for the canonical operator surface." >&2
        return 1
    fi
    return 0
}

# ----------------------------------------------------------
# Find and set micromamba executable
# ----------------------------------------------------------
find_micromamba() {
    local MAMBA_EXE=""
    local POSSIBLE_LOCATIONS=(
        "${MAMBA_EXE_CUSTOM:-}"                      # explicit override wins
        "$HOME/.local/bin/micromamba"                # default install layout
        "/home/$USER/.local/bin/micromamba"  # Euler layout when $HOME differs
    )

    for loc in "${POSSIBLE_LOCATIONS[@]}"; do
        if [ -n "$loc" ] && [ -f "$loc" ]; then
            MAMBA_EXE="$loc"
            echo "Found micromamba at: $MAMBA_EXE" >&2
            break
        fi
    done

    if [ -z "$MAMBA_EXE" ] || [ ! -f "$MAMBA_EXE" ]; then
        echo "ERROR: micromamba not found in any expected location" >&2
        echo "Tried locations:" >&2
        for loc in "${POSSIBLE_LOCATIONS[@]}"; do
            if [ -n "$loc" ]; then
                echo "  - $loc" >&2
            fi
        done
        echo >&2
        echo "Please install micromamba using: bash scripts/install_micromamba.sh" >&2
        echo "Or set MAMBA_EXE_CUSTOM environment variable" >&2
        return 1
    fi

    echo "$MAMBA_EXE"
    return 0
}

# ----------------------------------------------------------
# Load and activate micromamba environment
# ----------------------------------------------------------
activate_env() {
    local ENV_PATH="$1"

    # Find micromamba. Assign then export separately: `export VAR=$(cmd)` makes
    # `$?` reflect the `export` builtin (always 0), masking a find_micromamba
    # failure and letting the script fall through to `micromamba shell hook`
    # with an empty MAMBA_EXE.
    local MAMBA_EXE_LOCAL
    MAMBA_EXE_LOCAL=$(find_micromamba)
    if [ $? -ne 0 ]; then
        exit 1
    fi
    export MAMBA_EXE="$MAMBA_EXE_LOCAL"

    # Initialize micromamba
    eval "$($MAMBA_EXE shell hook -s bash)"

    # Check if environment exists
    if [ ! -d "$ENV_PATH" ]; then
        echo "ERROR: environment not found at $ENV_PATH"
        echo "Please run: bash scripts/setup_environments.sh --env <name> --non-interactive"
        exit 1
    fi

    # Activate environment
    micromamba activate "$ENV_PATH"
    if [ $? -eq 0 ]; then
        echo "Activated environment: $ENV_PATH"
    else
        echo "ERROR: Failed to activate environment: $ENV_PATH"
        exit 1
    fi
}

# ----------------------------------------------------------
# Set common environment variables
# ----------------------------------------------------------
setup_common_env() {
    # Stage 7 contract gate — refuse to set up partial environment with
    # hidden defaults; pre-flight reads the same variables (D-15, T-01-04).
    if ! check_stage7_contract; then
        echo "Refusing to set up Stage 7 environment without complete path contract." >&2
        exit 1
    fi

    # Personal R library — derives from $HOME/$USER, never from a literal user.
    export R_LIBS_USER="${R_LIBS_USER:-$HOME/R_libs}"
    mkdir -p "$R_LIBS_USER"

    # UTF-8 locale
    export LC_ALL=en_US.UTF-8
    export LANG=en_US.UTF-8
    echo "Using UTF-8 locale"

    # Terra temp directory — explicit env var only; no hidden user-specific
    # defaults. The contract guarantees TERRA_TEMP is non-empty by here.
    mkdir -p "$TERRA_TEMP"
    echo "Terra temp directory: $TERRA_TEMP"

    # TMPDIR follows the per-job tmp root from the contract.
    export TMPDIR="${TMPDIR:-$HPC_TMP_ROOT}"
    mkdir -p "$TMPDIR"
    echo "TMPDIR: $TMPDIR"
}

# ----------------------------------------------------------
# Verify Rscript exists in environment
# ----------------------------------------------------------
verify_rscript() {
    local ENV_PATH="$1"
    local RSCRIPT_BIN="$ENV_PATH/bin/Rscript"

    if [ ! -x "$RSCRIPT_BIN" ]; then
        echo "ERROR: Rscript not found in $ENV_PATH/bin/" >&2
        return 1
    fi

    echo "Using Rscript at: $RSCRIPT_BIN" >&2
    echo "$RSCRIPT_BIN"
    return 0
}

# ----------------------------------------------------------
# Lenient Rscript resolution (verifier / pre-flight scripts)
# ----------------------------------------------------------
# resolve_rscript_lenient: prefer the allocation_env interpreter under
# ENV_BASE_PATH, else fall back to a plain Rscript on PATH. For OPERATOR-FACING
# verifier/pre-flight scripts (verify_allocation_run.sh,
# preflight_rate_tables.sh) that must also work on a login node where the env
# is already active or the scratch contract is not configured. Submit scripts
# keep the strict setup_common_env/activate_env/verify_rscript path.
resolve_rscript_lenient() {
    if [ -n "${ENV_BASE_PATH:-}" ] && [ -x "$ENV_BASE_PATH/allocation_env/bin/Rscript" ]; then
        echo "$ENV_BASE_PATH/allocation_env/bin/Rscript"
        return 0
    fi
    if command -v Rscript >/dev/null 2>&1; then
        command -v Rscript
        return 0
    fi
    echo "ERROR: no Rscript found (allocation_env not present and Rscript not on PATH)." >&2
    echo "       Activate the allocation_env or add Rscript to PATH, then re-run." >&2
    return 2
}

# ----------------------------------------------------------
# Trusted get_config() probes
# ----------------------------------------------------------
# probe_region_labels <rscript_bin>: print one region label per line from
# regions.json under config[["reg_dir"]]. Region labels come ONLY from this
# trusted probe, never free operator text (T-036-04). Empty output means
# regions.json is missing/empty (the repo checkout) — it is authoritative only
# on HPC scratch, so callers MUST fail closed on an empty result. Requires
# PROJECT_ROOT to be set (exported to the child explicitly).
probe_region_labels() {
    local rscript_bin="$1"
    PROJECT_ROOT="${PROJECT_ROOT:?probe_region_labels requires PROJECT_ROOT}" \
    "$rscript_bin" --vanilla -e "
  setwd(Sys.getenv('PROJECT_ROOT')); source('src/setup.r'); cfg <- get_config();
  rj <- file.path(cfg[['reg_dir']], 'regions.json');
  if (!file.exists(rj)) quit(status = 0);
  regions <- jsonlite::fromJSON(rj);
  labs <- regions[['label']];
  labs <- labs[!is.na(labs) & nzchar(labs)];
  cat(labs, sep = '\n')
" 2>/dev/null
}

# ----------------------------------------------------------
# Common environment paths
# ----------------------------------------------------------
# Env install location is rooted in the scratch contract var; never a hardcoded
# user literal (PIPE-04, D-14). When sourced from a script that has not yet
# validated the contract, ENV_BASE_PATH may be empty — that is intentional, so
# any caller that uses it before setup_common_env triggers the same fail-fast
# error path.
export ENV_BASE_PATH="${HPC_SCRATCH_ROOT:+$HPC_SCRATCH_ROOT/micromamba/envs}"

# ----------------------------------------------------------
# CLI: noninteractive contract validation
# ----------------------------------------------------------
# Allow this file to be both sourced (the common case) and invoked directly
# with `--check-stage7-contract` for use in pre-flight, CI, and operator
# checks. Detect direct invocation by comparing $0 to ${BASH_SOURCE[0]}.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    case "${1:-}" in
        --check-stage7-contract)
            if check_stage7_contract; then
                echo "Stage 7 path contract OK."
                exit 0
            else
                exit 1
            fi
            ;;
        ""|--help|-h)
            cat <<EOF
Usage: scripts/hpc_common.sh [--check-stage7-contract]

Source this file from a submit script to use its helpers, or invoke it
directly with --check-stage7-contract to validate the Stage 7 path
contract (HPC_SCRATCH_ROOT, HPC_TMP_ROOT, TERRA_TEMP).
EOF
            [ "${1:-}" = "" ] && exit 0 || exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            exit 2
            ;;
    esac
fi
