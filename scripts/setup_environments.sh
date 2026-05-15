#!/bin/bash
# setup_environments.sh
# Create / update conda environments for the LULCC modelling pipeline.
#
# Stage 7 contract (Phase 1, PIPE-04, D-12..D-15):
#   - Env install root is derived from the HPC_SCRATCH_ROOT contract variable,
#     never from a hardcoded user-specific literal. On local/Windows where
#     HPC_SCRATCH_ROOT is unset we fall back to <repo>/.envs, but only when
#     the user hasn't asked for an HPC-style setup explicitly.
#   - This script is the canonical bootstrap path for the Stage 7 allocation
#     environment (`allocation_env`). Later Phase 1 R verification steps
#     (`Rscript`-based smoke tests, pre-flight, etc.) MUST be runnable after
#     `bash scripts/setup_environments.sh --env allocation_env --non-interactive`
#     against a fresh checkout, so we expose a single-env, noninteractive
#     entrypoint that reuses the same micromamba probing logic as the
#     submit-time helpers in hpc_common.sh.
#   - Failures yield exactly one actionable message: install micromamba via
#     scripts/install_micromamba.sh, or set MAMBA_EXE_CUSTOM.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENVS_DIR="$PROJECT_ROOT/environments"

# Source helper for find_micromamba() and ENV_BASE_PATH derivation. We do NOT
# call setup_common_env here — env creation does not require a SLURM-style
# Stage 7 contract; only the install root must be derivable.
# shellcheck source=/dev/null
source "$SCRIPT_DIR/hpc_common.sh"

# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------
ENV_FILTER=""
NON_INTERACTIVE="false"
FORCE_HPC="${FORCE_HPC:-false}"

while [ $# -gt 0 ]; do
    case "$1" in
        --env)
            ENV_FILTER="${2:-}"
            shift 2
            ;;
        --non-interactive)
            NON_INTERACTIVE="true"
            shift
            ;;
        --hpc)
            FORCE_HPC="true"
            shift
            ;;
        --help|-h)
            cat <<EOF
Usage: bash scripts/setup_environments.sh [--env NAME] [--non-interactive] [--hpc]

  --env NAME          Provision only the named environment (e.g.
                      allocation_env). When omitted, all environments listed
                      in environments/ are provisioned.
  --non-interactive   Do not prompt for confirmation. If a target env already
                      exists it is removed and recreated.
  --hpc               Force HPC-detection on (overrides automatic signals).
                      Use when running setup on a node that does not have
                      /cluster/scratch and is not under SLURM, but you want
                      HPC-style installation.
EOF
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

echo "========================================="
echo "Setting up LULCC modelling environments"
echo "========================================="
echo "Environments directory: $ENVS_DIR"
[ -n "$ENV_FILTER" ] && echo "Target environment       : $ENV_FILTER (single-env mode)"
echo "Non-interactive          : $NON_INTERACTIVE"
echo

# ---------------------------------------------------------------------------
# Locate micromamba via the shared helper (no hardcoded user paths)
# ---------------------------------------------------------------------------
MAMBA_EXE=$(find_micromamba) || exit 1
eval "$($MAMBA_EXE shell hook -s bash)"

# ---------------------------------------------------------------------------
# Resolve env install root (Phase 1.1 — D-112 / PIPE-04)
# ---------------------------------------------------------------------------
# Three-signal HPC detection mirrors scripts/hpc_common.sh:check_stage7_contract():
#   1. SLURM_JOB_ID or SLURM_CLUSTER_NAME set
#   2. /cluster/scratch directory exists
#   3. --hpc flag or FORCE_HPC=true environment variable
# On HPC detection without HPC_SCRATCH_ROOT, REFUSE to fall back to
# $PROJECT_ROOT/.envs (which fills $HOME quota); exit non-zero with a clear,
# named-signal message.
on_hpc=false
hpc_signal=""
if [ -n "${SLURM_JOB_ID:-}" ] || [ -n "${SLURM_CLUSTER_NAME:-}" ]; then
    on_hpc=true; hpc_signal="SLURM env var"
elif [ -d /cluster/scratch ]; then
    on_hpc=true; hpc_signal="/cluster/scratch present"
elif [ "${FORCE_HPC:-false}" = "true" ]; then
    on_hpc=true; hpc_signal="--hpc flag"
fi

if [ "$on_hpc" = "true" ]; then
    if [ -z "${HPC_SCRATCH_ROOT:-}" ]; then
        echo "ERROR: HPC context detected ($hpc_signal) but HPC_SCRATCH_ROOT is unset." >&2
        echo "       Refusing to install conda envs under \$HOME (home filesystem quota)." >&2
        echo "       Source the project .env or run: export HPC_SCRATCH_ROOT=/cluster/scratch/\$USER/nascent-lulcc" >&2
        exit 1
    fi
    ENV_BASE_PATH="$HPC_SCRATCH_ROOT/micromamba/envs"
    echo "Env install root (HPC): $ENV_BASE_PATH"
else
    ENV_BASE_PATH="$PROJECT_ROOT/.envs"
    echo "Env install root (local fallback, no HPC signals): $ENV_BASE_PATH"
fi
mkdir -p "$ENV_BASE_PATH"
echo

# ---------------------------------------------------------------------------
# Single-env provisioning function
# ---------------------------------------------------------------------------
create_env() {
    local env_file="$1"
    local env_name="$2"
    local env_path="$ENV_BASE_PATH/$env_name"

    echo "Creating environment: $env_name"
    echo "  From file   : $env_file"
    echo "  Target path : $env_path"

    if [ -d "$env_path" ]; then
        if [ "$NON_INTERACTIVE" = "true" ]; then
            echo "  Environment already exists. Removing first (non-interactive)..."
            micromamba env remove -p "$env_path" -y || true
        else
            echo "  Environment already exists. Removing first..."
            micromamba env remove -p "$env_path" -y || true
        fi
    fi

    if [ "$NON_INTERACTIVE" = "true" ]; then
        micromamba create -y -f "$env_file" -p "$env_path"
    else
        micromamba create -f "$env_file" -p "$env_path"
    fi

    if [ $? -eq 0 ]; then
        echo "  Successfully created $env_name"
    else
        echo "  Failed to create $env_name" >&2
        exit 1
    fi
    echo
}

# ---------------------------------------------------------------------------
# Default env list (in dependency / usage order)
# ---------------------------------------------------------------------------
declare -a DEFAULT_ENVS=(
    "feat_select_env|$ENVS_DIR/feat_select_env.yaml"
    "transition_model_env|$ENVS_DIR/transition_model_env.yml"
    "allocation_params_env|$ENVS_DIR/allocation_params_env.yml"
    "dist_calc_env|$ENVS_DIR/dist_calc_env.yml"
    "data_prep_env|$ENVS_DIR/data_prep_env.yml"
    "allocation_env|$ENVS_DIR/allocation_env.yml"
    "clim_data_env|$ENVS_DIR/clim_data_env.yml"
)

# ---------------------------------------------------------------------------
# Provision selected envs
# ---------------------------------------------------------------------------
provision_one() {
    local entry="$1"
    local env_name="${entry%%|*}"
    local env_file="${entry##*|}"
    if [ -f "$env_file" ]; then
        create_env "$env_file" "$env_name"
    else
        echo "WARNING: $env_file not found, skipping $env_name"
    fi
}

if [ -n "$ENV_FILTER" ]; then
    matched="false"
    for entry in "${DEFAULT_ENVS[@]}"; do
        env_name="${entry%%|*}"
        if [ "$env_name" = "$ENV_FILTER" ]; then
            provision_one "$entry"
            matched="true"
            break
        fi
    done
    if [ "$matched" != "true" ]; then
        echo "ERROR: --env $ENV_FILTER does not match any known environment" >&2
        exit 1
    fi
else
    for entry in "${DEFAULT_ENVS[@]}"; do
        provision_one "$entry"
    done
fi

echo "========================================="
echo "Environment Setup Complete"
echo "========================================="
echo "Available environments under $ENV_BASE_PATH:"
ls -1 "$ENV_BASE_PATH" 2>/dev/null || true
echo
echo "Activate with: micromamba activate $ENV_BASE_PATH/<env_name>"
echo
echo "Done!"
