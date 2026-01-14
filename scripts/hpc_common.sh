#!/bin/bash
# hpc_common.sh
# Common functions and variables for HPC job scripts
# Source this file in your submit scripts: source "$(dirname "$0")/hpc_common.sh"

# ----------------------------------------------------------
# Find and set micromamba executable
# ----------------------------------------------------------
find_micromamba() {
    local MAMBA_EXE=""
    local POSSIBLE_LOCATIONS=(
        "$HOME/.local/bin/micromamba"
        "/cluster/home/bblack/.local/bin/micromamba"
        "${MAMBA_EXE_CUSTOM}"  # Allow override via environment variable
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
    
    # Find micromamba
    export MAMBA_EXE=$(find_micromamba)
    if [ $? -ne 0 ]; then
        exit 1
    fi
    
    # Initialize micromamba
    eval "$($MAMBA_EXE shell hook -s bash)"
    
    # Check if environment exists
    if [ ! -d "$ENV_PATH" ]; then
        echo "ERROR: environment not found at $ENV_PATH"
        echo "Please run: bash scripts/setup_environments.sh"
        exit 1
    fi
    
    # Activate environment
    micromamba activate "$ENV_PATH"
    if [ $? -eq 0 ]; then
        echo "✓ Activated environment: $ENV_PATH"
    else
        echo "ERROR: Failed to activate environment: $ENV_PATH"
        exit 1
    fi
}

# ----------------------------------------------------------
# Set common environment variables
# ----------------------------------------------------------
setup_common_env() {
    # Personal R library
    export R_LIBS_USER="$HOME/R_libs"
    mkdir -p "$R_LIBS_USER"
    
    # UTF-8 locale
    export LC_ALL=en_US.UTF-8
    export LANG=en_US.UTF-8
    echo "Using UTF-8 locale"
    
    # Terra temp directory (in scratch for large temp files)
    export TERRA_TEMP="/cluster/scratch/bblack/terra_temp"
    mkdir -p "$TERRA_TEMP"
    echo "✓ Terra temp directory: $TERRA_TEMP"
}

# ----------------------------------------------------------
# Verify Rscript exists in environment
# ----------------------------------------------------------
verify_rscript() {
    local ENV_PATH="$1"
    local RSCRIPT_BIN="$ENV_PATH/bin/Rscript"
    
    if [ ! -x "$RSCRIPT_BIN" ]; then
        echo "ERROR: Rscript not found in $ENV_PATH/bin/"
        return 1
    fi
    
    echo "✓ Using Rscript at: $RSCRIPT_BIN"
    echo "$RSCRIPT_BIN"
    return 0
}

# ----------------------------------------------------------
# Common environment paths
# ----------------------------------------------------------
export ENV_BASE_PATH="/cluster/scratch/bblack/micromamba/envs"
