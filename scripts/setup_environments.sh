#!/bin/bash
# setup_environments.sh
# Script to create conda environments for LULCC modeling pipeline

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENVS_DIR="$PROJECT_ROOT/envs"

echo "========================================="
echo "Setting up LULCC Modeling Environments"
echo "========================================="
echo "Environments directory: $ENVS_DIR"
echo

# Check if micromamba is available
MAMBA_EXE="/cluster/project/eawag/p01002/.local/bin/micromamba"
if [ ! -f "$MAMBA_EXE" ]; then
    echo "ERROR: micromamba not found at $MAMBA_EXE"
    echo "Please adjust the MAMBA_EXE path in this script"
    exit 1
fi

eval "$($MAMBA_EXE shell hook -s bash)"

# Create base environments directory
ENV_BASE_PATH="/cluster/scratch/bblack/micromamba/envs"
mkdir -p "$ENV_BASE_PATH"

# Function to create environment
create_env() {
    local env_file="$1"
    local env_name="$2"
    
    echo "Creating environment: $env_name"
    echo "  From file: $env_file"
    echo "  Target path: $ENV_BASE_PATH/$env_name"
    
    if [ -d "$ENV_BASE_PATH/$env_name" ]; then
        echo "  Environment already exists. Removing first..."
        micromamba env remove -n "$env_name" -y
    fi
    
    micromamba env create -f "$env_file" -p "$ENV_BASE_PATH/$env_name"
    
    if [ $? -eq 0 ]; then
        echo "  ✓ Successfully created $env_name"
    else
        echo "  ✗ Failed to create $env_name"
        exit 1
    fi
    echo
}

# Create feature selection environment
if [ -f "$ENVS_DIR/feat_select_env.yaml" ]; then
    create_env "$ENVS_DIR/feat_select_env.yaml" "feat_select_env"
else
    echo "ERROR: feat_select_env.yaml not found"
    exit 1
fi

# Create transition modeling environment
if [ -f "$ENVS_DIR/transition_model_env.yml" ]; then
    create_env "$ENVS_DIR/transition_model_env.yml" "transition_model_env"
else
    echo "ERROR: transition_model_env.yml not found"
    exit 1
fi

# Create distance calculation environment (if exists)
if [ -f "$ENVS_DIR/dist_calc_env.yml" ]; then
    create_env "$ENVS_DIR/dist_calc_env.yml" "dist_calc_env"
else
    echo "WARNING: dist_calc_env.yml not found, skipping"
fi

# Create climate data environment (if exists)
if [ -f "$ENVS_DIR/clim_data_env.yml" ]; then
    create_env "$ENVS_DIR/clim_data_env.yml" "clim_data_env"
else
    echo "WARNING: clim_data_env.yml not found, skipping"
fi

echo "========================================="
echo "Environment Setup Complete"
echo "========================================="
echo "Available environments:"
micromamba env list
echo

echo "To activate an environment, use:"
echo "  micromamba activate $ENV_BASE_PATH/feat_select_env"
echo "  micromamba activate $ENV_BASE_PATH/transition_model_env"
echo

echo "Done!"