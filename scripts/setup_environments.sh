#!/bin/bash
# setup_environments.sh
# Script to create conda environments for LULCC modelling pipeline

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENVS_DIR="$PROJECT_ROOT/environments"

echo "========================================="
echo "Setting up LULCC modelling Environments"
echo "========================================="
echo "Environments directory: $ENVS_DIR"
echo

# Check if micromamba is available
# Try multiple possible locations
MAMBA_EXE=""
POSSIBLE_LOCATIONS=(
    "$HOME/.local/bin/micromamba"
    "/cluster/home/bblack/.local/bin/micromamba"
    "$MAMBA_EXE_CUSTOM"  # Allow override via environment variable
)

for loc in "${POSSIBLE_LOCATIONS[@]}"; do
    if [ -f "$loc" ]; then
        MAMBA_EXE="$loc"
        echo "Found micromamba at: $MAMBA_EXE"
        break
    fi
done

if [ -z "$MAMBA_EXE" ] || [ ! -f "$MAMBA_EXE" ]; then
    echo "ERROR: micromamba not found in any expected location"
    echo "Tried locations:"
    for loc in "${POSSIBLE_LOCATIONS[@]}"; do
        echo "  - $loc"
    done
    echo
    echo "Please install micromamba using: bash scripts/install_micromamba.sh"
    echo "Or set MAMBA_EXE_CUSTOM environment variable to point to your micromamba"
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

# Create transition modelling environment
if [ -f "$ENVS_DIR/transition_model_env.yml" ]; then
    create_env "$ENVS_DIR/transition_model_env.yml" "transition_model_env"
else
    echo "ERROR: transition_model_env.yml not found"
    exit 1
fi

# Create allocation parameters environment
if [ -f "$ENVS_DIR/allocation_params_env.yml" ]; then
    create_env "$ENVS_DIR/allocation_params_env.yml" "allocation_params_env"
else
    echo "ERROR: allocation_params_env.yml not found"
    exit 1
fi

# Create distance calculation environment
if [ -f "$ENVS_DIR/dist_calc_env.yml" ]; then
    create_env "$ENVS_DIR/dist_calc_env.yml" "dist_calc_env"
else
    echo "ERROR: dist_calc_env.yml not found"
    exit 1
fi

# Create data preparation environment
if [ -f "$ENVS_DIR/data_prep_env.yml" ]; then
    create_env "$ENVS_DIR/data_prep_env.yml" "data_prep_env"
else
    echo "ERROR: data_prep_env.yml not found"
    exit 1
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
echo "  micromamba activate $ENV_BASE_PATH/allocation_params_env"
echo "  micromamba activate $ENV_BASE_PATH/dist_calc_env"
echo "  micromamba activate $ENV_BASE_PATH/data_prep_env"
echo

echo "Done!"