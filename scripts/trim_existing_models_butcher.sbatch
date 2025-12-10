#!/bin/bash
#SBATCH --job-name=trim_models_butcher
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err
#SBATCH --time=03:00:00
#SBATCH --cpus-per-task=3
#SBATCH --mem-per-cpu=42G
#SBATCH --partition=compute

# Exit on any error
set -e

# Print basic job info
echo "============================================"
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Node: $HOSTNAME"
echo "Start time: $(date)"
echo "Working directory: $(pwd)"
echo "============================================"

# Load conda environment
echo "Loading conda and R environment..."
if [ -f "${HOME}/.bashrc" ]; then
    source "${HOME}/.bashrc"
fi

# Initialize micromamba for bash (required on some HPC systems)
if command -v micromamba &> /dev/null; then
    echo "Initializing micromamba..."
    eval "$(micromamba shell hook --shell bash)"
    
    echo "Activating transition_model_env environment..."
    micromamba activate transition_model_env
    MAMBA_EXIT_CODE=$?
    
    if [ $MAMBA_EXIT_CODE -eq 0 ]; then
        echo "✓ Successfully activated transition_model_env"
        echo "Active environment: $CONDA_DEFAULT_ENV"
    else
        echo "✗ Failed to activate transition_model_env (exit code: $MAMBA_EXIT_CODE)"
        echo "Available environments:"
        micromamba env list
        exit 1
    fi
else
    echo "✗ Micromamba not found in PATH"
    echo "PATH: $PATH"
    exit 1
fi

# Verify R and required packages are available
echo ""
echo "Verifying R environment..."
R --version | head -1

echo "Checking for required packages..."
Rscript -e "
if (!require(butcher, quietly = TRUE)) {
    stop('butcher package not found')
} else {
    cat('✓ butcher package loaded successfully\n')
}
if (!require(tidymodels, quietly = TRUE)) {
    stop('tidymodels package not found')  
} else {
    cat('✓ tidymodels package loaded successfully\n')
}
"

if [ $? -ne 0 ]; then
    echo "✗ Required packages not available"
    exit 1
fi

# Get the directory containing the SLURM script and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# If that doesn't work (running from SLURM spool), use the submitted directory
if [[ "$PROJECT_ROOT" == *"/var/spool/slurm"* ]]; then
    PROJECT_ROOT="$SLURM_SUBMIT_DIR"
fi

echo ""
echo "Script directory: $SCRIPT_DIR"
echo "Project root: $PROJECT_ROOT"

# Change to project root
cd "$PROJECT_ROOT"
echo "Current working directory: $(pwd)"

# Verify the R script exists
if [ ! -f "scripts/trim_existing_models_butcher.r" ]; then
    echo "✗ ERROR: scripts/trim_existing_models_butcher.r not found in $(pwd)"
    echo "Contents of scripts directory:"
    ls -la scripts/
    exit 1
else
    echo "✓ Found scripts/trim_existing_models_butcher.r"
fi

# Define model directory (adjust this path as needed)
MODEL_DIR="${PROJECT_ROOT}/models"
BACKUP_DIR="${PROJECT_ROOT}/models_backup"

# Check if model directory exists
if [ ! -d "$MODEL_DIR" ]; then
    echo "✗ ERROR: Model directory not found: $MODEL_DIR"
    echo "Contents of project root:"
    ls -la "$PROJECT_ROOT"
    echo ""
    echo "Please check if models are stored in a different location and update the MODEL_DIR variable"
    exit 1
fi

# Run the model trimming script
echo ""
echo "============================================"
echo "Starting model trimming with butcher package..."
echo "============================================"
echo "Model directory: $MODEL_DIR"
echo "Backup directory: $BACKUP_DIR"
echo ""

Rscript scripts/trim_existing_models_butcher.r "$MODEL_DIR" "$BACKUP_DIR"

TRIMMING_EXIT_CODE=$?

echo ""
echo "============================================"
echo "Job completed"
echo "End time: $(date)"
echo "Exit code: $TRIMMING_EXIT_CODE"
echo "============================================"

exit $TRIMMING_EXIT_CODE