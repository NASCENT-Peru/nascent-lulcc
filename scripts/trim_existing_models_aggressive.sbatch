#!/bin/bash
#SBATCH --job-name=trim_models_butcher
#SBATCH --output=logs/trim_models_butcher_%j.out
#SBATCH --error=logs/trim_models_butcher_%j.err
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --partition=cpu

# trim_existing_models_butcher.sbatch
# SLURM batch script to run the butcher model trimming script on HPC

echo "========================================="
echo "BUTCHER Model Trimming Job"
echo "========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Job Name: $SLURM_JOB_NAME"
echo "Node: $SLURM_NODELIST"
echo "Start Time: $(date)"
echo "Working Directory: $(pwd)"
echo ""

# Check if input directory argument is provided
if [ $# -eq 0 ]; then
    echo "ERROR: No input directory provided"
    echo "Usage: sbatch trim_existing_models_aggressive.sbatch <input_dir> [backup_dir]"
    echo ""
    echo "Example:"
    echo "  sbatch trim_existing_models_aggressive.sbatch /path/to/transition_models"
    echo "  sbatch trim_existing_models_aggressive.sbatch /path/to/transition_models /path/to/backup"
    exit 1
fi

INPUT_DIR="$1"
BACKUP_DIR="${2:-${INPUT_DIR}_backup_butcher}"

echo "Input directory: $INPUT_DIR"
echo "Backup directory: $BACKUP_DIR"
echo ""

# Validate input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "ERROR: Input directory does not exist: $INPUT_DIR"
    exit 1
fi

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

# Check R availability
echo "Checking R installation..."
which R
R --version | head -n 1

# Show memory info
echo ""
echo "Memory information:"
free -h

# Run the butcher trimming script
echo ""
echo "Starting BUTCHER model trimming process..."
echo "Using tidymodels butcher package for consistent model cleanup"
echo "========================================="

Rscript scripts/trim_existing_models_butcher.r "$INPUT_DIR" "$BACKUP_DIR"
EXIT_CODE=$?

echo ""
echo "========================================="
echo "Job completed with exit code: $EXIT_CODE"
echo "End Time: $(date)"

# Calculate runtime
if [ -n "$SLURM_JOB_START_TIME" ]; then
    RUNTIME=$(($(date +%s) - SLURM_JOB_START_TIME))
    echo "Runtime: ${RUNTIME} seconds ($((RUNTIME/60)) minutes)"
fi

# Show final memory usage
echo ""
echo "Final memory usage:"
free -h

exit $EXIT_CODE