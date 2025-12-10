#!/bin/bash
#SBATCH --job-name=trim_models
#SBATCH --output=logs/trim_models_%j.out
#SBATCH --error=logs/trim_models_%j.err
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --partition=cpu

# trim_existing_models.sbatch
# SLURM batch script to run the model trimming script on HPC

echo "========================================="
echo "Model Trimming Job"
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
    echo "Usage: sbatch trim_existing_models.sbatch <input_dir> [backup_dir]"
    echo ""
    echo "Example:"
    echo "  sbatch trim_existing_models.sbatch /path/to/transition_models"
    echo "  sbatch trim_existing_models.sbatch /path/to/transition_models /path/to/backup"
    exit 1
fi

INPUT_DIR="$1"
BACKUP_DIR="${2:-${INPUT_DIR}_backup}"

echo "Input directory: $INPUT_DIR"
echo "Backup directory: $BACKUP_DIR"
echo ""

# Validate input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "ERROR: Input directory does not exist: $INPUT_DIR"
    exit 1
fi

# Load R environment
echo "Loading R environment..."
if [ -f "${HOME}/.bashrc" ]; then
    source "${HOME}/.bashrc"
fi

# Try to activate conda environment if available
if command -v conda &> /dev/null; then
    echo "Activating conda environment..."
    conda activate transition_model_env 2>/dev/null || echo "Warning: Could not activate transition_model_env"
fi

# Check R availability
echo "Checking R installation..."
which R
R --version | head -n 1

# Run the trimming script
echo ""
echo "Starting model trimming process..."
echo "========================================="

Rscript scripts/trim_existing_models.r "$INPUT_DIR" "$BACKUP_DIR"
EXIT_CODE=$?

echo ""
echo "========================================="
echo "Job completed with exit code: $EXIT_CODE"
echo "End Time: $(date)"

# Calculate runtime
if [ -n "$SLURM_JOB_START_TIME" ]; then
    RUNTIME=$(($(date +%s) - SLURM_JOB_START_TIME))
    echo "Runtime: ${RUNTIME} seconds"
fi

exit $EXIT_CODE