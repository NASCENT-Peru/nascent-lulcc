#!/bin/bash
# setup_hpc.sh - Run this on the HPC after uploading the project

PROJECT_DIR="/cluster/home/bblack/nascent-lulcc"
SCRATCH_DIR="/cluster/scratch/bblack/nascent-lulcc"

echo "Setting up nascent-lulcc on HPC..."

# Create scratch directories
mkdir -p "$SCRATCH_DIR/data/raw/lulc"
mkdir -p "$SCRATCH_DIR/data/raw/predictors" 
mkdir -p "$SCRATCH_DIR/data/raw/climate"
mkdir -p "$SCRATCH_DIR/data/processed/datasets"
mkdir -p "$SCRATCH_DIR/data/processed/models"
mkdir -p "$SCRATCH_DIR/results/feature_selection"
mkdir -p "$SCRATCH_DIR/results/models"
mkdir -p "$SCRATCH_DIR/results/evaluation"
mkdir -p "$SCRATCH_DIR/results/projections"
mkdir -p "$SCRATCH_DIR/logs/jobs"
mkdir -p "$SCRATCH_DIR/logs/runs"
mkdir -p "$SCRATCH_DIR/temp/R"

# Create home directories
mkdir -p "/cluster/home/bblack/environments"
mkdir -p "/cluster/home/bblack/lib/R"
mkdir -p "/cluster/home/bblack/tools"

# Copy environment template to active config
cp "$PROJECT_DIR/.env.template" "$PROJECT_DIR/.env"

# Make scripts executable
chmod +x "$PROJECT_DIR/scripts/"*.sh
chmod +x "$PROJECT_DIR/scripts/"*.sbatch

echo "HPC setup complete!"
echo ""
echo "Next steps:"
echo "1. Install micromamba in /cluster/home/bblack/tools/"
echo "2. Create conda environments from environments/ directory"
echo "3. Source the environment: source $PROJECT_DIR/.env"
echo "4. Update job scripts with correct paths"
