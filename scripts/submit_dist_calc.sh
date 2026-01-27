#!/bin/bash
#SBATCH --job-name=dist-calc
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=48
#SBATCH --mem-per-cpu=2700M
#SBATCH --tmp=50G
#SBATCH --output=logs/dist-calc-%j.out
#SBATCH --error=logs/dist-calc-%j.err
#SBATCH --profile=task

# ----------------------------------------------------------
# Load common HPC functions
# ----------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/hpc_common.sh"

# ----------------------------------------------------------
# Load required modules
# ----------------------------------------------------------
echo "========================================="
echo "Job: Distance Calculation"
echo "========================================="
echo "Loading required modules..."
module load stack/2024-06
module load gcc/12.2.0 proj/9 gdal/3 geos/3
echo "✓ Modules loaded"
echo

# ----------------------------------------------------------
# Setup environment
# ----------------------------------------------------------
ENV_NAME="dist_calc_env"
ENV_PATH="$ENV_BASE_PATH/$ENV_NAME"

echo "Environment: $ENV_NAME"
echo "Path: $ENV_PATH"
echo

# Setup common environment variables
setup_common_env

# Additional GDAL/R settings for heavy operations
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export GDAL_CACHEMAX=8192
export CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif,.tiff"
export R_GC_MAXIMIZE=TRUE

echo "✓ OpenMP threads: $OMP_NUM_THREADS"
echo "✓ GDAL cache: ${GDAL_CACHEMAX}MB"
echo

# Activate environment
activate_env "$ENV_PATH"
echo

# Verify Rscript
RSCRIPT_BIN=$(verify_rscript "$ENV_PATH")
if [ $? -ne 0 ]; then
    exit 1
fi
echo

# ----------------------------------------------------------
# Run distance calculation script
# ----------------------------------------------------------
R_SCRIPT="$SLURM_SUBMIT_DIR/scripts/run_dist_calc.r"

if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_dist_calc.r not found at: $R_SCRIPT"
    exit 1
fi

echo "▶ Running pipeline: $R_SCRIPT"
"$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
exit $EXIT_CODE
