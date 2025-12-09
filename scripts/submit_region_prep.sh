#!/bin/bash
#SBATCH --job-name=region-prep
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=16G
#SBATCH --output=logs/region-prep-%j.out
#SBATCH --error=logs/region-prep-%j.err
#SBATCH --profile=task

# ----------------------------------------------------------
# 1) Load micromamba
# ----------------------------------------------------------
export MAMBA_EXE="/cluster/project/eawag/p01002/.local/bin/micromamba"

if [ ! -f "$MAMBA_EXE" ]; then
    echo "ERROR: micromamba not found at $MAMBA_EXE"
    exit 1
fi

eval "$($MAMBA_EXE shell hook -s bash)"

# ----------------------------------------------------------
# 2) Activate environment
# ----------------------------------------------------------
ENV_PATH="/cluster/scratch/bblack/micromamba/envs/feat_select_env"

if [ ! -d "$ENV_PATH" ]; then
    echo "ERROR: environment not found at $ENV_PATH"
    exit 1
fi

micromamba activate "$ENV_PATH"
echo "✓ Activated env: $ENV_PATH"

# ----------------------------------------------------------
# 3) Set personal R libs & UTF-8 locale
# ----------------------------------------------------------
export R_LIBS_USER="$HOME/R_libs"
mkdir -p "$R_LIBS_USER"

export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
echo "Using UTF-8 locale"

# Set terra temp directory
export TERRA_TEMP="/cluster/scratch/bblack/terra_temp"
mkdir -p "$TERRA_TEMP"
echo "✓ Terra temp directory: $TERRA_TEMP"

# ----------------------------------------------------------
# 4) Confirm Rscript exists
# ----------------------------------------------------------
RSCRIPT_BIN="$ENV_PATH/bin/Rscript"

if [ ! -x "$RSCRIPT_BIN" ]; then
    echo "ERROR: Rscript not found in $ENV_PATH/bin/"
    exit 1
fi

echo "✓ Using Rscript at: $RSCRIPT_BIN"
echo

# ----------------------------------------------------------
# 5) Run region preparation
# ----------------------------------------------------------
R_SCRIPT="$SLURM_SUBMIT_DIR/run_region_prep.r"

if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_region_prep.r not found at: $R_SCRIPT"
    exit 1
fi

echo "✓ Running region preparation: $R_SCRIPT"
"$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
exit $EXIT_CODE