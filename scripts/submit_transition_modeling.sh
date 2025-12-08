#!/bin/bash
#SBATCH --job-name=trans-model
#SBATCH --time=72:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=16G
#SBATCH --output=logs/trans-model-%j.out
#SBATCH --error=logs/trans-model-%j.err
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
ENV_PATH="/cluster/scratch/bblack/micromamba/envs/transition_model_env"

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
# 5) Run transition modeling pipeline
# ----------------------------------------------------------
R_SCRIPT="$SLURM_SUBMIT_DIR/run_transition_modelling.r"

if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_transition_modelling.r not found in job directory"
    exit 1
fi

echo "✓ Running transition modeling pipeline: $R_SCRIPT"
"$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
exit $EXIT_CODE