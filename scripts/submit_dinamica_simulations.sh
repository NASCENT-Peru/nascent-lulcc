#!/bin/bash
#SBATCH --job-name=dinamica-sim
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=8G
#SBATCH --output=logs/dinamica-sim-%j.out
#SBATCH --error=logs/dinamica-sim-%j.err
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

# Set environment variables for simulations
export EVOLAND_SIMULATION_DIR="/cluster/scratch/bblack/simulation"
mkdir -p "$EVOLAND_SIMULATION_DIR"
echo "✓ Simulation directory: $EVOLAND_SIMULATION_DIR"

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
# 5) Run Dinamica simulations
# ----------------------------------------------------------
R_SCRIPT="$SLURM_SUBMIT_DIR/run_dinamica_simulations.r"

if [ ! -f "$R_SCRIPT" ]; then
    echo "ERROR: run_dinamica_simulations.r not found at: $R_SCRIPT"
    exit 1
fi

echo "✓ Running Dinamica simulations: $R_SCRIPT"
"$RSCRIPT_BIN" --vanilla "$R_SCRIPT"
EXIT_CODE=$?

echo
echo "Rscript exit code: $EXIT_CODE"
exit $EXIT_CODE