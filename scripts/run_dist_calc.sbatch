#!/bin/bash
#SBATCH --job-name=dist-calc
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=48
#SBATCH --mem-per-cpu=2700M
#SBATCH --tmp=50G
#SBATCH --output=logs/dist-calc-%j.out
#SBATCH --error=logs/dist-calc-%j.err
#SBATCH --profile=task

# 1) Modules
module load stack/2024-06
module load gcc/12.2.0 proj/9 gdal/3 geos/3

# 2) Conda env
source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate dist_calc_env

# 3) Personal R libs (optional)
export R_LIBS_USER="$HOME/R_libs"

# 4) Workdir & threads
cd "$SLURM_SUBMIT_DIR"
mkdir -p logs
export OPENBLAS_NUM_THREADS=1     # keep BLAS single-threaded
export MKL_NUM_THREADS=1
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}  # allow OpenMP to use all CPUs

# 5) Useful GDAL/R settings for heavy in-memory ops
export GDAL_CACHEMAX=8192         # MB, fine with 128 GB total
export CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif,.tiff"
export R_GC_MAXIMIZE=TRUE

# 6) Run
Rscript --vanilla dist_calc_hpc.R