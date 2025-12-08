#!/bin/bash
#SBATCH --job-name=chelsa
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=48
#SBATCH --mem-per-cpu=2700M          # 48 * 2700M ≈ 129.6 GB total (Euler policy)
#SBATCH --tmp=300G                   # node-local scratch quota for temp files
#SBATCH --output=logs/pro_clim-%j.out
#SBATCH --error=logs/pro_clim-%j.err
#SBATCH --profile=task

set -euo pipefail

# --- 0) No conflicting modules when using Conda GDAL/PROJ/NetCDF
module purge

# --- 1) Activate your Conda env (must contain r-base, r-terra, gdal, proj, geos, netcdf-c, hdf5)
source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate clim_data_env

# --- 2) Fast node-local scratch; point GDAL/R temps there
export TMPDIR="${TMPDIR:-/scratch/$USER/$SLURM_JOB_ID}"
mkdir -p "$TMPDIR"
export CPL_TMPDIR="$TMPDIR"

# --- 3) Let GDAL use all cores; keep BLAS single-threaded (avoid oversubscription)
export GDAL_NUM_THREADS=ALL_CPUS     # multithreaded warps where supported
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OMP_NUM_THREADS=1

# --- 4) GDAL cache (MB) — safe at your memory request; adjust if needed
export GDAL_CACHEMAX=32768

# --- 5) Optional: multithreaded GeoTIFF compression when writing
export GTIFF_NUM_THREADS=ALL_CPUS     # equivalent to -co NUM_THREADS=ALL_CPUS

# --- 6) Sanity checks (netCDF support + subdatasets visible)
gdalinfo --formats | grep -i netcdf || { echo "GDAL netCDF driver missing"; exit 1; }

# --- 7) Run
cd "$SLURM_SUBMIT_DIR"
mkdir -p logs
#run  download_chelsa_data.py
Python3 download_historic_climate_data.py
Python3 download_future_climate_data.py
Rscript --vanilla process_climate_data.r
Rscript --vanilla calculate_mean_rsds_1981_2010
Rscript --vanilla calculate_et0.r