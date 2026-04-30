# STACK — nascent-lulcc
_Last updated: 2026-04-30_

## Summary
nascent-lulcc is a Land Use/Land Cover Change (LULCC) modelling pipeline for Peru. It is implemented primarily in R (v4.3–4.4), with Python used for climate data downloading, and C++ (via Rcpp) for performance-critical spatial computations. The pipeline is designed to run on an ETH Euler HPC cluster managed by SLURM, with separate conda environments per pipeline stage.

---

## Languages

**Primary:**
- **R** (4.3.x in most environments; 4.4.1 in `trans_rate_estimation_env`) — all core modelling, spatial data processing, configuration, and pipeline orchestration scripts
- **C++** (Rcpp) — compiled spatial neighbor-finding and patch statistics (`src/neighbors.cpp`, `src/patch_stats.cpp`)

**Secondary:**
- **Python** (version unspecified, `python` from conda-forge) — climate data download scripts (`scripts/download_historic_climate_data.py`, `scripts/download_future_climate_data.py`)
- **Bash** — HPC job submission (`scripts/submit_*.sh`, `scripts/master_pipeline.sh`, `scripts/hpc_common.sh`)

---

## Runtime

**Environment:**
- Conda (managed via micromamba on HPC; see `scripts/install_micromamba.sh`, `scripts/install_micromamba_simple.sh`)
- Multiple isolated conda environments, one per pipeline stage (see `environments/` directory)
- Target platform: ETH Euler HPC cluster (SLURM scheduler); local development on Windows (E:/ drive)

**Package Manager:**
- Conda (conda-forge channel exclusively, except `trans_rate_estimation_env` which also uses `defaults`)
- No lockfiles present; environment definitions are the canonical pin source

---

## Frameworks & Key R Libraries

### Spatial Processing
| Package | Version | Purpose |
|---------|---------|---------|
| `r-terra` | unspecified | Core raster/vector operations, reprojection, terrain analysis |
| `r-sf` | unspecified | Vector data (used in `dist_calc_env`) |
| `r-sp` / `r-raster` | unspecified | Legacy spatial ops (present in `dist_calc_env` only) |
| `gdal` | ≥3.0 (3.6+ for clim) | Geospatial library (underlies terra/sf) |
| `proj` | ≥9 (clim env) | Coordinate reference system transformations |
| `geos` | unspecified | Geometry engine |
| `r-ncdf4` | unspecified | NetCDF file I/O |
| `hdf5` / `libgdal-hdf5` | unspecified | HDF5 support for GDAL |

### Machine Learning / Modelling
| Package | Version | Purpose |
|---------|---------|---------|
| `r-tidymodels` | unspecified | ML workflow meta-package |
| `r-parsnip` | ≥1.2.1 | Unified model interface |
| `r-ranger` | unspecified | Random Forest implementation |
| `r-xgboost` | 1.7.x (pinned) | Gradient boosting (pinned for `tidypredict` compatibility) |
| `r-glmnet` | unspecified | Regularized regression (GLM/Lasso/Ridge) |
| `r-workflows` | unspecified | Modelling workflow management |
| `r-recipes` | unspecified | Data preprocessing |
| `r-tune` / `r-dials` | unspecified | Hyperparameter tuning |
| `r-rsample` | unspecified | Cross-validation / resampling |
| `r-yardstick` | unspecified | Model evaluation metrics |
| `r-vip` | unspecified | Variable importance plots |
| `r-butcher` | unspecified | Trim fitted model objects |
| `r-tidypredict` | unspecified | Ultra-minimal model storage as prediction expressions |
| `r-proc` | unspecified | ROC curves |
| `r-rrf` / `r-randomforest` | unspecified | Guided Regularized Random Forest (GRRF) for feature selection |
| `r-cvxr` | unspecified | Convex optimization (transition rate estimation) |

### Data Manipulation
| Package | Purpose |
|---------|---------|
| `r-dplyr`, `r-tidyr`, `r-purrr`, `r-tibble`, `r-readr`, `r-stringr`, `r-tidyselect` | Tidyverse core |
| `r-data.table` | Fast in-memory tabular operations |
| `r-furrr` / `r-future` / `r-future.apply` | Parallel processing |

### I/O & Configuration
| Package | Purpose |
|---------|---------|
| `r-arrow` | Parquet file read/write |
| `r-yaml` | YAML config parsing |
| `r-jsonlite` | JSON schema parsing (`config/lulc_schema.json`) |
| `r-readxl` / `r-openxlsx` | Excel file I/O (tools spreadsheets) |
| `r-fs` | File system utilities |

### Spatial Stats / Misc
| Package | Purpose |
|---------|---------|
| `r-mgcv` | GAMs (distance calculation env) |
| `r-speedglm` | Fast GLM fitting |
| `r-ggplot2` / `r-scales` | Visualisation / plotting |
| `r-lubridate` | Date handling (climate data) |
| `r-processx` | Run external processes (invokes Dinamica EGO from R) |
| `r-base64enc` | Base64 encoding (Dinamica model parameter serialisation) |
| `r-rcpp` | C++ integration (neighbor finding, patch statistics) |
| `r-profvis` / `r-pryr` | Memory/performance profiling |

### Python Libraries
| Package | Purpose |
|---------|---------|
| `chelsa_cmip6` | Download CHELSA-CMIP6 bioclimatic variables |
| `numpy` | Auxiliary numeric operations |

---

## External Simulation Software

- **Dinamica EGO 8** — proprietary land-use allocation simulation engine; invoked from R via `r-processx` (`src/dinamica_utils.r`). Requires `DinamicaConsole` on PATH and `DINAMICA_EGO_8_HOME` environment variable.

---

## Configuration System

- **Dual-environment config**: `config/local_config.yaml` (local, `E:/nascent-lulcc-agg`) and `config/hpc_config.yaml` (HPC, `/cluster/scratch/bblack/nascent-lulcc`)
- Auto-detection via SLURM env vars, `/cluster` mount, and hostname patterns (`src/setup.r`)
- All paths are resolved relative to `data_basepath` from the active config
- Additional config files: `config/model_specs.yaml` (ML hyperparameters), `config/lulc_schema.json` (class aggregation), `config/pred_data.yaml` (predictor catalogue, 931KB), `config/ancillary_data.yaml` (administrative boundary sources)
- Scenario interventions: `config/SSP0_interventions.yml` through `config/SSP5_interventions.yml`

---

## Build / Compilation

- C++ source (`src/neighbors.cpp`, `src/patch_stats.cpp`) is compiled via Rcpp; `src/RcppExports.cpp` and `src/RcppExports.r` are the auto-generated wrappers
- No Makefile or CMakeLists.txt — compilation is handled by `Rcpp::sourceCpp()` or `devtools::load_all()` at runtime

---

## HPC Job Scheduler

- **SLURM** (`sbatch`) on ETH Euler cluster
- Job scripts in `scripts/submit_*.sh` and `.sbatch` files
- Typical resource allocations: 8–48 CPUs, 2.7–8 GB RAM/CPU, up to 48h wall time
- Common SLURM modules loaded: `stack/2024-06`, `gcc/12.2.0`, `proj/9`, `gdal/3`, `geos/3`

---

## Gaps / Unknowns

- Python version is not explicitly pinned in any environment file (`python` from conda-forge without version constraint)
- `chelsa_cmip6` Python package version is not pinned (not declared in any environment YAML; installed separately)
- No CI/CD configuration files found (no `.github/workflows/`, `.gitlab-ci.yml`, etc.)
- No `renv.lock` or equivalent R lockfile; reproducibility relies entirely on conda environment YAMLs
- `config/pred_data.yaml` is too large to read (931KB); its full predictor catalogue is not examined here
- `r-tuneRanger` in `dist_calc_env` has a capitalisation inconsistency (`tuneRanger`) — may cause conda install issues
