<!-- generated-by: gsd-doc-writer -->
# nascent-lulcc

A 7-stage R pipeline for spatially-explicit Land Use/Land Cover Change (LULCC) modelling in Peru. The pipeline runs from raw raster data preparation through statistical transition modelling to spatial allocation via the Dinamica EGO 8 engine, simulating land cover transitions from 2022 to 2060 across four policy scenarios (BAU, NAT, CUL, SOC). Designed to run on either a local Windows workstation or the ETH Euler HPC cluster (SLURM).

## Prerequisites

- **R** >= 4.2 (4.3.x or 4.4.x recommended; pinned per-environment in `environments/`)
- **Conda / micromamba** — one isolated environment per pipeline stage (see [Environment Setup](#environment-setup))
- **Dinamica EGO 8** — proprietary spatial allocation engine (`DinamicaConsole` binary); the `DINAMICA_EGO_8_HOME` environment variable must point to its install directory or Singularity image path
- **SLURM** — required for HPC execution; stages can also be run manually for local development
- **GDAL >= 3.0**, **PROJ >= 9**, **GEOS** — geospatial system libraries (loaded via SLURM modules or included in conda environments)

## Directory Structure

```
nascent-lulcc/
├── src/                    # R source functions (flat, no sub-packages)
├── scripts/                # Executable entry points: run_*.r drivers + submit_*.sh SLURM wrappers
├── config/                 # YAML/JSON configuration files (version-controlled, no secrets)
├── environments/           # Conda environment definitions, one per pipeline stage
├── dinamica/               # Dinamica EGO model files (.ego-decoded, text-diffable)
├── docs/                   # Developer documentation
├── logs/                   # Job logs (created at runtime, not committed)
├── tests/                  # testthat test stubs
├── DESCRIPTION             # R package manifest (package: evoland)
└── .env.template           # Template for machine-specific runtime environment variables
```

## HPC Setup

### 1. Upload the Project

Upload the repository to your HPC home directory:

```bash
# Target path on ETH Euler
/home/$USER/nascent-lulcc
```

### 2. Configure Environment Variables

Copy the template and edit machine-specific paths:

```bash
cp .env.template .env
# Edit .env — set DINAMICA_EGO_8_HOME and verify scratch paths
source .env
```

Key variables in `.env`:

| Variable | Description |
|---|---|
| `NASCENT_LULCC_HOME` | Project code directory on HPC |
| `HPC_SCRATCH_ROOT` | Scratch filesystem root for data and outputs |
| `TERRA_TEMP` | terra temp directory (should be on scratch) |
| `DINAMICA_EGO_8_HOME` | Path to Dinamica EGO 8 install or container |
| `DINAMICA_BACKEND` | `auto`, `local`, or `hpc` (default: `auto`) |
| `MAMBA_EXE` | Path to micromamba binary |

### Environment Setup

Install micromamba and create all required conda environments:

```bash
# Install micromamba
bash scripts/install_micromamba_simple.sh

# Create all pipeline environments
bash scripts/setup_environments.sh
```

Environments are created in `/beegfs/$USER/micromamba/envs/` (scratch — recreate if wiped):

| Environment | Used by |
|---|---|
| `data_prep_env` | Data preparation stage |
| `feat_select_env` | Feature selection stage |
| `transition_model_env` | Transition modelling and model finalization |
| `allocation_params_env` | Allocation parameter calibration |
| `trans_rate_estimation_env` | Scenario transition rate preparation (CVXR) |
| `allocation_env` | Allocation and Dinamica simulation stage |
| `dist_calc_env` | Distance/accessibility predictor calculation |
| `clim_data_env` | Climate data download (Python) |

See `docs/MICROMAMBA_SETUP.md` for full setup instructions and troubleshooting.

### Configuration

The pipeline auto-detects the execution environment via SLURM environment variables and hostname patterns (`src/setup.r`). No manual switch is required:

- **Local development**: reads `config/local_config.yaml` (`data_basepath: E:/nascent-lulcc-agg`)
- **HPC (ETH Euler)**: reads `config/hpc_config.yaml` (`data_basepath: /beegfs/$USER/nascent-lulcc`)

Both configs share an identical key structure. All data paths are resolved relative to `data_basepath` at runtime — no hardcoded paths exist in `src/`.

## Pipeline Stages

The pipeline consists of 7 sequential stages with SLURM `afterok` dependency chaining:

| Stage | Submit script | Runtime estimate | Purpose |
|---|---|---|---|
| 1. Data Preparation | `submit_lulc_data_prep.sh`, `submit_ancillary_data_prep.sh`, etc. | 2–4 h | LULC reclassification, predictor assembly, transition identification |
| 2. Feature Selection | `submit_feature_selection.sh` | 6–12 h | Collinearity filtering + GRRF feature selection per transition |
| 3. Transition Modelling | `submit_transition_modelling.sh` | 12–24 h | tidymodels GLM/RF/XGBoost training with cross-validation |
| 4. Allocation Parameter Calibration | `submit_calibrate_allocation_parameters.sh` | 2–4 h | Monte-Carlo calibration of Dinamica patch parameters |
| 5. Scenario Preparation | `submit_simulation_trans_rates_estimation.sh` | 4–8 h | CVXR-optimised transition rate tables per scenario |
| 6. Spatial Interventions | `submit_spatial_interventions_prep.sh` | 2–4 h | Per-SSP spatial probability perturbation layer assembly |
| 7. Dinamica Simulations | `submit_allocation.sh` / `submit_dinamica_simulations.sh` | 12–48 h | Spatially-explicit LULC simulation 2022–2060 |

## Usage

### Run the Complete Pipeline

Submit all stages as a chained SLURM job sequence:

```bash
cd /home/$USER/nascent-lulcc/scripts
bash master_pipeline.sh
```

### Submit Individual Stages

Each stage has a paired `run_*.r` driver script and `submit_*.sh` SLURM wrapper:

```bash
# Submit data preparation
sbatch scripts/submit_lulc_data_prep.sh

# Submit with explicit dependency (wait for job 12345)
sbatch --dependency=afterok:12345 scripts/submit_feature_selection.sh

# Run allocation stage
sbatch scripts/submit_allocation.sh
```

### Local Development

Source functions and call pipeline functions interactively. Config auto-detects the local environment:

```r
source("src/setup.r")
source("src/utils.r")
source("src/lulc_data_prep.r")

config <- get_config()   # auto-selects config/local_config.yaml
lulc_data_prep(config)
```

All major pipeline functions accept `config = get_config()` as a default argument and support a `refresh_cache` flag to skip recomputation of existing outputs.

## Monitoring Jobs

```bash
squeue -u $USER                    # Check job queue
sacct -j JOB_ID                    # Check job details
scancel JOB_ID                     # Cancel a job
```

Log files are written to `logs/` with the pattern `<stage>-<JOBID>.{out,err}`.

## Scenarios

Four scenarios are defined in `config/local_config.yaml` (and mirrored in `hpc_config.yaml`):

| Scenario | SSP | Description |
|---|---|---|
| `BAU` | ssp245 | Business as usual |
| `NAT` | ssp126 | Nature-focused |
| `CUL` | ssp126 | Culture-focused |
| `SOC` | ssp126 | Society-focused |

Scenario differentiation is implemented through SSP-linked LULC demand targets and per-scenario spatial intervention files (`config/SSP*_interventions.yml`).

## Key Configuration Files

| File | Purpose |
|---|---|
| `config/local_config.yaml` | Local development paths and all scenario/simulation settings |
| `config/hpc_config.yaml` | HPC (Euler) paths — mirrors local_config key structure |
| `config/lulc_schema.json` | LULC class definitions: values 101–107, colours, original class mapping |
| `config/model_specs.yaml` | ML hyperparameter grids (GLM, RF, XGBoost) |
| `config/pred_data.yaml` | Predictor layer catalogue |
| `config/SSP*_interventions.yml` | Per-scenario spatial intervention rules |

## Common Setup Issues

**Dinamica EGO not found** — Set `DINAMICA_EGO_8_HOME` in `.env` before sourcing it. On ETH Euler, Dinamica runs via a Singularity container; set `DINAMICA_BACKEND=hpc`.

**Micromamba/environment not found** — Conda environments live in scratch and are wiped periodically. Recreate with `bash scripts/setup_environments.sh`. See `docs/MICROMAMBA_SETUP.md`.

**Source file errors in HPC scripts** — Ensure `NASCENT_LULCC_HOME` points to the project directory and that all `src/` files are present. Scripts source `../src/` relative to `scripts/`.

**Memory/time limit exceeded** — Adjust `#SBATCH --mem-per-cpu` and `#SBATCH --time` in the relevant `submit_*.sh` script. Typical allocations: 16–32 GB/CPU, 4–8 cores, up to 48h wall time.

**Missing scratch directories** — Run `source .env` to trigger directory creation, or create manually: `mkdir -p $HPC_SCRATCH_ROOT/{data,results,logs,terra_temp}`.

## License

See [LICENSE](LICENSE) file. Related data archive available at <!-- VERIFY: https://zenodo.org/records/12698471 --> (CC-BY-4.0).
