<!-- generated-by: gsd-doc-writer -->
# Development

This document covers local setup, the script inventory, code conventions, branching, and how to contribute changes to the nascent-lulcc pipeline.

---

## Local Setup

### Prerequisites

Before working on the project locally, ensure you have:

- **R >= 4.2** (4.3.x or 4.4.x recommended; declared in `DESCRIPTION` `Depends` field)
- **Conda or micromamba** — per-stage isolated environments; install via `scripts/install_micromamba_simple.sh`
- **Dinamica EGO 8** — proprietary spatial allocation engine; `DINAMICA_EGO_8_HOME` must point to its installation directory
- **GDAL >= 3.0**, **PROJ >= 9**, **GEOS** — geospatial system libraries (included in conda environments)
- Git

### Clone and configure

```bash
git clone <repository-url>
cd nascent-lulcc

# Copy the environment template and edit machine-specific paths
cp .env.template .env
# Edit .env: set DINAMICA_EGO_8_HOME, adjust local paths as needed
source .env
```

On a local Windows workstation `HPC_SCRATCH_ROOT`, `HPC_TMP_ROOT`, and `TERRA_TEMP` may be left unset — they are only required on HPC. `DINAMICA_EGO_8_HOME` is required on both environments.

### Install conda environments

```bash
# Install micromamba if not already present
bash scripts/install_micromamba_simple.sh

# Create all pipeline environments (uses environments/*.yml definitions)
bash scripts/setup_environments.sh

# Or create a single environment for the stage you are working on
bash scripts/setup_environments.sh --env transition_model_env --non-interactive
```

On local machines the environments are installed to `<repo>/.envs/` by default. On HPC they go to `$HPC_SCRATCH_ROOT/micromamba/envs/` (scratch — recreate after a wipe with the same command).

### Configuration auto-detection

`src/setup.r:get_config()` auto-detects whether it is running locally or on HPC by checking SLURM job variables, PBS job variables, presence of `/cluster`, and Euler hostname patterns. It then loads the appropriate config file:

- **Local**: `config/local_config.yaml` (`data_basepath: E:/nascent-lulcc-agg`)
- **HPC**: `config/hpc_config.yaml` (`data_basepath: ${HPC_SCRATCH_ROOT}`)

Override detection explicitly when needed:

```r
config <- get_config(force_environment = "local")
config <- get_config(force_environment = "hpc")
```

See `docs/CONFIGURATION.md` for the full environment variable and config file reference.

---

## Run Commands

The project has no `package.json` build system. All entry points are R scripts under `scripts/` and SLURM batch wrappers. The table below covers the commands relevant to local development.

### Interactive / local development

| Command | Description |
|---|---|
| `Rscript scripts/run_lulc_data_prep.r` | Run Stage 1 LULC data preparation locally |
| `Rscript scripts/run_feature_selection.r` | Run Stage 2 feature selection locally |
| `Rscript scripts/run_transition_modelling.r` | Run Stage 3 transition modelling locally |
| `Rscript scripts/run_calibrate_allocation_parameters.r` | Run Stage 4 allocation parameter calibration |
| `Rscript scripts/run_simulation_trans_rates_prep.r` | Run Stage 5 scenario transition rate preparation |
| `Rscript scripts/run_spatial_interventions_prep.r` | Run Stage 6 spatial interventions preparation |
| `Rscript scripts/run_allocation.r` | Run Stage 7 allocation simulations |
| `Rscript -e 'testthat::test_dir("tests/testthat")'` | Run testthat unit test suite |
| `bash tests/shell/test-setup-environments-hpc-refusal.sh` | Run shell-level integration tests |

All `run_*.r` scripts source their own dependencies from `src/`, call `get_config()`, and exit with a non-zero code on failure.

### Sourcing functions interactively

```r
source("src/setup.r")
source("src/utils.r")
source("src/lulc_data_prep.r")

config <- get_config()          # auto-selects local or HPC config
lulc_data_prep(config)          # most stage functions accept a config argument
```

All major pipeline functions accept `config = get_config()` as their primary argument and support a `refresh_cache` flag to skip recomputation of already-written outputs.

### HPC submission

```bash
# Submit all stages as a chained SLURM sequence
bash scripts/master_pipeline.sh

# Submit a single stage
sbatch scripts/submit_lulc_data_prep.sh

# Submit with explicit dependency (wait for job 12345 before starting)
sbatch --dependency=afterok:12345 scripts/submit_feature_selection.sh
```

---

## Script Inventory

Every pipeline stage has a paired run script and SLURM submit script. The table below lists all pairs with their SLURM resource allocations.

| Stage | Run script | Submit script | Conda env | CPUs | Memory | Wall time |
|---|---|---|---|---|---|---|
| 1a. Reference grid prep | `run_ref_grid_prep.r` | `submit_ref_grid_prep.sh` | `data_prep_env` | 4 | 16 GB/CPU | 6 h |
| 1b. Region prep | `run_region_prep.r` | `submit_region_prep.sh` | `data_prep_env` | 4 | 16 GB/CPU | 6 h |
| 1c. LULC data prep | `run_lulc_data_prep.r` | `submit_lulc_data_prep.sh` | `data_prep_env` | 4 | 16 GB/CPU | 6 h |
| 1d. Ancillary data prep | `run_ancillary_data_prep.r` | `submit_ancillary_data_prep.sh` | `data_prep_env` | 4 | 16 GB/CPU | — |
| 1e. Calibration predictor prep | `run_calibration_predictor_prep.r` | `submit_calibration_predictor_prep.sh` | `data_prep_env` | 4 | 16 GB/CPU | — |
| 1f. Predictor parquets | `run_predictor_parquets.r` | `submit_predictor_parquets.sh` | `data_prep_env` | 4 | 16 GB/CPU | — |
| 1g. Transition identification | `run_transition_identification.r` | `submit_transition_identification.sh` | `data_prep_env` | 4 | 16 GB/CPU | — |
| 1h. Transition dataset prep | `run_transition_dataset_prep.r` | `submit_transition_dataset_prep.sh` | `data_prep_env` | 4 | 16 GB/CPU | — |
| 2. Feature selection | `run_feature_selection.r` | `submit_feature_selection.sh` | `feat_select_env` | 4 | 32 GB/CPU | 72 h |
| 3. Transition modelling | `run_transition_modelling.r` | `submit_transition_modelling.sh` | `transition_model_env` | 3 | 42 GB/CPU | 72 h |
| 4. Allocation param calibration | `run_calibrate_allocation_parameters.r` | `submit_calibrate_allocation_parameters.sh` | `allocation_params_env` | 4 | 28 GB/CPU | 6 h |
| 5. Transition rate prep | `run_simulation_trans_rates_prep.r` | `submit_simulation_trans_rates_estimation.sh` | `trans_rate_estimation_env` | 6 | 16 GB/CPU | 4 h |
| 6. Spatial interventions prep | `run_spatial_interventions_prep.r` | `submit_spatial_interventions_prep.sh` | `transition_model_env` | 4 | 16 GB/CPU | 4 h |
| 7. Allocation simulations | `run_allocation.r` | `submit_allocation.sh` | `allocation_env` | 8 | 8 GB/CPU | 48 h |
| 7. Dinamica simulations | `run_dinamica_simulations.r` | `submit_dinamica_simulations.sh` | `allocation_env` | 8 | 8 GB/CPU | 48 h |

Additional helper scripts in `scripts/`:

| Script | Purpose |
|---|---|
| `master_pipeline.sh` | Submits all stages as a chained SLURM job sequence with `afterok` dependencies |
| `hpc_common.sh` | Shared HPC functions: `find_micromamba()`, `setup_common_env()`, `activate_env()`, `check_stage7_contract()` |
| `setup_environments.sh` | Creates or updates all conda environments from `environments/*.yml` |
| `install_micromamba_simple.sh` | Bootstraps the micromamba binary into `~/.local/bin/micromamba` |
| `submit_allocation_profile.sh` | Allocation with `ALLOCATION_PROFILE=TRUE` for performance profiling |
| `submit_allocation_smoke.sh` | Smoke-test allocation run (single region, short time window) |
| `smoke_test_dinamica.sh` | Verifies Dinamica EGO binary/container is reachable |
| `summarise_allocation_profile.r` | Parses profiling log output after a profiled allocation run |

---

## Code Style

The project does not use a formal R linter or formatter, but the following conventions are observed throughout `src/`:

- **File naming**: lowercase with underscores, one topic per file (e.g., `transition_modelling.r`, `dinamica_utils.r`)
- **Function naming**: `snake_case` (e.g., `get_config()`, `ensure_dir()`, `exec_dinamica()`)
- **No global side-effects**: `src/` files define functions only; side effects happen in `scripts/run_*.r`
- **Logging**: use `log_msg()` from `src/utils.r` for timestamped console output — do not use bare `cat()` or `print()` in pipeline functions
- **Config access**: always call `get_config()` — never access YAML keys directly or call `Sys.getenv()` for the Stage 7 contract variables; use `get_stage7_runtime_paths()` instead
- **Directory creation**: use `ensure_dir()` from `src/utils.r`; do not call `dir.create()` directly in pipeline functions
- **Cache pattern**: pipeline functions check whether output files already exist and skip computation when `refresh_cache = FALSE` (the default)

### Dependency isolation

Each pipeline stage runs in its own conda environment (defined in `environments/`). Do not add cross-stage dependencies to a single environment. When adding a new R package:

1. Add it to the relevant `environments/<stage_env>.yml` file
2. Rebuild the environment: `bash scripts/setup_environments.sh --env <stage_env> --non-interactive`
3. Verify the stage script still runs end-to-end

---

## Branch Conventions

No formal branch naming policy is documented. The following practice is used in this repository:

- `main` — stable, integration-tested code
- Feature branches follow descriptive naming (e.g., `allocation-step`, `fix/trans-rates`)

No branch protection rules or CI pipeline are configured (no `.github/` directory exists in the repository).

---

## PR Process

There is no pull request template in this repository. When contributing changes:

- Keep commits focused on a single stage or concern
- Verify the affected stage script runs end-to-end without error before merging
- Run the testthat unit tests: `Rscript -e 'testthat::test_dir("tests/testthat")'`
- Run the shell smoke tests if modifying `scripts/setup_environments.sh` or `scripts/hpc_common.sh`: `bash tests/shell/test-setup-environments-hpc-refusal.sh`
- Do not commit `.env` files — they are gitignored by convention and contain machine-specific paths
- Log files in `logs/` are not committed; the directory is created at job submission time

---

## Adding a New Pipeline Stage

To add a new stage following the project convention:

1. Add the core R function(s) to a new file in `src/<stage_name>.r`
2. Create a thin driver script `scripts/run_<stage_name>.r` that sources `src/setup.r`, `src/utils.r`, and the new `src/<stage_name>.r`, then calls the top-level function with `get_config()`
3. Create a SLURM batch script `scripts/submit_<stage_name>.sh` that activates the appropriate conda environment and calls the run script via `$RSCRIPT_BIN --vanilla`
4. Create or extend a conda environment definition in `environments/<env_name>.yml`
5. Add the new stage to `scripts/master_pipeline.sh` with the correct `--dependency=afterok:` chain
6. Update `docs/ARCHITECTURE.md` to include the new stage in the component diagram and data flow sections

---

## Troubleshooting

**Micromamba not found** — Run `bash scripts/install_micromamba_simple.sh`. If a custom install path is used, set `MAMBA_EXE_CUSTOM` before calling `setup_environments.sh`. See `docs/MICROMAMBA_SETUP.md`.

**Conda environment wiped on HPC** — Scratch storage on ETH Euler is wiped every 60–90 days. Recreate environments with `bash scripts/setup_environments.sh` after a wipe.

**Stage 7 contract validation failure** — Run `bash scripts/hpc_common.sh --check-stage7-contract` to identify which of `HPC_SCRATCH_ROOT`, `HPC_TMP_ROOT`, `TERRA_TEMP` are unset. Source `.env` first.

**Dinamica EGO not found** — Set `DINAMICA_EGO_8_HOME` in `.env` to the installation directory (local) or the `.sif` Apptainer image path (HPC). Set `DINAMICA_BACKEND=hpc` on ETH Euler.

**Memory or time limit exceeded on HPC** — Adjust `#SBATCH --mem-per-cpu` and `#SBATCH --time` in the relevant `submit_*.sh` script. Typical allocations for heavy stages: 32–42 GB/CPU, 3–8 cores, up to 72 h wall time.

**R package missing in conda environment** — The run scripts attempt to install missing packages at startup, but this is a fallback only. Add the package to the relevant `environments/*.yml` and rebuild the environment.
