<!-- generated-by: gsd-doc-writer -->
# Architecture

## System Overview

nascent-lulcc is a spatially-explicit Land Use/Land Cover Change (LULCC) modelling pipeline for Peru, implemented in R and designed to run on the ETH Euler SLURM cluster. The system ingests historic multi-temporal LULC raster data and a suite of environmental, socio-economic, and infrastructural predictor layers, then produces simulated future LULC maps for four scenarios (BAU, NAT, CUL, SOC) from a 2022 baseline through 2060. The core allocation engine is Dinamica EGO, invoked from R via subprocess (local `DinamicaConsole` or HPC Apptainer/Singularity container). The pipeline follows a strict seven-stage sequential dependency graph, enforced at runtime by SLURM `--dependency=afterok:` chains coordinated by `scripts/master_pipeline.sh`.

---

## Component Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│  Inputs                                                               │
│  Raw LULC rasters · Predictor rasters · Region shapefile · Config    │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Stage 1 – Data Preparation                                           │
│  lulc_data_prep · region_prep · ancillary_data_prep                   │
│  calibration_predictor_prep · create_predictor_parquets               │
│  transition_identification · transition_dataset_prep                  │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Stage 2 – Feature Selection                                          │
│  transition_feature_selection                                         │
│  (collinearity filter → GRRF per transition × region)                 │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Stage 3 – Transition Modelling                                       │
│  transition_modelling                                                 │
│  (mlr3: GLM / Random Forest / XGBoost per transition × region)        │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Stage 4 – Allocation Parameter Calibration                           │
│  calibrate_allocation_parameters                                      │
│  (Patcher / Expander parameters via Monte Carlo)                      │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Stage 5 – Scenario Preparation                                       │
│  simulation_trans_rates_prep                                          │
│  (CVXR convex optimisation of transition rate tables per scenario)    │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Stage 6 – Simulation Setup                                           │
│  spatial_interventions_prep                                           │
│  (SSP-specific spatial probability perturbation layers)               │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Stage 7 – Allocation / Dinamica Simulations                          │
│  allocation · exec_dinamica (via dinamica_utils)                      │
│  (Dinamica EGO .ego model, multi-scenario × multi-region)             │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Outputs                                                              │
│  Simulated LULC rasters per scenario/timestep/region                  │
│  outputs/simulations/                                                 │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

A typical end-to-end run follows this sequence:

1. **Reference grid and region preparation** — A spatial reference grid at 100 m resolution (EPSG:2056) is prepared by `lulc_data_prep()`. The study area is partitioned into named regions by `region_prep()`, which rasterises a region shapefile and writes `regions.tif` and `regions.json`.

2. **LULC reclassification and aggregation** — Raw LULC rasters are reclassified to the project's seven-class schema (defined in `config/lulc_schema.json`) and resampled to the reference grid. Aggregated rasters land in `inputs/lulc/aggregated/`.

3. **Predictor preparation** — `calibration_predictor_prep()` aligns terrain, soil, hydrological, climatic, infrastructure, socio-economic, and neighbourhood predictor layers to the reference grid. `create_predictor_parquets()` serialises the aligned layers to Apache Parquet files for fast columnar access during modelling.

4. **Transition identification** — `transition_identification()` computes historic areal change between consecutive LULC snapshots across all regions, producing a `viable_transitions_lists.csv` that enumerates which from–to class pairs have sufficient area to model.

5. **Transition dataset preparation** — `transition_dataset_prep()` assembles per-transition binary response datasets (presence of change vs. no-change sample) joined to predictor values, saved as Parquet files in `outputs/transition_datasets/`.

6. **Feature selection** — `transition_feature_selection()` applies Pearson/VIF collinearity filtering followed by Guided Regularized Random Forest (GRRF) to select the optimal predictor subset per transition × region combination. Results are cached in `outputs/feature_selection/`.

7. **Transition modelling** — `transition_modelling()` trains mlr3 classification models (GLM via `classif.glmnet`, Random Forest via `classif.ranger`, XGBoost via `classif.xgboost`) for each viable transition × region pair, using 3-fold cross-validation (AUC optimisation). Models are serialised with `qs` and saved to `outputs/transition_models/`.

8. **Allocation parameter calibration** — `calibrate_allocation_parameters()` estimates Dinamica EGO Patcher and Expander parameters (mean patch size, variance, isometry, `perc_patcher`, `perc_expander`) from historic LULC change, then performs Monte Carlo perturbation to identify best-performing parameter sets. Calibrated tables are written to `outputs/allocation_parameters/`.

9. **Simulation transition rate preparation** — `simulation_trans_rates_prep()` uses CVXR convex optimisation to derive per-scenario, per-region transition probability matrices that balance historic trends against scenario-specific area demand targets. Scale factors `[1.0, 3.0, 5.0, 9.0]` are applied for scenarios BAU, NAT, CUL, SOC respectively. Output tables go to `outputs/transition_tables/`.

10. **Spatial interventions** — `spatial_interventions_prep()` builds SSP-indexed raster layers that spatially modulate transition probabilities (e.g., protected areas, building zones) at each simulation timestep. Configurations are defined per-scenario in `config/SSP*_interventions.yml`.

11. **Dinamica simulations** — `allocation()` iterates over scenarios, timesteps, and regions, calling `exec_dinamica()` for each combination. Dinamica EGO executes the `.ego` model file (`dinamica/dinamica_model/allocation.ego-decoded`) and writes simulated LULC rasters to `outputs/simulations/`.

---

## Key Abstractions

| Function / File | Location | Role |
|---|---|---|
| `get_config()` | `src/setup.r` | Loads environment-aware YAML config; auto-detects local vs. HPC via SLURM indicators |
| `get_stage7_runtime_paths()` | `src/setup.r` | Single contract for machine-specific env vars (`TERRA_TEMP`, `HPC_SCRATCH_ROOT`, `HPC_TMP_ROOT`, `DINAMICA_EGO_8_HOME`, `DINAMICA_BACKEND`) |
| `transition_identification()` | `src/transition_identification.r` | Enumerates viable LULC from–to transitions from historic rasters |
| `transition_feature_selection()` | `src/transition_feature_selection.r` | Orchestrates collinearity + GRRF predictor selection per transition × region |
| `transition_modelling()` | `src/transition_modelling.r` | mlr3 model training loop (GLM, RF, XGBoost) with cross-validation |
| `calibrate_allocation_parameters()` | `src/calibrate_allocation_parameters.r` | Derives and calibrates Dinamica EGO Patcher/Expander parameters |
| `simulation_trans_rates_prep()` | `src/simulation_trans_rates_prep.r` | CVXR-based scenario transition rate optimisation |
| `spatial_interventions_prep()` | `src/spatial_interventions_prep.r` | Builds SSP-specific spatial probability perturbation rasters |
| `allocation()` | `src/allocation.r` | Stage 7 orchestrator: scenario × region simulation loop with profiling |
| `exec_dinamica()` | `src/dinamica_utils.r` | Unified Dinamica EGO launch (local `DinamicaConsole` or HPC Apptainer/Singularity) |
| `detect_dinamica_backend()` | `src/dinamica_utils.r` | Resolves `DINAMICA_BACKEND` env override or auto-detects from environment |
| `ensure_dir()` | `src/utils.r` | Creates directories recursively; used throughout all stages |
| `lulc_schema.json` | `config/lulc_schema.json` | Canonical LULC class definitions (7 classes, values 101–107) |
| `hpc_config.yaml` / `local_config.yaml` | `config/` | Environment-specific path maps and configuration settings |

---

## Directory Structure Rationale

```
nascent-lulcc/
├── src/                  R source functions (flat, one function per topic)
├── scripts/              Executable entry points: run_*.r wrappers and SLURM
│                         submit_*.sh batch scripts; one pair per pipeline stage
├── config/               YAML and JSON configuration; environment-specific configs
│                         (hpc_config.yaml, local_config.yaml) and domain schemas
│                         (lulc_schema.json, model_specs.yaml, pred_data.yaml,
│                         SSP*_interventions.yml)
├── environments/         Conda environment YAML files; one environment per pipeline
│                         stage (e.g., transition_model_env.yml, allocation_env.yml)
│                         to isolate heavy dependencies and pin package versions
├── dinamica/             Dinamica EGO model files (.ego-decoded format) and the
│                         Apptainer/Singularity container definition for HPC execution
├── docs/                 Human-readable documentation for pipeline stages and setup
├── tests/                testthat unit tests (tests/testthat/) and shell smoke tests
│                         (tests/shell/)
└── logs/                 SLURM job output and error logs (gitignored at runtime)
```

The flat `src/` layout keeps all R functions at a single level of indirection — callers `source()` specific files rather than loading a package namespace. The separation of `scripts/` (executables) from `src/` (functions) means every SLURM job submits a thin `run_*.r` wrapper that sources the relevant `src/` file and calls its top-level function with `get_config()`.

Per-stage Conda environments (managed via micromamba from `scripts/hpc_common.sh`) isolate conflicting R package dependencies — for example, `transition_model_env` includes the full tidymodels stack while `allocation_env` pins only the prediction-time packages needed for Stage 7 inference, avoiding unnecessary installation overhead on compute nodes.

---

## Configuration System

The pipeline uses two parallel configuration mechanisms:

**YAML config files** (`config/hpc_config.yaml`, `config/local_config.yaml`) are the authoritative source for all repository-relative paths and modelling parameters. `get_config()` auto-detects the environment from SLURM variables or hostname patterns and loads the appropriate file. Paths in the YAML may contain `${VAR}` placeholders that are expanded at load time from environment variables.

**Environment variables** are reserved for the small set of values that genuinely vary by host and cannot be committed to the repository. The canonical list is documented in `.env.template` and validated at job start by `scripts/hpc_common.sh:check_stage7_contract()`:

| Variable | Purpose |
|---|---|
| `HPC_SCRATCH_ROOT` | Root of the scratch filesystem data tree |
| `HPC_TMP_ROOT` | Per-job temporary directory root |
| `TERRA_TEMP` | terra raster processing temp directory |
| `DINAMICA_EGO_8_HOME` | Dinamica EGO install directory (local) or `.sif` image path (HPC) |
| `DINAMICA_BACKEND` | `auto` / `local` / `hpc` — overrides backend auto-detection |

---

## LULC Class Schema

The project aggregates source map classes into seven modelling classes defined in `config/lulc_schema.json`:

| Code | Class Name |
|---|---|
| 101 | Forested Areas |
| 102 | Natural Grasslands and Shrublands |
| 103 | Low-Intensity Agricultural Areas |
| 104 | High-Intensity Agricultural Areas |
| 105 | Built-Up and Barren Lands |
| 106 | Mining |
| 107 | Water Body |

---

## Scenario Framework

Four scenarios are modelled, each mapped to an SSP for climatic and intervention context:

| Scenario | SSP Mapping | Scale Factor |
|---|---|---|
| BAU (Business As Usual) | SSP2-4.5 | 1.0 |
| NAT (Nature) | SSP1-2.6 | 3.0 |
| CUL (Culture) | SSP1-2.6 | 5.0 |
| SOC (Society) | SSP1-2.6 | 9.0 |

Scale factors modulate the transition rate optimisation in `simulation_trans_rates_prep()`. SSP-specific spatial interventions are read from `config/SSP*_interventions.yml` and can operate at three stages: pre-allocation (Patcher/Expander parameter adjustment), allocation (spatial probability perturbation), and post-allocation (direct LULC map modification).

---

## HPC Execution Model

Each pipeline stage is packaged as a pair of files:

- `scripts/run_<stage>.r` — thin R script that sources `src/` functions and calls the stage function
- `scripts/submit_<stage>.sh` — SLURM batch script that activates the stage-specific Conda environment and executes the run script

SLURM resource allocations by stage:

| Stage | CPUs | Memory |
|---|---|---|
| Transition Modelling | 3 | 42 GB/CPU |
| Allocation | 8 | 8 GB/CPU |

Jobs are submitted with `sbatch --dependency=afterok:<previous_job_id>` to enforce stage ordering. The `scripts/master_pipeline.sh` script submits all stages in sequence and polls `squeue`/`sacct` for completion status. Conda environments are managed by micromamba, located via `find_micromamba()` in `scripts/hpc_common.sh`.
