# ARCHITECTURE — nascent-lulcc
_Last updated: 2026-04-30_

## Summary

nascent-lulcc is a Land Use/Land Cover Change (LULCC) modelling pipeline for Peru, structured as a sequential, HPC-oriented batch processing system. The system proceeds through seven discrete, dependency-chained stages — from raw raster data preparation through statistical transition modelling to spatially-explicit forward simulations via the Dinamica EGO engine. All business logic lives in R source files (`src/`), all execution entry points are R driver scripts (`scripts/run_*.r`), and HPC job submission is handled by paired shell scripts (`scripts/submit_*.sh`).

---

## Overall Pattern: Sequential Scientific Pipeline

The architecture follows a **linear scientific pipeline** pattern. Each stage consumes file-based outputs from the prior stage and writes its own outputs to a configured directory on the data filesystem. There is no message bus, no shared in-memory state between stages, and no web layer. The "glue" between stages is SLURM job dependencies (`--dependency=afterok:<job_id>`).

```
Raw LULC rasters + predictor layers (external data filesystem)
        │
        ▼
[Stage 1] Data Preparation  (lulc_data_prep, region_prep, ancillary_data_prep,
          calibration_predictor_prep, create_predictor_parquets,
          transition_identification, transition_dataset_prep)
        │  outputs: aggregated LULC .tif, predictor .parquet, transition datasets
        ▼
[Stage 2] Feature Selection  (transition_feature_selection → GRRF)
        │  outputs: per-transition selected predictor lists (.rds / .csv)
        ▼
[Stage 3] Transition Modelling  (transition_modelling → tidymodels GLM/RF/XGBoost)
        │  outputs: fitted model objects (.rds), evaluation summaries
        ▼
[Stage 4] Allocation Parameter Calibration  (calibrate_allocation_parameters)
        │  outputs: Dinamica patch parameter tables (calibration & simulation)
        ▼
[Stage 5] Scenario Preparation  (simulation_trans_rates_prep → CVXR optimisation)
        │  outputs: scenario-specific transition rate tables per region
        ▼
[Stage 6] Simulation Setup  (spatial_interventions_prep)
        │  outputs: pre-computed spatial intervention raster stacks
        ▼
[Stage 7] Dinamica EGO Simulations  (run_allocation → exec_dinamica)
           outputs: simulated LULC maps 2022–2060 per scenario × region
```

---

## Component Responsibilities

| Component | Role | Key file(s) |
|-----------|------|-------------|
| Configuration layer | Environment-aware path/param resolution | `src/setup.r` |
| LULC schema | Class aggregation + colour mapping | `config/lulc_schema.json` |
| Model hyperparams | GLM / RF / XGBoost grid specs | `config/model_specs.yaml` |
| Predictor metadata | Layer names and data source catalogue | `config/pred_data.yaml` |
| Scenario interventions | Per-SSP spatial + probability rules | `config/SSP*_interventions.yml` |
| Data prep functions | Raster processing, region/predictor assembly | `src/lulc_data_prep.r`, `src/ancilliary_data_prep.r`, `src/region_prep.r`, `src/*_pred_prep.r` |
| Transition identification | Historic LULC change rates, viable pair list | `src/transition_identification.r` |
| Dataset assembly | Per-transition tabular datasets (Parquet) | `src/transition_dataset_prep.r`, `src/create_predictor_parquets.r` |
| Feature selection | Collinearity filter + GRRF selection | `src/transition_feature_selection.r` |
| Transition modelling | tidymodels training + cross-validation | `src/transition_modelling.r` |
| Allocation calibration | Dinamica patch parameter Monte-Carlo | `src/calibrate_allocation_parameters.r` |
| Transition rate prep | CVXR convex-optimisation for scenario rates | `src/simulation_trans_rates_prep.r` |
| Spatial interventions | Probability perturbation layer assembly | `src/spatial_interventions_prep.r`, `src/implement_spatial_interventions.R` |
| Probability manipulation | Apply per-scenario spatial rules at sim time | `src/lulcc.spatprobmanipulation.r` |
| Allocation orchestrator | Loop scenarios × timesteps × regions → Dinamica | `src/allocation.r` |
| Dinamica EGO bridge | Subprocess execution via `processx` | `src/dinamica_utils.r` |
| C++ extensions | Spatial hashing for neighbour lookup; patch stats | `src/neighbors.cpp`, `src/patch_stats.cpp` |
| Utilities | Raster I/O, directory helpers, logging | `src/utils.r`, `src/utils-pipe.r` |
| R driver scripts | CLI entry points, load `src/` functions, call main fn | `scripts/run_*.r` |
| HPC job scripts | SLURM `sbatch` wrappers with resource requests | `scripts/submit_*.sh` |
| Master pipeline | Chained job submission with `afterok` dependencies | `scripts/master_pipeline.sh` |

---

## Configuration System

Environment detection is automatic (`src/setup.r → detect_environment()`):
- Checks for SLURM env vars (`SLURM_JOB_ID`), `` mountpoint, Euler hostname patterns.
- Selects `config/local_config.yaml` (local, `data_basepath: E:/nascent-lulcc-agg`) or `config/hpc_config.yaml` (HPC, `data_basepath: /beegfs/black/nascent-lulcc`).

`build_full_config()` expands all relative paths in the YAML against `data_basepath` and flattens the nested map into a single named list. All downstream functions accept `config = get_config()` as a default argument — they can be called with no arguments in an interactive session and will auto-configure.

Key config categories:
- `input_dirs` — source rasters, predictor layers
- `output_dirs` — model files, evaluation summaries, simulation outputs
- `input_output_files_paths` — canonical single-file references (ref grid, viable transitions list, demand CSV)
- `config_files_paths` — project-root config files resolved independently of `data_basepath`
- `configuration_settings` — scenario names, CRS, step length, regionalization flag
- `simulation_trans_rates_params` — CVXR optimisation hyperparameters

---

## Data Flow: Primary Path

1. **Raw LULC rasters** (external, pre-existing on data filesystem) → `lulc_data_prep()` reclassifies via `config/lulc_schema.json` and aggregates to 100 m grid → `inputs/lulc/aggregated/`
2. **Predictor rasters** (terrain, soil, climate, infrastructure, population, etc.) → various `src/*_pred_prep.r` functions → `inputs/predictors/prepared/`; assembled into columnar Parquet files by `create_predictor_parquets()` → `inputs/predictors/prepared/layers/`
3. **Transition identification** reads historic LULC pairs, computes area-change rates, emits `viable_transitions_lists.csv`
4. **Dataset assembly** joins LULC transition labels with predictor values for each viable transition × region → Parquet files in `outputs/transition_datasets/`
5. **Feature selection** reads Parquet datasets, runs collinearity filter then GRRF, writes selected predictor lists → `outputs/feature_selection/`
6. **Transition modelling** reads Parquet + selected features, trains GLM/RF/XGBoost via `tidymodels`, writes `.rds` model objects → `outputs/transition_models/`; writes reconciliation `.rds` (which transitions succeeded) → `outputs/transition_model_evaluation/`
7. **Allocation calibration** uses historic LULC rasters to estimate Dinamica patch parameters (mean patch size, variance, isometry, patcher/expander %) via Monte-Carlo → `outputs/allocation_parameters/`
8. **Transition rate preparation** uses CVXR to optimise scenario-specific annual transition rates from the historical base + SSP demand targets → `outputs/transition_tables/`
9. **Spatial interventions** assembles per-SSP spatial masks and probability adjustment rules → `inputs/spat_prob_perturb/`
10. **Allocation** (`run_allocation`) iterates scenarios × timesteps × regions; calls fitted models to predict transition probability maps; applies spatial interventions via `lulcc.spatprobmanipulation()`; invokes `DinamicaConsole` (external binary) via `exec_dinamica()`; outputs simulated LULC `.tif` per step → `outputs/simulations/`

---

## Parallelism Model

- **Between stages:** SLURM job-level parallelism via `afterok` dependency chains.
- **Within stages:** R `future`/`furrr` multisession workers, controlled by `ALLOCATION_NUM_WORKERS` env var (default 4 for allocation). Feature selection and transition modelling also use `future` internally.
- **Allocation stage:** `future::multisession` — each worker handles one scenario-region-timestep combination.
- **C++ extensions** (`src/neighbors.cpp`, `src/patch_stats.cpp`) compiled via `Rcpp` for performance-critical spatial operations (neighbour detection using spatial hashing).

---

## Regionalization

The pipeline supports optional spatial stratification into sub-national regions (controlled by `config$regionalization: true`). A `regions.tif` raster + `regions.json` name map live in `inputs/regionalization/`. Feature selection, transition modelling, allocation parameter calibration, and simulation all iterate over regions when regionalization is enabled. Each transition × region combination gets its own model file.

---

## Scenario Framework

Four scenarios are defined in `local_config.yaml`: `BAU`, `NAT`, `CUL`, `SOC`. Each maps to an SSP (`ssp245` or `ssp126`). Scenario differentiation occurs through:
1. Different SSP-linked demand futures (LULC area targets 2022–2060).
2. Per-scenario spatial intervention files: `config/SSP0/1/3/4/5_interventions.yml` (three stages: pre-allocation patch parameter tweaks, allocation probability adjustments, post-allocation map edits).
3. Scale factors applied to transition rates (`simulation_trans_rates_params.scale_factor`).

---

## External Dependencies

- **Dinamica EGO 8** (`DinamicaConsole` binary) — spatial allocation engine. Invoked as a subprocess by `exec_dinamica()` in `src/dinamica_utils.r`. Requires `DINAMICA_EGO_8_HOME` env var.
- **Conda environments** (`environments/`) — separate environments per pipeline stage to manage conflicting R package requirements (e.g., `xgboost` version pinning in `transition_model_env.yml`).
- **SLURM** — job scheduler assumed for HPC execution; not required for local runs.

---

## Anti-Patterns

### Mixed `raster` and `terra` usage
**What happens:** Some functions (notably `src/spatial_interventions_prep.r`, `src/lulcc.spatprobmanipulation.r`) use the legacy `raster` package while newer code uses `terra`.
**Why it's wrong:** Forces both packages to be loaded simultaneously, increases memory overhead, and requires conversion between `SpatRaster` and `RasterLayer` objects at boundaries.
**Do this instead:** Standardise on `terra` throughout, converting any remaining `raster` calls in `src/spatial_interventions_prep.r` and `src/lulcc.spatprobmanipulation.r`.

### Log file placement in Dinamica model directory
**What happens:** `exec_dinamica()` in `src/dinamica_utils.r` writes timestamped log files to `dirname(model_path)` (the `dinamica/dinamica_model/` directory).
**Why it's wrong:** Pollutes the version-controlled model directory with runtime logs; a TODO comment in the code flags this.
**Do this instead:** Write Dinamica logs to the project-level `logs/` directory, consistent with all other stage logs.

---

## Gaps / Unknowns

- The `scripts/master_pipeline.sh` references `submit_scenario_preparation.sh` and `submit_simulation_setup.sh` which do not appear in the repository — these stages may not yet be fully implemented for the current branch (`allocation-step`).
- `config/SSP0_interventions.yml` exists but `SSP0` is not a standard SSP designation — its relationship to the four named scenarios (`BAU`, `NAT`, `CUL`, `SOC`) is not documented.
- Intervention configs in `config/SSP*_interventions.yml` reference mask paths under `Data/Spat_prob_perturb_layers/` (Swiss-context paths) which appear to be inherited from an earlier project version (LULCC-CH/evoland) and may not reflect the Peru study area paths.
- The `src/old/` directory contains deprecated alternatives; it is not clear which, if any, of those functions are still referenced anywhere.
- Post-allocation map finalisation logic is referenced in old code (`src/old/simulated_map_finalisation.r`) but no active counterpart exists in `src/`.
