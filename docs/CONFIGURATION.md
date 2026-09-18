<!-- generated-by: gsd-doc-writer -->
# Configuration

This document describes all configuration surfaces for the nascent-lulcc LULCC modelling pipeline:
environment variables (the operator-facing shell contract), YAML configuration files (the authoritative
path and parameter map), model specification files, LULC class schema, and scenario intervention files.

---

## Environment Variables

Environment variables are the **only** machine-specific overrides in the pipeline. All repository-relative
paths are resolved from the YAML config files (see [Config File Format](#config-file-format) below).
Variables are declared in `.env.template`; copy this file to `.env` and edit for your machine before
sourcing it in any job script.

```bash
cp .env.template .env
# Edit .env, then:
source .env
```

### Stage 7 Path Contract (HPC-required)

These three variables are validated at job submission time by `scripts/hpc_common.sh
--check-stage7-contract`. Jobs will not start if any of them are unset or empty.

| Variable | Required | Default | Description |
|---|---|---|---|
| `HPC_SCRATCH_ROOT` | **Required on HPC** | _(none)_ | Root of the scratch filesystem data tree, used as `data_basepath` in `hpc_config.yaml`. Example: `/beegfs/$USER/nascent-lulcc`. |
| `HPC_TMP_ROOT` | **Required on HPC** | _(none)_ | Per-job temporary directory root on HPC scratch. Typically `$HPC_SCRATCH_ROOT/temp`. |
| `TERRA_TEMP` | **Required on HPC** | `tempdir()` locally | Directory for `terra` intermediate raster files (`terra::terraOptions(tempdir=...)`). Falls back to `tempdir()` on local machines if unset. |

### Dinamica EGO Runtime Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DINAMICA_EGO_8_HOME` | **Required (both)** | _(none)_ | On HPC: absolute path to the built Dinamica EGO 8 `.sif` Apptainer image (e.g., `/project/<project>/containers/dinamica-ego-8.sif`). Locally: absolute path to the Dinamica EGO 8 installation directory. See `docs/README_HPC.md` for details. |
| `DINAMICA_BACKEND` | Optional | `auto` | Backend selection hint consumed by `exec_dinamica()` in `src/dinamica_utils.r`. One of `auto` (detected from environment), `local`, or `hpc`. |
| `DINAMICA_DISABLE_PARALLEL_STEPS` | Optional | `1` (flag passed) | Whether the allocation step passes `-disable-parallel-steps` to DinamicaConsole. Set `0` to allow Dinamica's step-level parallelism (A/B against `PROFILE stage=dinamica` timings). Dinamica's worker thread count is set separately via `-processors=$SLURM_CPUS_PER_TASK` (automatic). |

### Project Path Variables (HPC)

These are derived from `HPC_SCRATCH_ROOT` and expand automatically when `.env` is sourced:

| Variable | Description |
|---|---|
| `NASCENT_LULCC_HOME` | Repository root on the HPC login node. Default: `/home/$USER/nascent-lulcc`. |
| `NASCENT_LULCC_SRC` | `src/` directory path (`$NASCENT_LULCC_HOME/src`). |
| `NASCENT_LULCC_SCRIPTS` | `scripts/` directory path. |
| `NASCENT_LULCC_CONFIG` | `config/` directory path. |
| `NASCENT_LULCC_SCRATCH` | Scratch data root (same as `HPC_SCRATCH_ROOT`). |
| `NASCENT_LULCC_DATA` | Data subdirectory on scratch (`$HPC_SCRATCH_ROOT/data`). |
| `NASCENT_LULCC_RESULTS` | Results subdirectory on scratch. |
| `NASCENT_LULCC_LOGS` | Logs subdirectory on scratch. |

### Runtime Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CONDA_ENVS_PATH` | Optional | `/home/$USER/environments` | Micromamba/conda environment install root. |
| `R_LIBS_USER` | Optional | `/home/$USER/lib/R` | User R library path. |
| `MAMBA_EXE` | Optional | auto-detected | Path to the micromamba executable. |
| `TMPDIR` | Optional | `$HPC_TMP_ROOT` | System temp directory (used by R and shell tools). |
| `R_TMPDIR` | Optional | `$TMPDIR/R` | R-specific temp directory. |

### Performance Tuning Variables

Set automatically in `.env.template`; may be adjusted per job:

| Variable | Default in `.env.template` | Description |
|---|---|---|
| `OMP_NUM_THREADS` | `$SLURM_CPUS_PER_TASK` (fallback: `1`) | OpenMP thread count for BLAS/parallelised compiled code. |
| `OPENBLAS_NUM_THREADS` | `$SLURM_CPUS_PER_TASK` (fallback: `1`) | OpenBLAS thread count. |
| `GDAL_NUM_THREADS` | `ALL_CPUS` | Number of threads used by GDAL for raster I/O. |
| `GDAL_CACHEMAX` | `8192` (MB) | GDAL block cache size in megabytes. |

### Allocation-Stage Runtime Variables

These variables tune allocation parallelism without requiring a config file edit. They are consumed by
`src/allocation.r`:

| Variable | Required | Default | Description |
|---|---|---|---|
| `ALLOCATION_NUM_WORKERS` | Optional | auto (`parallelly::availableCores()`) | Override the number of parallel workers used during allocation. |
| `ALLOCATION_PARALLEL_STRATEGY` | Optional | auto | Force parallel strategy: `sequential`, `multicore`, or `multisession`. |
| `ALLOCATION_YEAR_POST_FILTER` | Optional | _(none)_ | Single-timestep **smoke** filter: run allocation for exactly this one posterior year. Not a resume control — production resume is driver-side and automatic; do not set this for full/resumed runs. |
| `ALLOCATION_WORKER_RSS_BUDGET_MB` | Optional | _(none)_ | Post-run RSS verification threshold consumed by `verify_phase3_smoke.sh` (MB). It does **not** bound, chunk, or gate anything at runtime — allocation only logs it as a breadcrumb. Use `ALLOCATION_PREDICT_BATCH_ROWS` to actually cap prediction-time peak RSS. |
| `ALLOCATION_PREDICT_BATCH_ROWS` | Optional | _(none / single-shot)_ | Batch large-transition ranger prediction into row-chunks of this size to bound prediction-time peak RSS (e.g. `5000000` for the big forest regions). |
| `ALLOCATION_PREDICT_NUM_THREADS` | Optional | auto (`cores / effective_workers`) | Override ranger `num.threads` for the large-transition predict. |
| `ALLOCATION_PROFILE` | Optional | `FALSE` | Enable profiling output (`TRUE`/`FALSE`). When enabled, emits `PROFILE … elapsed=…s rss_before=…MB rss_after=…MB` lines via the job log. |

---

## Config File Format

Two YAML configuration files serve as the authoritative path and parameter map for the pipeline.
`src/setup.r:get_config()` detects the environment automatically (local vs HPC) and loads the
appropriate file. Override detection by passing `force_environment = "local"` or `force_environment = "hpc"`.

| File | Environment | `data_basepath` |
|---|---|---|
| `config/local_config.yaml` | Local workstation | `E:/nascent-lulcc-agg` (hardcoded) |
| `config/hpc_config.yaml` | HPC (Euler SLURM) | `${HPC_SCRATCH_ROOT}` (expanded at load time) |

Environment auto-detection checks for SLURM job IDs, PBS job IDs, presence of ``, and Euler
hostname patterns. Both config files share the same top-level key structure:

```yaml
environment: "hpc"          # or "local"

# All paths below are relative to data_basepath (expanded and created on first load)
data_basepath: "${HPC_SCRATCH_ROOT}"
inputs_dir: "inputs"
outputs_dir: "outputs"
tools_dir: "tools"
config_dir: "config"
temp_dir: "temp"

input_dirs:           # Relative to data_basepath; created automatically if absent
  reg_dir: "inputs/regionalization"
  predictors_dir: "inputs/predictors"
  ...

output_dirs:          # Relative to data_basepath; created automatically if absent
  calibration_param_dir: "outputs/allocation_parameters/calibration"
  simulation_param_dir: "outputs/allocation_parameters/simulation"
  ...

input_output_files_paths:   # Key individual file paths relative to data_basepath
  viable_transitions_lists: "outputs/transition_identification/viable_transitions_lists.csv"
  ref_grid_path: "inputs/spatial_reference_grid/ref_grid_aggregated.tif"
  lulc_demand_path: "inputs/lulc/future_demand/lulc_demand_results.csv"
  ...

config_files_paths:   # Relative to the project root (repository), not data_basepath
  lulc_aggregation_path: "config/lulc_schema.json"
  model_specs_path: "config/model_specs.yaml"
  pred_table_path: "config/pred_data.yaml"
  model_lookup_path: "tools/model_lookup.xlsx"
  ...

configuration_settings:     # Spatial and simulation settings
  ref_grid_target_cellsize: 100      # metres
  reference_crs: "epsg:2056"
  step_length: 4
  data_periods: ["2018_2022"]
  regionalization: true
  scenario_names: ["BAU", "NAT", "CUL", "SOC"]
  scenario_to_ssp_mapping:
    BAU: "ssp245"
    NAT: "ssp126"
    CUL: "ssp126"
    SOC: "ssp126"
  simulation_start_year: 2022
  simulation_end_year: 2060
  selected_scalar: 9.0
  max_training_rows: 1000000        # HPC: 1 000 000; local: 500 000

simulation_trans_rates_params:      # Solver tuning for transition rate preparation
  ...                               # See section below

transition_identification_params:
  num_workers: 8                    # HPC: 8; local: 4
```

### `configuration_settings` Keys

| Key | HPC value | Local value | Description |
|---|---|---|---|
| `ref_grid_target_cellsize` | `100` | `100` | Target raster cell size in metres. |
| `reference_crs` | `epsg:2056` | `epsg:2056` | Coordinate reference system (Swiss LV95). |
| `step_length` | `4` | `4` | Number of years per simulation time step. |
| `data_periods` | `["2018_2022"]` | `["2018_2022"]` | LULC calibration periods. |
| `regionalization` | `true` | `true` | Whether to apply spatial regionalization. |
| `scenario_names` | `["BAU","NAT","CUL","SOC"]` | same | Scenario identifiers. |
| `simulation_start_year` | `2022` | `2022` | First simulation year. |
| `simulation_end_year` | `2060` | `2060` | Final simulation year. |
| `selected_scalar` | `9.0` | `9.0` | Default scale factor applied during calibration. |
| `max_training_rows` | `1000000` | `500000` | Maximum rows sampled for transition model training. Tune to available RAM. |

---

## Simulation Transition Rates Parameters

The `simulation_trans_rates_params` block in both YAML files controls the constrained optimisation
solver used in `src/simulation_trans_rates_prep.r` to derive per-scenario transition probability
matrices. All values are identical between `hpc_config.yaml` and `local_config.yaml`.

| Parameter | Value | Description |
|---|---|---|
| `margin` | `0.50` | Margin of allowed deviation from historic rates. |
| `stay_min` | `0.10` | Minimum diagonal (stay) probability. |
| `stay_max` | `0.999` | Maximum diagonal (stay) probability. |
| `eps_ridge` | `1.0e-8` | Ridge regularisation epsilon for numerical stability. |
| `solver_eps` | `1.0e-5` | Solver convergence tolerance. |
| `monotone_tol` | `1.0e-3` | Tolerance for monotonicity constraints. |
| `lambda` | `0.001` | L2 regularisation weight. |
| `eta_pref` | `0.001` | Preferred step size for the iterative solver. |
| `rho` | `100` | ADMM penalty parameter. |
| `mu` | `5` | ADMM penalty adaptation factor. |
| `fair_weight` | `5` | Fairness regularisation weight across transitions. |
| `kappa_zero` | `0.1` | Zero-rate penalisation strength. |
| `zero_hist_thresh` | `1.0e-5` | Threshold below which a historic rate is treated as zero. |
| `beta_dev` | `1.0` | Deviation penalty weight from target rates. |
| `rel_guard` | `0.05` | Relative guard band around historical values. |
| `forbid_inflow` | `[]` | LULC classes that are forbidden from receiving inflow. |
| `num_workers` | `6` | Parallel workers for transition rate estimation. |
| `scale_factor` | `[1.0, 3.0, 5.0, 9.0]` | Per-scenario scale multipliers applied in the same order as `scenario_names`. |

---

## Model Specifications (`config/model_specs.yaml`)

Controls cross-validation and hyperparameter grids for the three transition model algorithms.

```yaml
global:
  cv_folds: 3           # Cross-validation folds used by the mlr3 AutoTuner
  random_seed: 123

metrics:
  optimization_metric: classif.auc

models:
  glm:
    parameters:
      alpha: [1]         # Lasso (glmnet mixture = 1)
      s: [0.01]          # Regularisation penalty (lambda in glmnet)

  rf:
    parameters:
      num.trees: [500]
      min.node.size: [5]
      mtry: [4]

  xgboost:
    parameters:
      nrounds: [100]
      max_depth: [6]
      eta: [0.1]
      min_child_weight: [5]
      colsample_bytree: [0.8]
```

The `max_final_fit_size` and `num_replicates` global keys are retained for reference but are not
actively used; `max_training_rows` from the top-level config file takes precedence for sample caps,
and AutoTuner CV folds control replication.

---

## LULC Class Schema (`config/lulc_schema.json`)

Defines how raw LULC raster classes are aggregated into the seven modelling classes used throughout
the pipeline. Each entry maps `original_classes` (raw integer values from the source map) to an
aggregated class.

| Aggregated class | Value | `nhood_class` | Original source classes |
|---|---|---|---|
| Forested Areas | 101 | false | Forest (3), Dry Forest (4), Mangrove (5), Flooded forest (6) |
| Natural Grasslands and Shrublands | 102 | false | Flooded grassland/shrubland (11), Grasslands/herbaceous (12), Salt Flat (32), Scrubland (13) |
| Low-Intensity Agricultural Areas | 103 | true | Pasture (15), Mosaic of agriculture and pasture (21) |
| High-Intensity Agricultural Areas | 104 | true | Agriculture (18), Forest plantation (9), Oil palm (35), Aquaculture (31) |
| Built-Up and Barren Lands | 105 | true | Infrastructure (24), Other non-vegetated area (25) |
| Mining | 106 | false | Mining (30) |
| Water body | 107 | false | River/lake/ocean (33), Glacier (34) |

The `nhood_class` flag marks classes included in neighbourhood predictor calculations.

---

## Ancillary Spatial Data (`config/ancillary_data.yaml`)

Declares paths to administrative boundary layers used for regionalization and reporting. All
`raw_dir` entries are relative to the `ancillary_spatial_dir` input directory.

| Layer | Raw filename | Prepared path |
|---|---|---|
| Country boundary | `per_admbnda_adm0_ign_20200714.shp` | `ancillary_spatial_data/prepared/country.shp` |
| Regions (admin level 1) | `per_admbnda_adm1_ign_20200714.shp` | `ancillary_spatial_data/prepared/regions.shp` |
| Provinces (admin level 2) | `per_admbnda_adm2_ign_20200714.shp` | `ancillary_spatial_data/prepared/provinces.shp` |
| Districts (admin level 3) | `per_admbnda_adm3_ign_20200714.shp` | `ancillary_spatial_data/prepared/districts.shp` |

Source: OCHA Peru administrative boundaries (HUMDATA). <!-- VERIFY: source URL and metadata file remain current -->

---

## Scenario Intervention Files (`config/SSP*_interventions.yml`)

One YAML file per scenario controls the spatial interventions injected into the Dinamica EGO
allocation step. Four scenario files are shipped:

| File | Scenario mapping |
|---|---|
| `config/SSP0_interventions.yml` | Baseline (no interventions) |
| `config/SSP1_interventions.yml` | SSP1 / sustainability pathway |
| `config/SSP3_interventions.yml` | SSP3 / regional rivalry |
| `config/SSP4_interventions.yml` | SSP4 / inequality |
| `config/SSP5_interventions.yml` | SSP5 / fossil-fuelled development |

Each file is a YAML list of intervention blocks. Each block specifies:

| Key | Description |
|---|---|
| `Intervention_stage` | When the intervention fires: `Pre-allocation`, `Allocation`, or `Post-allocation`. |
| `Intervention_ID` | Unique string identifier for the intervention. |
| `Time_steps_implemented` | List of simulation years (integer) when the intervention is active. |
| `Transition_target_classes` | LULC class name(s) affected. |

**Pre-allocation keys** (modify Dinamica spatial patch parameters):

| Key | Options/Example | Description |
|---|---|---|
| `Param_adjust_type` | `Absolute` | How the parameter is set. |
| `Param_adjust_name` | `Mean_Patch_Size`, `Patch_Size_Variance`, `Patch_Isometry`, `Perc_expander`, `Perc_patcher` | Which patch parameter to modify. |
| `Param_adjust_value` | `0.15` | The value to apply. |

**Allocation keys** (modify transition probabilities):

| Key | Options/Example | Description |
|---|---|---|
| `Intervention_ranking` | `1`, `2`, … | Execution order within the stage (lower = higher priority). |
| `Prob_adjust_type` | `Absolute`, `Relative` | Whether to set a fixed probability or modify by percentile. |
| `Prob_adjust_valency` | `Increase_inside_decrease_outside`, `Decrease`, … | Direction of relative adjustment. |
| `Prob_adjust_intervention_percentile` | `90` | Percentile threshold for intervention cells. |
| `Prob_adjust_non_intervention_percentile` | `90` | Percentile threshold for non-intervention cells. |
| `Prob_adjust_threshold` | `5` | Minimum probability for adjustment to apply. |
| `Prob_adjust_zone` | `Inside`, `Outside` | Which side of the mask is considered the intervention zone. |
| `Mask_type` | `Static`, `Dynamic` | Whether the spatial mask changes over time. |
| `Intervention_mask` | path string or year-keyed map | Raster file defining the intervention zone. Dynamic masks use a year-keyed mapping. |
| `From_lulc_filter` | `["Alp_Past", "Int_AG"]` | (Optional) Restrict cell selection to these source LULC classes. |

**Conservation-area expansion keys** (used with `Mask_type: Dynamic`):

| Key | Example | Description |
|---|---|---|
| `Ca_expansion_target` | `30%` | Target protected area coverage for the full simulation period. |
| `Ca_prioritization` | `Biodiversity`, `NCPs`, `Cultural`, `Agricultural unproductive` | Basis for selecting new conservation areas. |
| `Ca_patch_preference` | `Large_patches`, `Small_patches`, `Connectivity` | Patch type prioritized when expanding. |
| `Ca_expansion_rate` | `Rapid`, `Steady`, `Lagged` | Temporal rate of expansion. |
| `Ca_expansion_start_year` / `Ca_expansion_end_year` | `2020` / `2030` | Year window for expansion. |
| `Ca_types` | list of strings | Conservation area types included (e.g., `Ramsar`, `Swiss_National_Park`, `Forest_reserves`). |

---

## Per-Environment Overrides

### HPC vs Local differences

The two YAML files are structurally identical. The only differences are:

| Setting | Local (`local_config.yaml`) | HPC (`hpc_config.yaml`) |
|---|---|---|
| `data_basepath` | `E:/nascent-lulcc-agg` (literal path) | `${HPC_SCRATCH_ROOT}` (env var placeholder) |
| `max_training_rows` | `500000` | `1000000` |
| `transition_identification_params.num_workers` | `4` | `8` |

### Forcing the environment at runtime

Pass `force_environment` to `get_config()` in R to override auto-detection:

```r
config <- get_config(force_environment = "hpc")
config <- get_config(force_environment = "local")
```

### Stage 7 path contract validation (HPC shell)

Run the following before submitting any Stage 7 SLURM job:

```bash
bash scripts/hpc_common.sh --check-stage7-contract
```

This validates `HPC_SCRATCH_ROOT`, `HPC_TMP_ROOT`, and `TERRA_TEMP` are set and non-empty. Job
submission scripts source `hpc_common.sh` and call `setup_common_env()`, which runs this check and
exits non-zero if the contract is incomplete.

---

## Required vs Optional Summary

| Setting | Required for HPC jobs | Fails if absent |
|---|---|---|
| `HPC_SCRATCH_ROOT` | Yes | Yes — pre-flight gate exits 1 |
| `HPC_TMP_ROOT` | Yes | Yes — pre-flight gate exits 1 |
| `TERRA_TEMP` | Yes (HPC) | Yes — pre-flight gate exits 1 |
| `DINAMICA_EGO_8_HOME` | Yes (both) | Yes — `exec_dinamica()` errors clearly |
| `DINAMICA_BACKEND` | No | No — defaults to `auto` |
| `ALLOCATION_*` tuning vars | No | No — safe defaults apply |
| `GDAL_CACHEMAX` / `OMP_NUM_THREADS` | No | No — defaults apply |
