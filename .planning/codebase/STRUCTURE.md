# STRUCTURE — nascent-lulcc
_Last updated: 2026-04-30_

## Summary

nascent-lulcc is an R-based scientific pipeline project structured as a flat source library (`src/`) driven by thin CLI scripts (`scripts/`). There is no package installation step at runtime — scripts source `src/` files directly. Configuration, environments, and Dinamica model assets each occupy their own top-level directory, keeping code separate from data definitions and execution infrastructure.

---

## Directory Layout

```
nascent-lulcc/
├── src/                    # All R source functions (flat, no subdirs except old/)
│   ├── setup.r             # Environment detection + config loading (entry point for all stages)
│   ├── utils.r             # Shared utilities: raster I/O, dir helpers, logging
│   ├── utils-pipe.r        # magrittr pipe re-export
│   ├── allocation.r        # Allocation stage orchestrator + profiling helpers
│   ├── dinamica_utils.r    # DinamicaConsole subprocess wrapper
│   ├── lulcc.spatprobmanipulation.r  # Spatial probability intervention application
│   ├── implement_spatial_interventions.R  # Intervention rule implementation
│   ├── spatial_interventions_prep.r  # Pre-simulation intervention layer assembly
│   ├── lulc_data_prep.r    # LULC reclassification + aggregation
│   ├── ancilliary_data_prep.r  # Ancillary spatial data preparation
│   ├── region_prep.r       # Study area regionalization
│   ├── transition_identification.r  # Historic LULC transition detection
│   ├── transition_dataset_prep.r    # Tabular dataset assembly (Parquet)
│   ├── transition_feature_selection.r  # Collinearity filter + GRRF
│   ├── transition_modelling.r       # tidymodels training pipeline
│   ├── calibrate_allocation_parameters.r  # Dinamica patch parameter calibration
│   ├── calibration_predictor_prep.r  # Predictor prep for calibration period
│   ├── simulation_trans_rates_prep.r  # CVXR scenario transition rate optimisation
│   ├── create_predictor_parquets.r   # Assemble predictor Parquet files
│   ├── *_pred_prep.r       # Per-theme predictor preparation (terrain, soil,
│   │                       #   climate, infrastructure, population, nhood, inei,
│   │                       #   hydrological, socio_economic)
│   ├── extract_valid_cells_with_region.r  # Cell filtering helper
│   ├── landscape_pattern_analysis.r  # Landscape metrics
│   ├── parquet_check.r     # Parquet file integrity checks
│   ├── neighbors.cpp       # C++/Rcpp spatial hash neighbour detection
│   ├── patch_stats.cpp     # C++/Rcpp patch statistics
│   ├── RcppExports.cpp     # Auto-generated Rcpp glue (do not edit)
│   ├── RcppExports.r       # Auto-generated R side of Rcpp exports
│   └── old/                # Deprecated/superseded scripts (not sourced by active pipeline)
│
├── scripts/                # Executable entry points
│   ├── run_*.r             # R CLI drivers — source src/ files and call main function
│   ├── submit_*.sh         # SLURM sbatch wrappers for each pipeline stage
│   ├── master_pipeline.sh  # End-to-end SLURM orchestration with afterok deps
│   ├── hpc_common.sh       # Shared shell functions for HPC scripts
│   ├── setup_environments.sh / install_micromamba*.sh / quick_setup.sh
│   ├── download_*.py       # Climate data download utilities (Python)
│   ├── calculate_*.r / process_climate_data.r  # Climate raster processing
│   ├── dist_calc_hpc.r / run_dist_calc.r / run_dist_calc.sbatch
│   ├── run_climatic_data_prep.sbatch
│   ├── submit_allocation.sh / submit_allocation_profile.sh
│   └── summarise_allocation_profile.r  # Parse profiling logs
│
├── config/                 # Static configuration — committed to repo
│   ├── local_config.yaml   # Local dev paths + all scenario/simulation settings
│   ├── hpc_config.yaml     # HPC (Euler) paths (mirrors local_config structure)
│   ├── lulc_schema.json    # LULC class definitions: value, name, original classes, colour
│   ├── model_specs.yaml    # ML model hyperparameter grids (GLM, RF, XGBoost)
│   ├── pred_data.yaml      # Predictor layer catalogue
│   ├── ancillary_data.yaml # Ancillary spatial data catalogue
│   └── SSP*_interventions.yml  # Per-scenario intervention rules (SSP0,1,3,4,5)
│
├── environments/           # Conda environment definitions (one per pipeline stage)
│   ├── allocation_env.yml          # Packages for allocation stage
│   ├── allocation_params_env.yml   # Packages for parameter calibration
│   ├── data_prep_env.yml           # Packages for data preparation
│   ├── dist_calc_env.yml           # Packages for distance calculation
│   ├── feat_select_env.yaml        # Packages for feature selection
│   ├── clim_data_env.yml           # Packages for climate data download
│   ├── trans_rate_estimation_env.yml  # Packages for CVXR optimisation
│   └── transition_model_env.yml    # Packages for tidymodels training
│
├── dinamica/               # Dinamica EGO model assets
│   └── dinamica_model/
│       ├── allocation.ego-decoded  # Standalone allocation model
│       ├── evoland.ego-decoded     # Main simulation model
│       └── evoland_ego_Submodels/  # Submodel components
│           ├── AllocateTransitions.ego-decoded
│           ├── CalcSimilarityOfDifferences.ego-decoded
│           ├── CreateCubeOfProbabilityMaps.ego-decoded
│           ├── ExpandTableToUniqueKeys.ego-decoded
│           └── ListFilenames.ego-decoded
│
├── docs/                   # Developer documentation
│   ├── HPC_PIPELINE_README.md      # Stage-by-stage HPC usage guide
│   ├── README_HPC.md               # Euler-specific setup notes
│   ├── CACHE_REFRESH_BEHAVIOR.md   # When/how caching works per stage
│   ├── MICROMAMBA_SETUP.md         # Conda environment setup
│   ├── SIMULATION_TRANS_RATES_REFACTOR_SUMMARY.md
│   └── TRANS_RATE_ESTIMATION_ENV.md
│
├── .planning/              # GSD planning artefacts (not part of pipeline)
│   └── codebase/           # Codebase map documents
│
├── DESCRIPTION             # R package manifest (package name: evoland)
├── README.md               # Project overview + HPC setup quickstart
├── TODO.md                 # Active work items
├── LICENSE
├── .env.template           # Template for runtime environment variables
├── .gitignore
├── .gitattributes
├── intervention_planning.txt
└── spatial_interventions_integration_explainer.md
```

---

## Directory Purposes

**`src/`**
- Purpose: All R function definitions for the pipeline. Flat structure — no sub-packages, no `R/` install layout.
- Key entry point: `src/setup.r` (`get_config()`) is sourced first by every driver script.
- C++ files (`neighbors.cpp`, `patch_stats.cpp`) are compiled via `Rcpp::sourceCpp()` or package build. `RcppExports.*` are auto-generated.
- `src/old/` — archived scripts from earlier project iterations; not sourced by any active script.

**`scripts/`**
- Purpose: Executable entry points. Each `run_*.r` script sources the required `src/` files, loads config, and calls a single top-level function.
- Each stage has a `run_<stage>.r` (R logic) and a `submit_<stage>.sh` (SLURM wrapper that calls the R script in the correct conda environment).
- `master_pipeline.sh` chains all `submit_*.sh` calls with SLURM `--dependency=afterok` to enforce stage ordering.

**`config/`**
- Purpose: All static, version-controlled configuration. Nothing in `config/` contains secrets.
- `lulc_schema.json` is the single source of truth for LULC class definitions.
- Both `local_config.yaml` and `hpc_config.yaml` share an identical key structure — `setup.r` selects the right one.
- `SSP*_interventions.yml` files define scenario-specific intervention sequences (pre-allocation, allocation, post-allocation stages).

**`environments/`**
- Purpose: Conda environment specs, one per pipeline stage. Separate environments manage dependency conflicts (e.g., `xgboost=1.7` pinned for tidypredict compatibility in `transition_model_env.yml`).
- Not committed data — committed YAML specs that are used to build environments on HPC.

**`dinamica/`**
- Purpose: Dinamica EGO model files (`.ego-decoded` format). These define the spatial allocation logic executed by `DinamicaConsole`.
- Version-controlled because the `.ego-decoded` text format is diff-able.
- Runtime Dinamica log files should NOT be committed here (see Architecture > Anti-Patterns).

---

## Key File Locations

**Configuration entry point:**
- `config/local_config.yaml` — primary config for local development; edit `data_basepath` to match local data drive.
- `config/hpc_config.yaml` — HPC config; `data_basepath: /cluster/scratch/bblack/nascent-lulcc`.

**Pipeline orchestration:**
- `scripts/master_pipeline.sh` — submit the entire pipeline as chained SLURM jobs.
- `scripts/run_allocation.r` — standalone CLI entry for the allocation stage (most complex, supports profiling via `ALLOCATION_PROFILE` env var).

**Core function files:**
- `src/setup.r` — `get_config()`, `detect_environment()`, `build_full_config()`
- `src/allocation.r` — `run_allocation()`, profiling helpers (`prof_tic`, `prof_toc`)
- `src/dinamica_utils.r` — `exec_dinamica()`
- `src/transition_modelling.r` — `transition_modelling()`, `perform_transition_modelling()`
- `src/simulation_trans_rates_prep.r` — `simulation_trans_rates_prep()`
- `src/utils.r` — `ensure_dir()`, `write_raster()`, `log_msg()`

**Schema/model specs:**
- `config/lulc_schema.json` — 7 LULC classes (values 101–107), with original class mapping and neighbourhood flag.
- `config/model_specs.yaml` — ML model grids: GLM (penalty, mixture), RF (trees, min_n, mtry), XGBoost (trees, tree_depth, learn_rate, min_n, mtry).

---

## Naming Conventions

**R source files (`src/`):**
- `snake_case` throughout.
- Theme predictor files follow `<theme>_pred_prep.r` pattern: `terrain_pred_prep.r`, `soil_pred_prep.r`, `climate_pred_prep.r`, `infrastructure_pred_prep.r`, `population_pred_prep.r`, `nhood_predictor_prep.r`, `hydrological_pred_prep.r`, `socio_economic_pred_prep.r`, `inei_pred_prep.r`.
- Function names match file names (one primary exported function per file).

**Driver scripts (`scripts/`):**
- `run_<stage>.r` for R CLI entry points.
- `submit_<stage>.sh` for SLURM wrappers.
- Always paired: every `run_*.r` has a corresponding `submit_*.sh`.

**Config files:**
- `<environment>_config.yaml` for environment-specific configs.
- `SSP<number>_interventions.yml` for scenario intervention files.

---

## Where to Add New Code

**New predictor theme:**
- Implementation: `src/<theme>_pred_prep.r` — define one main function `<theme>_pred_prep(config)`.
- Register the predictor in `config/pred_data.yaml`.
- Call the new function from `src/ancilliary_data_prep.r` or `src/calibration_predictor_prep.r` as appropriate.

**New pipeline stage:**
- Implementation function: `src/<stage_name>.r`
- CLI driver: `scripts/run_<stage_name>.r` (source `src/setup.r`, `src/utils.r`, `src/<stage_name>.r`; call `get_config()`; call main function)
- SLURM wrapper: `scripts/submit_<stage_name>.sh` (activate correct conda env, call `Rscript scripts/run_<stage_name>.r`)
- Add to `scripts/master_pipeline.sh` with appropriate `--dependency=afterok` chain.

**New output directory:**
- Add the directory key to both `config/local_config.yaml` and `config/hpc_config.yaml` under `output_dirs`.
- Call `ensure_dir(config[["new_dir_key"]])` at the start of the function that writes to it.

**New scenario:**
- Add name to `scenario_names` in both config YAMLs.
- Add SSP mapping to `scenario_to_ssp_mapping`.
- Add scale factor to `simulation_trans_rates_params.scale_factor`.
- Create `config/SSP<n>_interventions.yml` following the existing structure.

**New LULC class:**
- Add entry to `config/lulc_schema.json` with a unique integer `value` (currently 101–107), `class_name`, `clean_name`, `colour`, `nhood_class` flag, and `original_classes` list.

---

## Special Directories

**`src/old/`:**
- Contains deprecated and superseded R scripts from earlier project phases.
- Not sourced by any active `scripts/run_*.r` driver.
- Committed for historical reference only. Do not add new files here; delete or refactor content instead.

**`.planning/`:**
- GSD planning artefacts (codebase maps, phase plans).
- Generated by planning tooling, not part of the scientific pipeline.
- Should be listed in `.gitignore` or committed selectively.

**`dinamica/dinamica_model/`:**
- Version-controlled Dinamica EGO model files.
- Text-diffable `.ego-decoded` format.
- Do not commit runtime log files to this directory (see Architecture > Anti-Patterns).

---

## Gaps / Unknowns

- Several `scripts/submit_*.sh` files referenced in `master_pipeline.sh` (`submit_scenario_preparation.sh`, `submit_simulation_setup.sh`, `submit_model_finalization.sh`) do not exist in the repository — these pipeline stages may be partially implemented.
- There is no `tools/` directory in the repo; `config/local_config.yaml` references several files under `tools/` (`model_lookup.xlsx`, `simulation_lulc_areas_2060.csv`, `calibration_control.csv`, `ctrl_tbl_path`) that must exist on the data filesystem but are not version-controlled.
- No `tests/` directory — the `DESCRIPTION` file lists `testthat` as a suggested dependency but no test files exist.
- `src/landscape_pattern_analysis.r` and `src/parquet_check.r` are present in `src/` but not referenced by any active `scripts/run_*.r` driver — their role in the pipeline is unclear.
