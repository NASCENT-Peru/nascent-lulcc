# Simulation Transition Rates Scripts Refactoring Summary

## Date
February 17, 2026

## Overview
This document summarizes the refactoring of the simulation transition rates preparation scripts to add standardized config loading, message logging, and package management patterns while **preserving the original optimization logic** from `simulation_transition_rates_estimation.R`.

## Key Principle

**The original script logic is preserved**. Unlike `calibrate_allocation_parameters.r` which has a deeply nested structure for region/transition processing, this script follows its original design with scalar iterations and convex optimization. The refactoring adds **only**:
- Standardized config parameter loading
- Consistent message logging
- Proper package management  
- Error handling patterns

## Changes Made

### 1. Configuration Files Updated

#### `config/local_config.yaml` and `config/hpc_config.yaml`

Added new section `simulation_trans_rates` containing all optimization parameters that were previously hardcoded in the script:

```yaml
simulation_trans_rates:
  margin: 0.50
  stay_min: 0.10
  stay_max: 0.999
  eps_ridge: 1.0e-8
  solver_eps: 1.0e-5
  monotone_tol: 1.0e-3
  lambda: 0.001
  eta_pref: 0.001
  rho: 100
  mu: 5
  fair_weight: 5
  kappa_zero: 0.1
  zero_hist_thresh: 1.0e-5
  beta_dev: 1.0
  rel_guard: 0.05
  forbid_inflow: []
  num_workers: 6
```

**Benefits:**
- All tunable parameters now in config files
- Easy to adjust between local and HPC environments
- No need to edit source code to change optimization settings

### 2. New Source File Created

#### `src/simulation_trans_rates_prep.r`

Created a clean implementation with the original script's logic structure:

**Structure (mirrors original script):**
1. Load configuration from config files (**NEW**)
2. Load input data (Excel files) with proper path handling (**IMPROVED**)
3. Prepare data structures (original logic)
4. Setup parallel processing (original logic + config-based workers)
5. Define helper functions (original logic)
6. Setup scalar iterations (original logic)
7. Execute optimization loop (**PLACEHOLDER - needs core logic integration**)
8. Cleanup (**NEW** - proper shutdown)

**Key Features Added:**
- Reads all parameters from `config$simulation_trans_rates`
- Clear progress messages with section numbers [1/7], [2/7], etc.
- File existence checks with informative error messages
- Proper directory management using `ensure_dir()`
- Standardized logging format
- Config-based output paths

**Original Logic Preserved:**
- Helper functions (`recode_lulc_to_english`, `clean_numeric`, `pick_shape`)
- Data loading from specific Excel files
- Scalar iteration structure (1.0, 3.0, 5.0, 7.0, 9.0)
- Task grid for region-scenario combinations
- Color palette setup for consistent plotting
- Time step definitions

**Implementation Status:**
The structure is complete with config loading and messaging. The core optimization loop from lines 220-776 of the original script needs to be integrated into section G.

### 3. Run Script Updated

#### `scripts/run_simulation_trans_rates_prep.r`

Completely refactored to match pattern from other pipeline scripts:

**Changes:**
- Consistent package loading with `install_if_missing()` helper
- Multiple path fallback for sourcing files (handles SLURM and local environments)
- Proper diagnostics output (R version, library paths)
- Configuration validation before execution
- Standardized error reporting with traceback
- Summary file generation in appropriate directory
- Exit code handling (0 for success, 1 for error)

**Package Dependencies Added:**
- `CVXR` - convex optimization (core requirement)
- `future`, `future.apply` - parallel processing
- `readxl`, `readr` - Excel and CSV I/O
- `ggplot2`, `scales` - visualization support

**Messaging Pattern:**
- Section headers with `========`
- Progress indicators with `✓` and `✗`
- Elapsed time tracking in hours
- Job ID from SLURM environment

### 4. Submit Script Updated

#### `scripts/submit_simulation_trans_rates_estimation.sh`

Aligned with other HPC submit scripts:

**Changes:**
- Updated job name: `sim-trans-rates-prep`
- Increased CPUs to 6 (from 4) to match config  
- Consistent header formatting and section comments
- Proper use of `$SCRIPT_DIR` variable
- Updated R script path to `run_simulation_trans_rates_prep.r`
- Clear error messaging

## What's Different from calibrate_allocation_parameters.r

The `calibrate_allocation_parameters.r` script has a specific structure:
- Deeply nested: periods → regions → transitions
- Heavy use of raster processing
- Sequential then parallel processing pattern
- Creates intermediate cache files
- Region-masking and trimming operations

The `simulation_trans_rates_prep.r` script has a different structure:
- Scalar iterations at top level
- Convex optimization with CVXR
- Task grid for region-scenario combinations
- Parallel processing via `future_lapply`
- Focus on transition rate tables, not rasters

**We preserve this difference**. Each script's structure fits its problem domain.

## Migration from Old Script

The old `simulation_transition_rates_estimation.R` had:
- Hardcoded working directory with `setwd()`
- All parameters defined at script level
- Direct library loading without error handling
- Minimal progress messaging
- File paths hardcoded for Peru case study

The new implementation:
- Reads all settings from config files
- Uses workspace-relative paths
- Robust package loading with automatic installation
- Comprehensive progress logging
- Maintains Peru case study file structure but with proper path handling

## Next Steps to Complete

### 1. Integrate Core Optimization Logic

The main missing piece is integrating lines 220-776 from `simulation_transition_rates_estimation.R` into section G of the new script:

```r
# From original script (lines 220-776):
# - Outer loop over scalars
# - Recalculate targets based on scaled sliders
# - Recalculate shapes using pick_shape()
# - Inner parallel loop via future_lapply()
#   - Build constraint matrices
#   - Setup CVXR variables and constraints
#   - Solve optimization problem
#   - Extract and save results
# - Export CSV files
# - Generate individual plots per region-scenario
```

This logic should be inserted into the marked section with minimal modifications—just using the config-loaded parameters instead of hardcoded ones.

###
