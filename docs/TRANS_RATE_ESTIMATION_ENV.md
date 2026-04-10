# Simulation Transition Rates Estimation Environment Setup

## Overview

The transition rate estimation pipeline requires a specialized conda environment with convex optimization libraries (CVXR) and supporting packages.

## Environment File

**Location:** `environments/trans_rate_estimation_env.yml`

**Name:** `trans_rate_estimation_env`

## Key Dependencies

### Core Optimization
- **r-cvxr**: Convex optimization package for solving transition rate problems
- **r-matrix**: Sparse and dense matrix support
- **r-slam**: Sparse lightweight arrays and matrices

### Data Processing
- **r-dplyr, r-tidyr, r-purrr**: Data manipulation
- **r-readxl, r-readr**: Excel and CSV I/O
- **r-arrow**: Parquet file support (if needed)
- **r-yaml, r-jsonlite**: Configuration file handling

### Parallel Processing
- **r-future, r-future.apply, r-furrr**: Parallel execution framework

### Visualization
- **r-ggplot2, r-scales**: Plotting and color palettes

## Installation Instructions

### Local Environment

```bash
# Navigate to project root
cd /path/to/nascent-lulcc

# Create environment
mamba env create -f environments/trans_rate_estimation_env.yml

# Activate environment
mamba activate trans_rate_estimation_env

# Verify CVXR installation
Rscript -e "library(CVXR); print(installed_solvers())"
```

### HPC (Euler) Environment

```bash
# Load micromamba module
module load stack/2024-06 micromamba/1.5.10

# Set environment base path
ENV_BASE=/cluster/project/climate/bblack/envs

# Create environment
micromamba create -f environments/trans_rate_estimation_env.yml -p $ENV_BASE/trans_rate_estimation_env

# Test the environment
micromamba run -p $ENV_BASE/trans_rate_estimation_env Rscript -e "library(CVXR)"
```

## CVXR Solvers

CVXR comes with several solvers. The default open-source solvers include:
- **ECOS**: For conic optimization problems (default for most problems)
- **SCS**: Splitting conic solver (alternative)
- **OSQP**: Operator splitting quadratic program solver

### Checking Available Solvers

```r
library(CVXR)
installed_solvers()
```

### Optional Commercial Solvers

For larger problems, commercial solvers can be more efficient:

**CPLEX (IBM):**
```bash
# If you have CPLEX installed and licensed
mamba install -c conda-forge r-rcplex
```

**Gurobi:**
```bash
# If you have Gurobi installed and licensed  
mamba install -c gurobi r-gurobi
```

## Configuration Parameters

The optimization parameters are defined in `config/local_config.yaml` and `config/hpc_config.yaml` under the `simulation_trans_rates` section:

```yaml
simulation_trans_rates:
  margin: 0.50              # Soft bounds margin
  stay_min: 0.10            # Minimum stay rate
  stay_max: 0.999           # Maximum stay rate
  eps_ridge: 1.0e-8         # Ridge regularization epsilon
  solver_eps: 1.0e-5        # Solver tolerance
  monotone_tol: 1.0e-3      # Monotonicity tolerance
  lambda: 0.001             # Regularization weight
  eta_pref: 0.001           # Preference weight
  rho: 100                  # Penalty weight
  mu: 5                     # Smoothness weight
  fair_weight: 5            # Fairness weight
  kappa_zero: 0.1           # Zero forcing parameter
  zero_hist_thresh: 1.0e-5  # Zero history threshold
  beta_dev: 1.0              # Deviation parameter
  rel_guard: 0.05           # Relative guard
  forbid_inflow: []         # Forbidden transitions
  num_workers: 6            # Parallel workers
```

## Running the Pipeline

### Locally

```bash
# Activate environment
mamba activate trans_rate_estimation_env

# Run script
Rscript scripts/run_simulation_trans_rates_prep.r
```

### On HPC

```bash
# Submit job
sbatch scripts/submit_simulation_trans_rates_estimation.sh

# Check job status
squeue -u $USER

# Check output
tail -f logs/sim-trans-rates-prep-<jobid>.out
```

## Troubleshooting

### CVXR Installation Issues

If CVXR fails to install:

```bash
# Try installing system dependencies first
mamba install -c conda-forge libgfortran5 openblas

# Then reinstall CVXR
mamba install -c conda-forge r-cvxr
```

### Solver Issues

If you get solver errors:

```r
# Check which solvers are available
library(CVXR)
installed_solvers()

# Try specifying a different solver
# In the optimization code, add: solver = "SCS"
```

### Memory Issues

For large problems, you may need:

1. **Increase memory allocation:**
   - Edit `submit_simulation_trans_rates_estimation.sh`
   - Increase `--mem-per-cpu` value

2. **Reduce parallel workers:**
   - Edit config file
   - Reduce `num_workers` value

3. **Process fewer scalars:**
   - Modify the scalars vector in the source code
   - Process in batches

## Expected Input Files

The pipeline expects these files in the `outputs_dir`:

- `LULC_initial_amount.xlsx` - Initial LULC class areas
- `LULC_demand_results.xlsx` - Expert demand/preference data
- `LULC_transition_rates_Singlestep.xlsx` - Historical transition rates (in `trans_rates_raw_dir`)

## Output Files

Results are saved to `trans_rate_table_dir/Output_Scalar_X.Xx/`:

- `areas_long_scalar_X.X.csv` - Time series of LULC areas (long format)
- `areas_wide_scalar_X.X.csv` - Time series of LULC areas (wide format)
- Individual plots per region-scenario combination

## Performance Considerations

### Optimization Problem Size

The problem size scales with:
- Number of LULC classes: L
- Number of time steps: T
- Variables per time step: L × L (transition matrix)
- Total variables: L × L × T

For L=6 and T=10: ~360 decision variables

### Solver Selection

- **ECOS**: Good for most problems, generally faster
- **SCS**: More robust for ill-conditioned problems
- **OSQP**: Better for quadratic programs

### Parallelization

The pipeline parallelizes over region-scenario combinations. Each worker:
- Loads data once
- Solves optimization problem
- Saves results independently

Optimal worker count depends on:
- Available cores
- Memory per core
- Problem size

## References

- CVXR Documentation: https://cvxr.rbind.io/
- CVXR Paper: Fu, A., Narasimhan, B., & Boyd, S. (2020). CVXR: An R Package for Disciplined Convex Optimization. Journal of Statistical Software, 94(14).

## Contact

For issues with this environment or pipeline, contact the development team.
