# HPC Pipeline Setup for LULCC modelling

This directory contains scripts and environments for running the Land Use Land Cover Change (LULCC) modelling pipeline on HPC systems using Slurm.

## Files Overview

### Environment Files (`../envs/`)
- `feat_select_env.yaml` - Environment for feature selection (includes RRF, arrow, etc.)
- `transition_model_env.yml` - Environment for transition modelling (includes tidymodels, ranger, xgboost, etc.)
- `dist_calc_env.yml` - Environment for distance calculations (if needed)
- `clim_data_env.yml` - Environment for climate data processing (if needed)

### Pipeline Scripts
- `setup_environments.sh` - Creates all conda environments
- `submit_feature_selection.sh` - Slurm job for feature selection
- `submit_transition_modelling.sh` - Slurm job for transition modelling  
- `master_pipeline.sh` - Runs the complete pipeline sequentially
- `run_feature_selection.r` - R script for feature selection pipeline
- `run_transition_modelling.r` - R script for transition modelling pipeline

## Usage

### 1. First Time Setup

Create the conda environments:
```bash
cd inst/
./setup_environments.sh
```

This will create environments at `/cluster/scratch/bblack/micromamba/envs/` (adjust paths as needed).

### 2. Running the Complete Pipeline

To run both feature selection and transition modelling sequentially:
```bash
cd inst/
./master_pipeline.sh
```

This will:
- Submit feature selection job
- Wait for it to complete
- Submit transition modelling job (depends on feature selection)
- Wait for it to complete
- Generate a summary report

### 3. Running Individual Steps

#### Feature Selection Only
```bash
cd inst/
sbatch submit_feature_selection.sh
```

#### Transition modelling Only
```bash
cd inst/
sbatch submit_transition_modelling.sh
```

### 4. Monitoring Jobs

Check job status:
```bash
squeue -u $USER
```

Check job details:
```bash
scontrol show job JOBID
```

View logs:
```bash
tail -f logs/feat-select-JOBID.out
tail -f logs/trans-model-JOBID.out
```

## Resource Allocation

### Feature Selection
- **CPUs**: 4 cores
- **Memory**: 32GB per CPU (128GB total)
- **Time**: 72 hours
- **Environment**: `feat_select_env`

### Transition modelling  
- **CPUs**: 8 cores
- **Memory**: 16GB per CPU (128GB total)
- **Time**: 72 hours
- **Environment**: `transition_model_env`

## Customization

### Adjusting Resource Requirements

Edit the `#SBATCH` directives in the submission scripts:
- `--cpus-per-task`: Number of CPU cores
- `--mem-per-cpu`: Memory per CPU core
- `--time`: Maximum runtime (HH:MM:SS)

### Adjusting Environment Paths

Update these variables in the scripts:
- `MAMBA_EXE`: Path to micromamba executable
- `ENV_PATH`: Base path for conda environments

### Adding Dependencies

Add packages to the appropriate `.yml` file in `envs/` directory, then recreate the environment:
```bash
micromamba env remove -p /path/to/env
micromamba env create -f envs/environment_file.yml -p /path/to/env
```

## Troubleshooting

### Common Issues

1. **Environment not found**: Ensure `setup_environments.sh` completed successfully
2. **Package missing**: Check if all required packages are in the environment files
3. **Memory issues**: Increase `--mem-per-cpu` or reduce data batch sizes
4. **Time limit exceeded**: Increase `--time` or optimize processing

### Debug Information

The pipeline scripts include extensive logging and diagnostics:
- R version and library paths
- Package installation status
- Configuration loading
- Runtime statistics

### Log Files

All output is captured in timestamped log files:
- `logs/feat-select-JOBID.{out,err}`
- `logs/trans-model-JOBID.{out,err}`
- `logs/pipeline_summary_TIMESTAMP.txt`

## Notes

- The scripts assume a specific HPC setup with micromamba. Adjust paths as needed for your system.
- The transition modelling job depends on feature selection completing successfully.
- All intermediate results are saved to disk to allow for resuming if needed.
- The pipeline uses parallel processing within jobs (controlled by `--cpus-per-task`).