# LULCC Modeling Pipeline - HPC Environment

This document describes the complete LULCC (Land Use Land Cover Change) modeling pipeline that has been updated for HPC (High Performance Computing) environments.

## Overview

The pipeline consists of 7 main stages that process land use data, build statistical models, and run spatially-explicit simulations using Dinamica EGO. All stages are designed to run on SLURM-based HPC systems.

## Pipeline Stages

### 1. Data Preparation (`submit_data_preparation.sh`)
**Runtime:** ~2-4 hours  
**Purpose:** Prepares all input data for modeling
**Steps:**
- LULC data preparation (`lulc_data_prep()`)
- Region preparation (`region_prep()`)
- Ancillary spatial data preparation (`ancillary_data_prep()`)
- Suitability and accessibility predictors (`calibration_predictor_prep()`)
- Parquet files of predictor data (`create_predictor_parquets()`)
- LULC transition identification (`transition_identification()`)
- Transition dataset preparation (`transition_dataset_prep()`)

### 2. Feature Selection (`submit_feature_selection.sh`)
**Runtime:** ~6-12 hours  
**Purpose:** Selects optimal predictor variables using GRRF
**Steps:**
- Collinearity filtering
- Guided Regularized Random Forest (GRRF) feature selection
- Results saved for each transition type

### 3. Transition Modeling (`submit_transition_modeling.sh`)
**Runtime:** ~12-24 hours  
**Purpose:** Builds statistical models for land use transitions
**Steps:**
- Model training using tidymodels framework
- Cross-validation and hyperparameter tuning
- Model performance evaluation

### 4. Model Finalization (`submit_model_finalization.sh`)
**Runtime:** ~2-4 hours  
**Purpose:** Evaluates models and prepares final specifications
**Steps:**
- Model evaluation (`transition_model_evaluation()`)
- Finalize model specifications (`lulcc.finalisemodelspecifications()`)
- Final model training (`trans_model_finalization()`)
- Deterministic transition preparation (`deterministic_trans_prep()`)

### 5. Scenario Preparation (`submit_scenario_preparation.sh`)
**Runtime:** ~4-8 hours  
**Purpose:** Prepares data for scenario simulations
**Steps:**
- Transition rate tables (`simulation_trans_tables_prep()`)
- Predictor data for scenarios (`simulation_predictor_prep()`)

### 6. Simulation Setup (`submit_simulation_setup.sh`)
**Runtime:** ~2-4 hours  
**Purpose:** Calibrates parameters and prepares interventions
**Steps:**
- Allocation parameter calibration (`calibrate_allocation_parameters()`)
- Spatial interventions preparation (`spatial_interventions_prep()`)

### 7. Dinamica Simulations (`submit_dinamica_simulations.sh`)
**Runtime:** ~12-48 hours  
**Purpose:** Runs final land use change simulations
**Steps:**
- Dinamica EGO simulations (`run_evoland_dinamica_sim()`)

## Usage

### Complete Pipeline

To run the entire pipeline from start to finish:

```bash
cd /path/to/nascent-lulcc/scripts
./master_pipeline.sh
```

This will submit all jobs with appropriate dependencies and monitor their progress.

### Partial Pipeline

To run specific stages only:

```bash
cd /path/to/nascent-lulcc/scripts
./partial_pipeline.sh [stage1] [stage2] ... [stageN]
```

Available stages:
- `data-prep`
- `feature-selection`
- `transition-modeling`
- `model-finalization`
- `scenario-prep`
- `simulation-setup`
- `dinamica-sims`

Examples:
```bash
# Run only data preparation and feature selection
./partial_pipeline.sh data-prep feature-selection

# Run final stages only
./partial_pipeline.sh simulation-setup dinamica-sims
```

### Individual Jobs

You can also submit individual jobs manually:

```bash
# Submit data preparation
sbatch submit_data_preparation.sh

# Submit with dependency (wait for job 12345 to complete)
sbatch --dependency=afterok:12345 submit_feature_selection.sh
```

## Environment Requirements

### Conda/Micromamba Environments

The pipeline uses different conda environments for different stages:

1. **feat_select_env** - Used for data preparation, feature selection, scenario prep
   - File: `environments/feat_select_env.yml`
   - Contains: R, arrow, terra, tidyverse, RRF, etc.

2. **transition_model_env** - Used for modeling, finalization, simulation setup
   - File: `environments/transition_model_env.yml`
   - Contains: R, tidymodels, ranger, xgboost, glmnet, etc.

### File Structure Requirements

The pipeline assumes the following directory structure:

```
project_root/
├── src/                    # R source files
│   ├── setup.r
│   ├── utils.r
│   ├── lulc_data_prep.r
│   └── ...
├── scripts/                # HPC job scripts
│   ├── master_pipeline.sh
│   ├── partial_pipeline.sh
│   ├── submit_*.sh
│   └── run_*.r
├── config/                 # Configuration files
│   ├── model_specs.yaml
│   ├── pred_data.yaml
│   └── ancillary_data.yaml
├── environments/           # Conda environment specs
└── logs/                   # Job logs (created automatically)
```

## Key Changes from Original QMD

### What Was Added:
- Complete data preparation pipeline
- Model evaluation and finalization steps  
- Scenario preparation workflow
- Calibration and spatial interventions
- Dinamica simulation orchestration
- Dependency management between stages
- Comprehensive logging and monitoring

### What Was Excluded:
- `fetch_zenodo_predictors()` calls (assumed data already available)
- Package installation steps (handled by conda environments)
- `devtools::load_all()` calls (using source() instead)

### Path Fixes:
- All R scripts now source from `../src/` relative paths
- Working directory management for HPC environment
- Proper handling of command-line execution vs interactive sessions

## Monitoring and Logs

### Job Status
Monitor jobs using standard SLURM commands:
```bash
squeue -u $USER                    # Check job queue
sacct -j JOB_ID                   # Check job details
scancel JOB_ID                    # Cancel a job
```

### Log Files
Each stage produces detailed logs:
- `logs/data-prep-JOBID.{out,err}`
- `logs/feat-select-JOBID.{out,err}`  
- `logs/trans-model-JOBID.{out,err}`
- `logs/model-final-JOBID.{out,err}`
- `logs/scenario-prep-JOBID.{out,err}`
- `logs/sim-setup-JOBID.{out,err}`
- `logs/dinamica-sim-JOBID.{out,err}`

### Pipeline Summaries
The master pipeline creates summary files:
- `logs/complete_pipeline_summary_TIMESTAMP.txt`
- `logs/partial_pipeline_summary_TIMESTAMP.txt`
- Individual stage summaries in respective logs directories

## Troubleshooting

### Common Issues:

1. **Environment not found**: Check that conda environments are created and paths are correct in submit scripts

2. **Source file errors**: Verify that all required R files exist in `src/` directory

3. **Memory/time limits**: Adjust SLURM parameters in submit scripts based on your data size

4. **Path issues**: Ensure all file paths in configuration files are correct for your HPC system

5. **Dependencies**: Make sure each stage completes successfully before the next stage starts

### Resource Requirements:

Adjust these based on your dataset size and HPC system:
- **Memory**: 16-32GB per CPU for most stages  
- **CPUs**: 4-8 cores recommended
- **Time**: See runtime estimates above
- **Storage**: Ensure adequate scratch space for intermediate files

## Configuration

The pipeline reads configuration from YAML files in the `config/` directory. Key settings include:
- Data paths and directories
- Modeling parameters  
- Scenario specifications
- Output locations

Make sure to update these files to match your HPC environment and data locations.