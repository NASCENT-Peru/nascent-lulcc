# HPC Pipeline Setup for LULCC modelling

This directory contains scripts and environments for running the Land Use Land Cover Change (LULCC) modelling pipeline on HPC systems using Slurm.

## Stage 7 Dinamica-on-Euler contract (INFRA-01)

Stage 7 (allocation) calls Dinamica EGO 8 through a single R entrypoint
(`exec_dinamica()` in `src/dinamica_utils.r`). On Euler the Dinamica binary is
**not** installed natively; instead, an external Apptainer/Singularity image
is consumed verbatim by `apptainer exec` / `singularity exec`. The repository
ships only the container definition (`dinamica/container/rocker-geospatial-dinamica.def`)
and the build instructions (`dinamica/container/README.md`); the built `.sif`
artifact stays outside the repo (locked decision D-10).

### `DINAMICA_EGO_8_HOME` contract

| Backend | What `DINAMICA_EGO_8_HOME` must be                                   |
| ------- | -------------------------------------------------------------------- |
| HPC     | **Absolute path to the external Dinamica `.sif` image** on Euler.    |
| Local   | Absolute path to the local Dinamica EGO 8 install directory.         |

On Euler, set it to wherever the built `.sif` lives, e.g.:

```bash
export DINAMICA_EGO_8_HOME=/cluster/project/<project>/containers/dinamica-ego-8.sif
```

`exec_dinamica()` consumes that exact path as the image argument to
`apptainer exec "$DINAMICA_EGO_8_HOME" DinamicaConsole <model>`, with
`singularity exec` as the only fallback runtime spelling. The runtime is
probed in the order `apptainer` first, `singularity` second.

### Required env vars (Stage 7)

In addition to the path contract from Plan 01-01 (`HPC_SCRATCH_ROOT`,
`HPC_TMP_ROOT`, `TERRA_TEMP`, validated by
`scripts/hpc_common.sh --check-stage7-contract`), Stage 7 requires:

| Variable               | Required on HPC | Required locally | Notes                                   |
| ---------------------- | --------------- | ---------------- | --------------------------------------- |
| `DINAMICA_EGO_8_HOME`  | yes             | yes              | `.sif` path on HPC, install dir locally |
| `DINAMICA_BACKEND`     | optional        | optional         | `auto` (default), `local`, or `hpc`     |

`.env.template` exposes both variables with the right defaults.

### Container definition + build instructions

The committed container definition lives at:

- `dinamica/container/rocker-geospatial-dinamica.def` — Apptainer/Singularity
  build definition rooted in the upstream
  [`ethzplus/rocker-geospatial-dinamica`](https://github.com/ethzplus/rocker-geospatial-dinamica)
  image.
- `dinamica/container/README.md` — operator-facing build flow and where to
  publish the built `.sif` as an external artifact (D-10).

To (re)build the image on a node that supports container builds:

```bash
# Preferred:
apptainer build dinamica-ego-8.sif \
    dinamica/container/rocker-geospatial-dinamica.def

# Fallback spelling:
singularity build dinamica-ego-8.sif \
    dinamica/container/rocker-geospatial-dinamica.def
```

Then publish the resulting `.sif` to a stable path **outside** the repo (the
external artifact pattern, D-10) and export `DINAMICA_EGO_8_HOME` to that path.

### Smoke-test commands (D-11)

`scripts/smoke_test_dinamica.sh` is the operator-facing wrapper that proves
the Dinamica wiring **before** any real Stage 7 batch job. The same script
covers two modes: a workstation dry-run (no runtime probe, no real `.sif`
needed) and the live Euler smoke test that requires Dinamica to actually
complete and write a timestamped `dinamica-smoke-*.log` under `logs/`.

#### Dry-run command (workstation, no apptainer/singularity required)

```bash
bash scripts/smoke_test_dinamica.sh \
    --dry-run \
    --runtime apptainer \
    --artifact /tmp/dinamica.sif \
    --ego dinamica/dinamica_model/allocation.ego-decoded
```

This prints the resolved launch plan (runtime, `DINAMICA_EGO_8_HOME` artifact
path, ego model, log file destination) and exits 0 without spawning Dinamica.
The dry-run uses the operator-supplied runtime name verbatim so the contract
can be validated on a host that has neither `apptainer` nor `singularity`
installed.

#### Live Euler smoke-test command

```bash
# Source the .env file first so DINAMICA_EGO_8_HOME and the Stage 7 path
# contract are populated.
source .env

bash scripts/smoke_test_dinamica.sh \
    --live \
    --runtime auto \
    --artifact "$DINAMICA_EGO_8_HOME" \
    --ego dinamica/dinamica_model/allocation.ego-decoded \
    --require-log-under logs
```

The live mode:

1. Probes `apptainer` first, then `singularity`, on `PATH`.
2. Checks that `$DINAMICA_EGO_8_HOME` actually points to a readable `.sif`.
3. Runs `apptainer exec "$DINAMICA_EGO_8_HOME" DinamicaConsole <ego>` (or the
   `singularity exec` equivalent) and tees combined stdout/stderr into
   `logs/dinamica-smoke-<timestamp>.log`.
4. Exits **non-zero** unless Dinamica completes successfully **and** the
   timestamped logfile lands under `logs/`.

The exit-code contract is documented at the top of the script.

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