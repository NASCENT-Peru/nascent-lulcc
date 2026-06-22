<!-- generated-by: gsd-doc-writer -->
# Getting Started

This guide covers everything needed to set up and run nascent-lulcc for the first time, whether
on the ETH Euler HPC cluster (the primary target) or a local Windows workstation for development.

---

## Prerequisites

### Runtime

| Requirement | Version | Notes |
|---|---|---|
| **R** | >= 4.2 (4.3.x recommended) | Pinned to `4.3` in each conda environment YAML |
| **micromamba** | latest | Used to create and activate per-stage conda environments |
| **Dinamica EGO 8** | 8.x | Required only for Stage 7 (allocation); proprietary binary |
| **GDAL** | >= 3.0 | Loaded via conda environments; no separate system install needed |
| **PROJ** | >= 9 | Included in conda environments |
| **GEOS** | any recent | Included in conda environments |

### HPC-specific

| Requirement | Notes |
|---|---|
| **SLURM** | ETH Euler cluster; stages can be run manually for local development |
| **Apptainer / Singularity** | Required for running Dinamica EGO 8 on HPC via the `.sif` container |
| **Access to scratch filesystem** | `/beegfs/$USER` — conda environments and data live here |

### Local development

Local development uses a Windows workstation with R >= 4.2 installed natively. Conda environments
are created under the project root (`.envs/`) when no HPC signals are detected. Dinamica EGO 8
must be installed locally and `DINAMICA_EGO_8_HOME` must point to the install directory.

---

## Installation Steps

### HPC Setup

#### 1. Upload the repository

Transfer the project to your Euler home directory:

```bash
# Target path on ETH Euler
/home/$USER/nascent-lulcc
```

#### 2. Configure environment variables

Copy `.env.template` to `.env` and set the machine-specific values:

```bash
cd /home/$USER/nascent-lulcc
cp .env.template .env
```

Open `.env` and fill in the required values:

| Variable | Required | Description |
|---|---|---|
| `HPC_SCRATCH_ROOT` | Required on HPC | Scratch filesystem root, e.g. `/beegfs/$USER/nascent-lulcc` |
| `HPC_TMP_ROOT` | Required on HPC | Per-job temp root, e.g. `$HPC_SCRATCH_ROOT/temp` |
| `TERRA_TEMP` | Required on HPC | terra temp directory, e.g. `$HPC_SCRATCH_ROOT/terra_temp` |
| `DINAMICA_EGO_8_HOME` | Required (both) | HPC: absolute path to the built `.sif` image; local: Dinamica install directory |
| `DINAMICA_BACKEND` | Optional | `auto` (default), `local`, or `hpc` |

Then source the file to export the variables and create scratch directories:

```bash
source .env
```

#### 3. Install micromamba

```bash
bash scripts/install_micromamba_simple.sh
```

This installs the micromamba binary to `$HOME/.local/bin/micromamba`. If the simple installer fails,
try the standard installer or install manually:

```bash
# Standard installer
bash scripts/install_micromamba.sh

# Manual fallback
mkdir -p $HOME/.local/bin
curl -L https://github.com/mamba-org/micromamba-releases/releases/latest/download/micromamba-linux-64 \
    -o $HOME/.local/bin/micromamba
chmod +x $HOME/.local/bin/micromamba
```

Optionally add micromamba to your shell:

```bash
echo 'export MAMBA_EXE="$HOME/.local/bin/micromamba"' >> ~/.bashrc
echo 'export MAMBA_ROOT_PREFIX="$HOME/.micromamba"' >> ~/.bashrc
echo 'eval "$($MAMBA_EXE shell hook -s bash)"' >> ~/.bashrc
source ~/.bashrc
```

#### 4. Create conda environments

```bash
bash scripts/setup_environments.sh
```

This reads all `.yml` files under `environments/` and creates one conda environment per pipeline
stage under `/beegfs/$USER/micromamba/envs/`. If `HPC_SCRATCH_ROOT` is unset when HPC
context is detected, the script exits with a named-signal error rather than silently installing
under `$HOME`.

The environments created are:

| Environment | Pipeline stage |
|---|---|
| `data_prep_env` | Stage 1 — Data preparation |
| `feat_select_env` | Stage 2 — Feature selection |
| `transition_model_env` | Stage 3 — Transition modelling |
| `allocation_params_env` | Stage 4 — Allocation parameter calibration |
| `trans_rate_estimation_env` | Stage 5 — Scenario transition rate preparation |
| `allocation_env` | Stage 7 — Allocation and Dinamica simulations |
| `dist_calc_env` | Distance/accessibility predictor calculation |
| `clim_data_env` | Climate data download (Python-based) |

To provision a single environment only (useful after a scratch wipe):

```bash
bash scripts/setup_environments.sh --env allocation_env --non-interactive
```

#### 5. Build or obtain the Dinamica EGO 8 container

On Euler, Dinamica EGO 8 runs via an Apptainer/Singularity container. The repository ships
the container definition at `dinamica/container/rocker-geospatial-dinamica.def` but not the
built `.sif` artifact (it is ~1 GB and lives outside the repo).

Build the container directly to the path you will set as `DINAMICA_EGO_8_HOME`:

```bash
# Route build cache to scratch to avoid home quota exhaustion
export APPTAINER_TMPDIR="$HPC_SCRATCH_ROOT/apptainer-tmp"
export APPTAINER_CACHEDIR="$HPC_SCRATCH_ROOT/apptainer-cache"
mkdir -p "$APPTAINER_TMPDIR" "$APPTAINER_CACHEDIR"

# Build directly to the external artifact path
apptainer build "$DINAMICA_EGO_8_HOME" \
    dinamica/container/rocker-geospatial-dinamica.def

# Fallback spelling if apptainer is not available
singularity build "$DINAMICA_EGO_8_HOME" \
    dinamica/container/rocker-geospatial-dinamica.def
```

See `dinamica/container/README.md` for the full build flow and publication instructions.

---

### Local Development Setup

For local development on a Windows workstation:

1. Install R >= 4.2 (4.3.x recommended).
2. Install micromamba for Windows.
3. Clone the repository and `cd` into it.
4. Copy `.env.template` to `.env` and set `DINAMICA_EGO_8_HOME` to the local Dinamica EGO 8
   install directory. Leave `HPC_SCRATCH_ROOT`, `HPC_TMP_ROOT`, and `TERRA_TEMP` unset (they
   fall back to local defaults automatically).
5. Run `bash scripts/setup_environments.sh` — with no HPC signals detected, environments are
   created under `<repo>/.envs/`.
6. Place input data at `E:/nascent-lulcc-agg/` (the `data_basepath` in `config/local_config.yaml`)
   or update `local_config.yaml` to point to your data location.

---

## First Run

### Verify the Dinamica wiring (HPC)

Before submitting any Stage 7 batch job, run the Dinamica smoke test:

```bash
source .env

bash scripts/smoke_test_dinamica.sh \
    --live \
    --runtime auto \
    --artifact "$DINAMICA_EGO_8_HOME" \
    --ego dinamica/dinamica_model/smoketest.ego \
    --require-log-under logs
```

A successful run exits 0 and writes a timestamped log to `logs/dinamica-smoke-<timestamp>.log`.
For a dry-run that validates the launch plan without requiring `apptainer`:

```bash
bash scripts/smoke_test_dinamica.sh \
    --dry-run \
    --runtime apptainer \
    --artifact /tmp/dinamica.sif \
    --ego dinamica/dinamica_model/smoketest.ego
```

### Run the full pipeline (HPC)

Submit all 7 stages as a dependency-chained SLURM sequence:

```bash
cd /home/$USER/nascent-lulcc/scripts
bash master_pipeline.sh
```

### Submit an individual stage (HPC)

Each stage has a paired SLURM submit script:

```bash
# Stage 1 — data preparation
sbatch scripts/submit_lulc_data_prep.sh

# Stage 3 — transition modelling (depends on stage 2 completing)
sbatch --dependency=afterok:JOBID scripts/submit_transition_modelling.sh

# Stage 7 — Dinamica simulations
sbatch scripts/submit_allocation.sh
```

### Run interactively (local)

Source functions directly in R. The config auto-detects the local environment:

```r
source("src/setup.r")
source("src/utils.r")
source("src/lulc_data_prep.r")

config <- get_config()   # auto-selects config/local_config.yaml
lulc_data_prep(config)
```

All major pipeline functions accept `config = get_config()` as a default argument and support
a `refresh_cache` flag to skip recomputation of existing outputs.

---

## Common Setup Issues

**`HPC_SCRATCH_ROOT` not set on HPC**

`setup_environments.sh` detects HPC context (via SLURM env vars or `/beegfs` presence)
and refuses to install environments under `$HOME` when `HPC_SCRATCH_ROOT` is unset. Source `.env`
first:

```bash
source .env
bash scripts/setup_environments.sh
```

**Micromamba or environment not found**

Conda environments live on scratch and are wiped periodically (every 60–90 days on Euler).
Recreate them:

```bash
bash scripts/setup_environments.sh
```

Check the current environment list:

```bash
$HOME/.local/bin/micromamba env list
```

**Dinamica EGO not found / `std::exception` errors**

Ensure `DINAMICA_EGO_8_HOME` is set to the absolute path of the `.sif` file (not a directory)
and that `DINAMICA_BACKEND` is `auto` or `hpc`. The smoke test validates the full wiring before
a real run. If you see `std::exception` in a Dinamica log, check that the container was built
from the current `dinamica/container/rocker-geospatial-dinamica.def` definition — older images
had a circular singleton initialisation bug that is fixed in the current definition.

**Stage 7 pre-flight exits 1**

The `check-stage7-contract` validation requires `HPC_SCRATCH_ROOT`, `HPC_TMP_ROOT`, and
`TERRA_TEMP` to be set and non-empty. Validate manually:

```bash
bash scripts/hpc_common.sh --check-stage7-contract
```

**Memory or time limit exceeded**

Adjust `#SBATCH --mem-per-cpu` and `#SBATCH --time` in the relevant `submit_*.sh` script.
Typical allocations: 16–32 GB/CPU, 4–8 cores, up to 48 h wall time. For Stage 3 (transition
modelling) the default is 3 CPUs at 42 GB/CPU; for Stage 7 (allocation) it is 8 CPUs at 8 GB/CPU.

**Missing scratch directories**

Re-source `.env` (which runs `mkdir -p` for required directories), or create them manually:

```bash
mkdir -p "$HPC_SCRATCH_ROOT"/{data,results,logs,terra_temp,temp}
```

**Home directory quota exhausted during container build**

Always build the Dinamica `.sif` directly to the `$DINAMICA_EGO_8_HOME` path on a project or
scratch filesystem. A relative path resolves under `$HOME` and will exhaust Euler's home quota
at the final `Creating SIF file` step. Route intermediate build cache to scratch:

```bash
export APPTAINER_TMPDIR="$HPC_SCRATCH_ROOT/apptainer-tmp"
export APPTAINER_CACHEDIR="$HPC_SCRATCH_ROOT/apptainer-cache"
```

---

## Next Steps

- **Pipeline stages and architecture**: see `docs/ARCHITECTURE.md`
- **Configuration reference**: see `docs/CONFIGURATION.md`
- **Micromamba setup and troubleshooting**: see `docs/MICROMAMBA_SETUP.md`
- **HPC contract details**: see `docs/README_HPC.md`
- **Monitoring jobs**: `squeue -u $USER` / `sacct -j JOB_ID`; logs are written to `logs/<stage>-<JOBID>.{out,err}`
