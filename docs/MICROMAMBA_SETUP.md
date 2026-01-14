# Micromamba Setup and Migration Guide

## Overview

This guide helps you set up micromamba in your HPC workspace and migrate existing scripts to use the new location.

## Problem

The previous micromamba location is no longer available:
- **Old location**: `/cluster/project/eawag/p01002/.local/bin/micromamba` (deleted)
- **Solution**: Install micromamba in your home directory

## Storage Locations

### Home Directory (`/cluster/home/bblack`)
- **Size**: Limited (a few GB)
- **Persistence**: Permanent
- **Use for**: Micromamba executable (~20 MB), scripts, small configs

### Scratch Directory (`/cluster/scratch/bblack`)
- **Size**: Large (TBs)
- **Persistence**: Wiped regularly (every 60-90 days)
- **Use for**: Conda environments, temporary files, large datasets

## Installation Steps

### 1. Install Micromamba

**Option A: Simple Installation (Recommended)**

```bash
cd /cluster/home/bblack/nascent-lulcc
bash scripts/install_micromamba_simple.sh
```

This uses a direct binary download which is more reliable.

**Option B: Standard Installation**

```bash
cd /cluster/home/bblack/nascent-lulcc
bash scripts/install_micromamba.sh
```

**Troubleshooting Installation**

If installation fails, run the diagnostic:

```bash
bash scripts/troubleshoot_micromamba.sh
```

This will check:
- Directory permissions
- File existence and executable status
- Environment variables
- Common issues

**Manual Installation (if scripts fail)**

```bash
# Create directory
mkdir -p $HOME/.local/bin
cd $HOME/.local/bin

# Download micromamba binary directly
curl -L https://github.com/mamba-org/micromamba-releases/releases/latest/download/micromamba-linux-64 -o micromamba

# Make executable
chmod +x micromamba

# Test it
./micromamba --version
```

Expected output:
```
✓ Micromamba installed successfully to: /cluster/home/bblack/.local/bin/micromamba
Version: 1.x.x
```

### 2. Configure Your Shell (Optional but Recommended)

Add micromamba to your `~/.bashrc`:

```bash
echo 'export MAMBA_EXE="$HOME/.local/bin/micromamba"' >> ~/.bashrc
echo 'export MAMBA_ROOT_PREFIX="$HOME/.micromamba"' >> ~/.bashrc
echo 'eval "$($MAMBA_EXE shell hook -s bash)"' >> ~/.bashrc
source ~/.bashrc
```

### 3. Create Conda Environments

Create all required environments:

```bash
cd /cluster/home/bblack/nascent-lulcc
bash scripts/setup_environments.sh
```

This will create environments in `/cluster/scratch/bblack/micromamba/envs/`:
- `feat_select_env`
- `transition_model_env`
- `allocation_params_env`
- `dist_calc_env` (if applicable)
- `clim_data_env` (if applicable)

### 4. Verify Installation

Check that environments were created:

```bash
$HOME/.local/bin/micromamba env list
```

Expected output:
```
  feat_select_env           /cluster/scratch/bblack/micromamba/envs/feat_select_env
  transition_model_env      /cluster/scratch/bblack/micromamba/envs/transition_model_env
  allocation_params_env     /cluster/scratch/bblack/micromamba/envs/allocation_params_env
```

## Script Updates

### New Common Functions (`hpc_common.sh`)

All HPC submit scripts now use shared functions from `scripts/hpc_common.sh`:

- **`find_micromamba()`**: Automatically finds micromamba in multiple locations
- **`activate_env()`**: Activates a conda environment with error checking
- **`setup_common_env()`**: Sets up R_LIBS_USER, locale, TERRA_TEMP
- **`verify_rscript()`**: Verifies Rscript exists in environment

### Scripts Already Updated

✅ `submit_calibrate_allocation_parameters.sh` - Uses `hpc_common.sh`
✅ `setup_environments.sh` - Auto-finds micromamba in multiple locations

### Update Other Submit Scripts

To update remaining submit scripts:

```bash
cd /cluster/home/bblack/nascent-lulcc
bash scripts/update_all_submit_scripts.sh
```

This will:
- Create backups (`.bak` files)
- Identify scripts using old micromamba path
- Provide update instructions

### Manual Update Pattern

For each `submit_*.sh` script, replace the old pattern:

**OLD:**
```bash
export MAMBA_EXE="/cluster/project/eawag/p01002/.local/bin/micromamba"
eval "$($MAMBA_EXE shell hook -s bash)"
micromamba activate "$ENV_PATH"
export R_LIBS_USER="$HOME/R_libs"
# ... etc
```

**NEW:**
```bash
# Load common HPC functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/hpc_common.sh"

# Setup and activate environment
setup_common_env
activate_env "$ENV_BASE_PATH/your_env_name"
```

## Environment Recreation

Since environments are in scratch (wiped regularly), you may need to recreate them:

```bash
# Check if environments exist
ls -la /cluster/scratch/bblack/micromamba/envs/

# If missing, recreate all
bash scripts/setup_environments.sh

# Or create specific environment
$HOME/.local/bin/micromamba env create -f environments/allocation_params_env.yml \
  -p /cluster/scratch/bblack/micromamba/envs/allocation_params_env
```

## Troubleshooting

### Micromamba Not Found

```bash
# Check installation
ls -lh $HOME/.local/bin/micromamba

# If missing, reinstall
bash scripts/install_micromamba.sh
```

### Environment Not Found

```bash
# List existing environments
$HOME/.local/bin/micromamba env list

# Recreate missing environment
bash scripts/setup_environments.sh
```

### Job Fails with "micromamba not found"

Ensure `hpc_common.sh` can find micromamba:

```bash
# Set custom location (if needed)
export MAMBA_EXE_CUSTOM="/path/to/your/micromamba"
sbatch scripts/submit_calibrate_allocation_parameters.sh
```

### Disk Quota Exceeded in Home

If home directory is full:

1. Check usage: `quota -s`
2. Clean up: Remove old files, logs, backups
3. Micromamba itself is small (~20 MB)
4. Environments are in scratch, not home

## File Locations Summary

| Component | Location | Size | Persistence |
|-----------|----------|------|-------------|
| Micromamba binary | `$HOME/.local/bin/micromamba` | ~20 MB | Permanent |
| Project code | `$HOME/nascent-lulcc` or on switchdrive | ~100 MB | Permanent |
| Conda environments | `/cluster/scratch/bblack/micromamba/envs/` | ~2-5 GB | Temporary* |
| Temporary raster files | `/cluster/scratch/bblack/terra_temp/` | Variable | Temporary* |
| Results/outputs | `/cluster/scratch/bblack/nascent-lulcc/` | Large | Temporary* |

*Can be recreated from scripts when needed

## Next Steps

1. ✅ Install micromamba: `bash scripts/install_micromamba.sh`
2. ✅ Create environments: `bash scripts/setup_environments.sh`
3. ✅ Update remaining submit scripts (if needed)
4. ✅ Test a job: `sbatch scripts/submit_calibrate_allocation_parameters.sh`
5. ✅ Monitor job: `squeue -u bblack`
6. ✅ Check output: `cat logs/calibrate-allocation-params-*.out`
