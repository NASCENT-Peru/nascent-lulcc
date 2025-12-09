# nascent-lulcc

Land Use/Land Cover Change (LULCC) modelling project for nascent transitions.

## Directory Structure

- **src/**: All R source functions (flat structure)
- **scripts/**: Executable scripts (R pipelines, shell scripts, Python scripts)
- **config/**: Configuration files (YAML, model specs)
- **environments/**: Conda environment definitions
- **dinamica/**: Dinamica EGO model files
- **docs/**: Documentation

## HPC Setup

1. Upload this directory to `/cluster/home/bblack/nascent-lulcc`
2. Copy `.env.template` to `.env` and customize paths
3. Source the environment: `source .env`
4. Create scratch directories on HPC scratch filesystem
5. Set up conda environments from `environments/` directory

## Environment Variables

The `.env` file defines all necessary paths:
- Project code in `/cluster/home/bblack/nascent-lulcc` 
- Data and results on scratch filesystem `/cluster/scratch/bblack/nascent-lulcc`

## Usage

Source the environment configuration in your job scripts:
```bash
source /cluster/home/bblack/nascent-lulcc/.env
```

Then run scripts from the scripts/ directory with proper paths set.
