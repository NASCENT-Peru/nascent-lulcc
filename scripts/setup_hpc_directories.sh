#!/bin/bash
# setup_hpc_directories.sh
# Script to create the home directory structure for LULCC modeling on HPC
# Focus: /cluster/home/bblack organization

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
USERNAME="bblack"
PROJECT_ID="p01002"
PROJECT_GROUP="eawag"

# Focus on home directory structure
HOME_BASE="/cluster/home/${USERNAME}"

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}HPC Home Directory Setup for LULCC${NC}"
echo -e "${BLUE}=========================================${NC}"
echo
echo -e "${YELLOW}Configuration:${NC}"
echo "  Username: ${USERNAME}"
echo "  Home Directory: ${HOME_BASE}"
echo "  Focus: Personal workspace organization"
echo

# Function to create directory with logging
create_dir() {
    local dir_path="$1"
    local description="$2"
    
    if [ ! -d "$dir_path" ]; then
        mkdir -p "$dir_path"
        if [ $? -eq 0 ]; then
            echo -e "  ${GREEN}✓${NC} Created: $description"
        else
            echo -e "  ${RED}✗${NC} Failed: $description"
            return 1
        fi
    else
        echo -e "  ${YELLOW}↻${NC} Exists: $description"
    fi
}

# Function to set directory permissions
set_permissions() {
    local dir_path="$1"
    local permissions="$2"
    
    if [ -d "$dir_path" ]; then
        chmod "$permissions" "$dir_path"
        echo -e "  ${GREEN}✓${NC} Set permissions $permissions on $(basename $dir_path)"
    fi
}

echo -e "${YELLOW}Creating HOME directory structure...${NC}"
create_dir "${HOME_BASE}/R_libs" "Personal R library"

echo
echo -e "${YELLOW}Creating PROJECT directory structure...${NC}"
create_dir "${PROJECT_BASE}" "Project base directory"
create_dir "${PROJECT_BASE}/code" "Code repository directory"
create_dir "${PROJECT_BASE}/environments" "Conda environments"
create_dir "${PROJECT_BASE}/reference_data" "Small reference datasets"
create_dir "${PROJECT_BASE}/archived_results" "Archived results backup"
create_dir "${PROJECT_BASE}/archived_results/models" "Archived models"
create_dir "${PROJECT_BASE}/archived_results/feature_selection" "Archived feature selection"
create_dir "${PROJECT_BASE}/archived_data" "Archived datasets"

echo
echo -e "${YELLOW}Creating SCRATCH directory structure...${NC}"

# Main scratch directories
create_dir "${SCRATCH_BASE}" "Scratch base directory"
create_dir "${SCRATCH_BASE}/data" "Data directory"
create_dir "${SCRATCH_BASE}/results" "Results directory"
create_dir "${SCRATCH_BASE}/logs" "Logs directory"
create_dir "${SCRATCH_BASE}/temp" "Temporary files directory"

# Data subdirectories
echo
echo -e "${YELLOW}Creating data subdirectories...${NC}"
create_dir "${SCRATCH_BASE}/data/raw" "Raw data"
create_dir "${SCRATCH_BASE}/data/raw/lulc" "LULC data"
create_dir "${SCRATCH_BASE}/data/raw/lulc/original" "Original LULC rasters"
create_dir "${SCRATCH_BASE}/data/raw/lulc/aggregated" "Aggregated LULC data"
create_dir "${SCRATCH_BASE}/data/raw/predictors" "Predictor variables"
create_dir "${SCRATCH_BASE}/data/raw/predictors/static" "Time-invariant predictors"
create_dir "${SCRATCH_BASE}/data/raw/predictors/dynamic" "Time-varying predictors"
create_dir "${SCRATCH_BASE}/data/raw/climate" "Climate data"
create_dir "${SCRATCH_BASE}/data/raw/ancillary_spatial_data" "Additional spatial layers"

create_dir "${SCRATCH_BASE}/data/processed" "Processed datasets"
create_dir "${SCRATCH_BASE}/data/processed/transition_rates" "Transition probability tables"
create_dir "${SCRATCH_BASE}/data/processed/transition_rates/prepared_trans_tables" "Prepared transition tables"
create_dir "${SCRATCH_BASE}/data/processed/transition_rates/raw_trans_tables" "Raw transition tables"
create_dir "${SCRATCH_BASE}/data/processed/transition_rates/extrapolations" "Transition extrapolations"
create_dir "${SCRATCH_BASE}/data/processed/transition_datasets" "Modeling datasets"
create_dir "${SCRATCH_BASE}/data/processed/transition_datasets/parquet" "Arrow/Parquet format"
create_dir "${SCRATCH_BASE}/data/processed/transition_datasets/parquet/transitions" "Response variables"
create_dir "${SCRATCH_BASE}/data/processed/transition_datasets/parquet/static_predictors" "Static predictors (parquet)"
create_dir "${SCRATCH_BASE}/data/processed/transition_datasets/parquet/dynamic_predictors" "Dynamic predictors (parquet)"
create_dir "${SCRATCH_BASE}/data/processed/transition_datasets/pre_predictor_filtering" "Pre-filtering datasets"
create_dir "${SCRATCH_BASE}/data/processed/regionalization" "Regional boundaries"

create_dir "${SCRATCH_BASE}/data/intermediate" "Temporary processing files"
create_dir "${SCRATCH_BASE}/data/intermediate/feature_selection" "Intermediate FS results"
create_dir "${SCRATCH_BASE}/data/intermediate/model_fitting" "Temporary model files"

# Results subdirectories
echo
echo -e "${YELLOW}Creating results subdirectories...${NC}"
create_dir "${SCRATCH_BASE}/results/feature_selection" "Feature selection results"
create_dir "${SCRATCH_BASE}/results/feature_selection/2010_2014" "FS results 2010-2014"
create_dir "${SCRATCH_BASE}/results/feature_selection/2014_2018" "FS results 2014-2018"
create_dir "${SCRATCH_BASE}/results/feature_selection/2018_2022" "FS results 2018-2022"
create_dir "${SCRATCH_BASE}/results/feature_selection/run_summaries" "FS run summaries"

create_dir "${SCRATCH_BASE}/results/models" "Trained models"
create_dir "${SCRATCH_BASE}/results/models/transition_models" "Transition probability models"
create_dir "${SCRATCH_BASE}/results/models/transition_models/national" "National-scale models"
create_dir "${SCRATCH_BASE}/results/models/transition_models/regional" "Regional models"
create_dir "${SCRATCH_BASE}/results/models/allocation_parameters" "Dinamica EGO parameters"
create_dir "${SCRATCH_BASE}/results/models/allocation_parameters/calibration" "Calibration parameters"
create_dir "${SCRATCH_BASE}/results/models/allocation_parameters/simulation" "Simulation parameters"

create_dir "${SCRATCH_BASE}/results/evaluation" "Model evaluation results"
create_dir "${SCRATCH_BASE}/results/evaluation/transition_model_eval" "Performance metrics"
create_dir "${SCRATCH_BASE}/results/evaluation/validation" "Validation results"

create_dir "${SCRATCH_BASE}/results/projections" "Future projections"
create_dir "${SCRATCH_BASE}/results/projections/2030" "2030 projections"
create_dir "${SCRATCH_BASE}/results/projections/2040" "2040 projections"
create_dir "${SCRATCH_BASE}/results/projections/2050" "2050 projections"

# Log subdirectories
echo
echo -e "${YELLOW}Creating log subdirectories...${NC}"
create_dir "${SCRATCH_BASE}/logs/feature_selection" "FS job logs"
create_dir "${SCRATCH_BASE}/logs/transition_modeling" "Modeling job logs"
create_dir "${SCRATCH_BASE}/logs/slurm_logs" "Slurm output files"
create_dir "${SCRATCH_BASE}/logs/pipeline_summaries" "Pipeline run summaries"

# Temp subdirectories
create_dir "${SCRATCH_BASE}/temp/R_temp" "R temporary files"

# Set appropriate permissions
echo
echo -e "${YELLOW}Setting directory permissions...${NC}"
set_permissions "${SCRATCH_BASE}/temp" "755"
set_permissions "${SCRATCH_BASE}/logs" "755"
set_permissions "${PROJECT_BASE}" "755"

# Create environment configuration file
echo
echo -e "${YELLOW}Creating environment configuration...${NC}"
ENV_CONFIG="${HOME_BASE}/.evoland_hpc_config"

cat > "$ENV_CONFIG" << EOF
#!/bin/bash
# LULCC Modeling HPC Environment Configuration
# Source this file in your ~/.bashrc or job scripts

# Base paths
export EVOLAND_DATA_BASEPATH="${SCRATCH_BASE}/data"
export EVOLAND_CODE_PATH="${PROJECT_BASE}/code/evoland-with-baggage"
export EVOLAND_RESULTS_PATH="${SCRATCH_BASE}/results"
export EVOLAND_LOGS_PATH="${SCRATCH_BASE}/logs"

# Environment paths
export CONDA_ENVS_PATH="${PROJECT_BASE}/environments"
export R_LIBS_USER="${HOME_BASE}/R_libs"

# Temporary directories
export TMPDIR="${SCRATCH_BASE}/temp"
export R_TMPDIR="${SCRATCH_BASE}/temp/R_temp"

# Processing options (set in job scripts)
export OMP_NUM_THREADS=\${SLURM_CPUS_PER_TASK:-1}
export OPENBLAS_NUM_THREADS=\${SLURM_CPUS_PER_TASK:-1}

# Create temp directories if they don't exist
mkdir -p "\$TMPDIR" "\$R_TMPDIR" "\$R_LIBS_USER"

# Micromamba configuration
export MAMBA_EXE="/cluster/project/${PROJECT_GROUP}/${PROJECT_ID}/.local/bin/micromamba"
EOF

echo -e "  ${GREEN}✓${NC} Created environment config: $ENV_CONFIG"

# Create a simple disk usage monitoring script
MONITOR_SCRIPT="${HOME_BASE}/check_disk_usage.sh"
cat > "$MONITOR_SCRIPT" << EOF
#!/bin/bash
# Disk usage monitoring script for LULCC modeling

echo "Disk Usage Summary - \$(date)"
echo "================================="
echo
echo "Home Directory (\${HOME}):"
du -sh \${HOME}/* 2>/dev/null | sort -hr | head -10
echo
echo "Project Directory (${PROJECT_BASE}):"
du -sh ${PROJECT_BASE}/* 2>/dev/null | sort -hr
echo
echo "Scratch Directory (${SCRATCH_BASE}):"
du -sh ${SCRATCH_BASE}/* 2>/dev/null | sort -hr
echo
echo "Largest subdirectories in scratch:"
find ${SCRATCH_BASE} -maxdepth 2 -type d -exec du -sh {} \; 2>/dev/null | sort -hr | head -15
echo
echo "Quota Information:"
quota -u ${USERNAME} 2>/dev/null || echo "Quota command not available"
EOF

chmod +x "$MONITOR_SCRIPT"
echo -e "  ${GREEN}✓${NC} Created disk usage monitor: $MONITOR_SCRIPT"

# Summary
echo
echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}Directory Setup Complete!${NC}"
echo -e "${GREEN}=========================================${NC}"
echo
echo -e "${YELLOW}Next Steps:${NC}"
echo "1. Add this line to your ~/.bashrc:"
echo "   source $ENV_CONFIG"
echo
echo "2. Clone your code repository:"
echo "   cd ${PROJECT_BASE}/code"
echo "   git clone <your-repo-url> evoland-with-baggage"
echo
echo "3. Set up conda environments:"
echo "   cd ${PROJECT_BASE}/code/evoland-with-baggage/inst"
echo "   ./setup_environments.sh"
echo
echo "4. Monitor disk usage regularly:"
echo "   $MONITOR_SCRIPT"
echo
echo -e "${YELLOW}Directory Structure Created:${NC}"
echo "  Home:    ${HOME_BASE}"
echo "  Project: ${PROJECT_BASE}"
echo "  Scratch: ${SCRATCH_BASE}"
echo
echo -e "${YELLOW}Important Notes:${NC}"
echo "  - Scratch directory is not backed up - backup important results!"
echo "  - Consider file retention policies for temporary files"
echo "  - Monitor disk quotas regularly"
echo "  - Use the provided environment variables in your scripts"