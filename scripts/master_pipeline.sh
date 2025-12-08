#!/bin/bash
# master_pipeline.sh
# Master script to run the complete LULCC modeling pipeline

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "========================================="
echo "LULCC Modeling Master Pipeline"
echo "========================================="
echo "Script directory: $SCRIPT_DIR"
echo "Project root: $PROJECT_ROOT"
echo

# Create logs directory if it doesn't exist
mkdir -p "$PROJECT_ROOT/logs"

# Function to submit job and wait for completion
submit_and_wait() {
    local script_name="$1"
    local job_name="$2"
    local dependency="$3"
    
    echo "Submitting $job_name..."
    
    if [ -n "$dependency" ]; then
        job_id=$(sbatch --dependency=afterok:$dependency --parsable "$SCRIPT_DIR/$script_name")
    else
        job_id=$(sbatch --parsable "$SCRIPT_DIR/$script_name")
    fi
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to submit $job_name"
        exit 1
    fi
    
    echo "  Job ID: $job_id"
    return $job_id
}

# Function to check job status
check_job_status() {
    local job_id="$1"
    local job_name="$2"
    
    echo "Monitoring $job_name (Job ID: $job_id)..."
    
    while true; do
        status=$(squeue -j $job_id -h -o %T 2>/dev/null)
        
        if [ -z "$status" ]; then
            # Job no longer in queue, check if it completed successfully
            sacct -j $job_id -n -o State | grep -q "COMPLETED"
            if [ $? -eq 0 ]; then
                echo "  ✓ $job_name completed successfully"
                return 0
            else
                echo "  ✗ $job_name failed or was cancelled"
                echo "  Check logs: logs/*-$job_id.{out,err}"
                exit 1
            fi
        fi
        
        case "$status" in
            "PENDING"|"RUNNING")
                echo "  Status: $status ($(date '+%H:%M:%S'))"
                sleep 30
                ;;
            "COMPLETED")
                echo "  ✓ $job_name completed successfully"
                return 0
                ;;
            *)
                echo "  ✗ $job_name failed with status: $status"
                echo "  Check logs: logs/*-$job_id.{out,err}"
                exit 1
                ;;
        esac
    done
}

# Record start time
start_time=$(date)
echo "Pipeline started at: $start_time"
echo

# Step 1: Data Preparation
echo "========================================="
echo "Step 1: Data Preparation"
echo "========================================="

data_prep_job_id=$(sbatch --parsable "$SCRIPT_DIR/submit_data_preparation.sh")
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to submit data preparation job"
    exit 1
fi

echo "Data preparation job submitted with ID: $data_prep_job_id"
check_job_status $data_prep_job_id "data preparation"

# Step 2: Feature Selection (depends on data preparation)
echo
echo "========================================="
echo "Step 2: Feature Selection"
echo "========================================="

fs_job_id=$(sbatch --dependency=afterok:$data_prep_job_id --parsable "$SCRIPT_DIR/submit_feature_selection.sh")
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to submit feature selection job"
    exit 1
fi

echo "Feature selection job submitted with ID: $fs_job_id"
check_job_status $fs_job_id "feature selection"

# Step 3: Transition Modeling (depends on feature selection)
echo
echo "========================================="
echo "Step 3: Transition Modeling"
echo "========================================="

model_job_id=$(sbatch --dependency=afterok:$fs_job_id --parsable "$SCRIPT_DIR/submit_transition_modeling.sh")
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to submit transition modeling job"
    exit 1
fi

echo "Transition modeling job submitted with ID: $model_job_id"
check_job_status $model_job_id "transition modeling"

# Step 4: Model Finalization (depends on transition modeling)
echo
echo "========================================="
echo "Step 4: Model Finalization"
echo "========================================="

model_final_job_id=$(sbatch --dependency=afterok:$model_job_id --parsable "$SCRIPT_DIR/submit_model_finalization.sh")
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to submit model finalization job"
    exit 1
fi

echo "Model finalization job submitted with ID: $model_final_job_id"
check_job_status $model_final_job_id "model finalization"

# Step 5: Scenario Preparation (depends on model finalization)
echo
echo "========================================="
echo "Step 5: Scenario Preparation"
echo "========================================="

scenario_prep_job_id=$(sbatch --dependency=afterok:$model_final_job_id --parsable "$SCRIPT_DIR/submit_scenario_preparation.sh")
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to submit scenario preparation job"
    exit 1
fi

echo "Scenario preparation job submitted with ID: $scenario_prep_job_id"
check_job_status $scenario_prep_job_id "scenario preparation"

# Step 6: Simulation Setup (depends on scenario preparation)
echo
echo "========================================="
echo "Step 6: Simulation Setup"
echo "========================================="

sim_setup_job_id=$(sbatch --dependency=afterok:$scenario_prep_job_id --parsable "$SCRIPT_DIR/submit_simulation_setup.sh")
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to submit simulation setup job"
    exit 1
fi

echo "Simulation setup job submitted with ID: $sim_setup_job_id"
check_job_status $sim_setup_job_id "simulation setup"

# Step 7: Dinamica Simulations (depends on simulation setup)
echo
echo "========================================="
echo "Step 7: Dinamica Simulations"
echo "========================================="

dinamica_job_id=$(sbatch --dependency=afterok:$sim_setup_job_id --parsable "$SCRIPT_DIR/submit_dinamica_simulations.sh")
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to submit Dinamica simulations job"
    exit 1
fi

echo "Dinamica simulations job submitted with ID: $dinamica_job_id"
check_job_status $dinamica_job_id "dinamica simulations"

# Pipeline completed
end_time=$(date)
echo
echo "========================================="
echo "Complete Pipeline Finished Successfully!"
echo "========================================="
echo "Started: $start_time"
echo "Ended: $end_time"
echo

# Generate comprehensive summary report
summary_file="$PROJECT_ROOT/logs/complete_pipeline_summary_$(date +%Y%m%d_%H%M%S).txt"
{
    echo "Complete LULCC Modeling Pipeline Summary"
    echo "========================================"
    echo "Started: $start_time"
    echo "Ended: $end_time"
    echo
    echo "Job IDs:"
    echo "  Data Preparation: $data_prep_job_id"
    echo "  Feature Selection: $fs_job_id"
    echo "  Transition Modeling: $model_job_id"
    echo "  Model Finalization: $model_final_job_id"
    echo "  Scenario Preparation: $scenario_prep_job_id"
    echo "  Simulation Setup: $sim_setup_job_id"
    echo "  Dinamica Simulations: $dinamica_job_id"
    echo
    echo "Log files:"
    echo "  Data Preparation: logs/data-prep-$data_prep_job_id.{out,err}"
    echo "  Feature Selection: logs/feat-select-$fs_job_id.{out,err}"
    echo "  Transition Modeling: logs/trans-model-$model_job_id.{out,err}"
    echo "  Model Finalization: logs/model-final-$model_final_job_id.{out,err}"
    echo "  Scenario Preparation: logs/scenario-prep-$scenario_prep_job_id.{out,err}"
    echo "  Simulation Setup: logs/sim-setup-$sim_setup_job_id.{out,err}"
    echo "  Dinamica Simulations: logs/dinamica-sim-$dinamica_job_id.{out,err}"
    echo
    echo "Pipeline stages:"
    echo "  1. Data Preparation: LULC data, regions, ancillary data, predictors, transitions"
    echo "  2. Feature Selection: Predictor variable selection with GRRF"
    echo "  3. Transition Modeling: Statistical modeling of LULC transitions"
    echo "  4. Model Finalization: Evaluation, specification selection, final training"
    echo "  5. Scenario Preparation: Transition tables and predictor data for scenarios"
    echo "  6. Simulation Setup: Calibration parameters and spatial interventions"
    echo "  7. Dinamica Simulations: Final land use change simulations"
    echo
    echo "Output directories: Check individual step logs and configuration files"
} > "$summary_file"

echo "Complete pipeline summary saved to: $summary_file"
echo "All steps completed successfully!"