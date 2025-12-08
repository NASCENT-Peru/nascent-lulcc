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
                echo "  Check logs: logs/$job_name-$job_id.{out,err}"
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
                echo "  Check logs: logs/$job_name-$job_id.{out,err}"
                exit 1
                ;;
        esac
    done
}

# Record start time
start_time=$(date)
echo "Pipeline started at: $start_time"
echo

# Step 1: Feature Selection
echo "========================================="
echo "Step 1: Feature Selection"
echo "========================================="

fs_job_id=$(sbatch --parsable "$SCRIPT_DIR/submit_feature_selection.sh")
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to submit feature selection job"
    exit 1
fi

echo "Feature selection job submitted with ID: $fs_job_id"
check_job_status $fs_job_id "feature selection"

# Step 2: Transition Modeling (depends on feature selection)
echo
echo "========================================="
echo "Step 2: Transition Modeling"
echo "========================================="

model_job_id=$(sbatch --dependency=afterok:$fs_job_id --parsable "$SCRIPT_DIR/submit_transition_modeling.sh")
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to submit transition modeling job"
    exit 1
fi

echo "Transition modeling job submitted with ID: $model_job_id"
check_job_status $model_job_id "transition modeling"

# Pipeline completed
end_time=$(date)
echo
echo "========================================="
echo "Pipeline Completed Successfully!"
echo "========================================="
echo "Started: $start_time"
echo "Ended: $end_time"
echo

# Generate summary report
summary_file="$PROJECT_ROOT/logs/pipeline_summary_$(date +%Y%m%d_%H%M%S).txt"
{
    echo "LULCC Modeling Pipeline Summary"
    echo "==============================="
    echo "Started: $start_time"
    echo "Ended: $end_time"
    echo
    echo "Job IDs:"
    echo "  Feature Selection: $fs_job_id"
    echo "  Transition Modeling: $model_job_id"
    echo
    echo "Log files:"
    echo "  Feature Selection: logs/feat-select-$fs_job_id.{out,err}"
    echo "  Transition Modeling: logs/trans-model-$model_job_id.{out,err}"
    echo
    echo "Output directories:"
    echo "  Feature Selection Results: Check config for feature_selection_dir"
    echo "  Model Results: Check config for transition_model_dir"
    echo "  Model Evaluations: Check config for transition_model_eval_dir"
} > "$summary_file"

echo "Summary saved to: $summary_file"
echo "Done!"