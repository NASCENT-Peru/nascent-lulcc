#!/usr/bin/env Rscript
# trim_existing_models.r
# Script to trim large components from existing model .rds files
# This script processes ranger models to remove predictions matrix and other large components

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 0) {
  cat("Usage: Rscript trim_existing_models.r <input_dir> [backup_dir]\n")
  cat("  input_dir: Directory containing .rds model files to process\n")
  cat(
    "  backup_dir: Optional directory to backup original files (default: <input_dir>_backup)\n"
  )
  cat("\nExample:\n")
  cat("  Rscript trim_existing_models.r /path/to/models\n")
  cat("  Rscript trim_existing_models.r /path/to/models /path/to/backup\n")
  quit(status = 1)
}

input_dir <- args[1]
backup_dir <- if (length(args) >= 2) args[2] else paste0(input_dir, "_backup")

# Validate input directory
if (!dir.exists(input_dir)) {
  cat("ERROR: Input directory does not exist:", input_dir, "\n")
  quit(status = 1)
}

# Create backup directory
if (!dir.exists(backup_dir)) {
  cat("Creating backup directory:", backup_dir, "\n")
  dir.create(backup_dir, recursive = TRUE, showWarnings = FALSE)
}

cat("\n========================================\n")
cat("Model Trimming Script\n")
cat("========================================\n")
cat("Input directory:", input_dir, "\n")
cat("Backup directory:", backup_dir, "\n")
cat("Start time:", format(Sys.time()), "\n\n")

# Find all .rds files in the input directory
rds_files <- list.files(
  input_dir,
  pattern = "\\.rds$",
  full.names = TRUE,
  recursive = TRUE
)

if (length(rds_files) == 0) {
  cat("No .rds files found in:", input_dir, "\n")
  quit(status = 0)
}

cat("Found", length(rds_files), ".rds files to process\n\n")

# Initialize counters
total_files <- length(rds_files)
processed_files <- 0
skipped_files <- 0
error_files <- 0
total_size_before <- 0
total_size_after <- 0

# Function to get file size in MB
get_file_size_mb <- function(filepath) {
  if (file.exists(filepath)) {
    size_bytes <- file.info(filepath)$size
    return(size_bytes / (1024^2))
  }
  return(0)
}

# Function to trim model components
trim_model_components <- function(model_obj, filepath) {
  original_size <- as.numeric(object.size(model_obj)) / (1024^2)

  # Check if this is a workflow or minimal model structure
  is_workflow <- inherits(model_obj, "workflow")
  is_minimal <- is.list(model_obj) &&
    "model" %in% names(model_obj) &&
    "recipe" %in% names(model_obj)

  if (is_workflow) {
    # Standard tidymodels workflow
    fitted_model <- model_obj$fit$fit
    model_location <- "workflow$fit$fit"
  } else if (is_minimal) {
    # Minimal model structure
    fitted_model <- model_obj$model
    model_location <- "minimal$model"
  } else {
    return(list(
      trimmed = FALSE,
      reason = "Unknown model structure",
      original_size_mb = original_size,
      final_size_mb = original_size,
      reduction_mb = 0,
      reduction_pct = 0
    ))
  }

  # Check if it's a ranger model
  if (!"_ranger" %in% class(fitted_model)) {
    return(list(
      trimmed = FALSE,
      reason = "Not a ranger model",
      original_size_mb = original_size,
      final_size_mb = original_size,
      reduction_mb = 0,
      reduction_pct = 0
    ))
  }

  # Track what components we remove
  removed_components <- c()

  # Remove predictions matrix (this is usually the largest component)
  if (!is.null(fitted_model$fit$predictions)) {
    pred_size <- object.size(fitted_model$fit$predictions) / (1024^2)
    fitted_model$fit$predictions <- NULL
    removed_components <- c(
      removed_components,
      sprintf("predictions (%.1f MB)", pred_size)
    )
  }

  # Remove variable importance
  if (!is.null(fitted_model$fit$variable.importance)) {
    fitted_model$fit$variable.importance <- NULL
    removed_components <- c(removed_components, "variable.importance")
  }

  # Remove call object
  if (!is.null(fitted_model$fit$call)) {
    fitted_model$fit$call <- NULL
    removed_components <- c(removed_components, "call")
  }

  # Remove inbag.counts if present
  if (!is.null(fitted_model$fit$inbag.counts)) {
    inbag_size <- object.size(fitted_model$fit$inbag.counts) / (1024^2)
    fitted_model$fit$inbag.counts <- NULL
    removed_components <- c(
      removed_components,
      sprintf("inbag.counts (%.1f MB)", inbag_size)
    )
  }

  # For workflows, also remove large components from recipe/mold
  if (is_workflow) {
    # Remove template from recipe if present
    if (!is.null(model_obj$pre$actions$recipe$recipe$template)) {
      template_size <- object.size(
        model_obj$pre$actions$recipe$recipe$template
      ) /
        (1024^2)
      model_obj$pre$actions$recipe$recipe$template <- NULL
      removed_components <- c(
        removed_components,
        sprintf("recipe template (%.1f MB)", template_size)
      )
    }

    # Remove mold if it's very large
    if (!is.null(model_obj$pre$mold)) {
      mold_size <- object.size(model_obj$pre$mold) / (1024^2)
      if (mold_size > 50) {
        # Only remove if > 50MB
        model_obj$pre$mold <- NULL
        removed_components <- c(
          removed_components,
          sprintf("mold (%.1f MB)", mold_size)
        )
      }
    }

    # Update the fitted model in the workflow
    model_obj$fit$fit <- fitted_model
  } else {
    # Update the fitted model in minimal structure
    model_obj$model <- fitted_model
  }

  final_size <- as.numeric(object.size(model_obj)) / (1024^2)
  reduction <- original_size - final_size
  reduction_pct <- (reduction / original_size) * 100

  return(list(
    trimmed = length(removed_components) > 0,
    reason = if (length(removed_components) > 0) {
      paste(removed_components, collapse = ", ")
    } else {
      "No components to remove"
    },
    original_size_mb = original_size,
    final_size_mb = final_size,
    reduction_mb = reduction,
    reduction_pct = reduction_pct,
    model_location = model_location
  ))
}

# Process each file
for (i in seq_along(rds_files)) {
  filepath <- rds_files[i]
  filename <- basename(filepath)
  relative_path <- gsub(paste0("^", input_dir, "/?"), "", filepath)

  cat(sprintf("[%d/%d] Processing: %s\n", i, total_files, relative_path))

  # Get original file size
  original_file_size <- get_file_size_mb(filepath)
  total_size_before <- total_size_before + original_file_size

  tryCatch(
    {
      # Load the model
      cat("  Loading model...")
      model_obj <- readRDS(filepath)
      cat(" Done\n")

      # Trim the model
      cat("  Analyzing and trimming...")
      trim_result <- trim_model_components(model_obj, filepath)
      cat(" Done\n")

      if (trim_result$trimmed) {
        # Create backup path maintaining directory structure
        backup_path <- file.path(backup_dir, relative_path)
        backup_subdir <- dirname(backup_path)

        if (!dir.exists(backup_subdir)) {
          dir.create(backup_subdir, recursive = TRUE, showWarnings = FALSE)
        }

        # Backup original file
        cat("  Creating backup...")
        file.copy(filepath, backup_path, overwrite = TRUE)
        cat(" Done\n")

        # Save trimmed model
        cat("  Saving trimmed model...")
        saveRDS(model_obj, filepath)
        cat(" Done\n")

        # Get new file size
        new_file_size <- get_file_size_mb(filepath)
        total_size_after <- total_size_after + new_file_size

        cat(sprintf(
          "  ✓ TRIMMED: %.1f MB → %.1f MB (%.1f%% reduction)\n",
          trim_result$original_size_mb,
          trim_result$final_size_mb,
          trim_result$reduction_pct
        ))
        cat(sprintf(
          "    File size: %.1f MB → %.1f MB\n",
          original_file_size,
          new_file_size
        ))
        cat(sprintf("    Removed: %s\n", trim_result$reason))

        processed_files <- processed_files + 1
      } else {
        total_size_after <- total_size_after + original_file_size
        cat(sprintf(
          "  - SKIPPED: %s (%.1f MB)\n",
          trim_result$reason,
          trim_result$original_size_mb
        ))
        skipped_files <- skipped_files + 1
      }
    },
    error = function(e) {
      total_size_after <- total_size_after + original_file_size
      cat(sprintf("  ✗ ERROR: %s\n", e$message))
      error_files <- error_files + 1
    }
  )

  cat("\n")
}

# Summary
cat("========================================\n")
cat("PROCESSING COMPLETE\n")
cat("========================================\n")
cat("End time:", format(Sys.time()), "\n")
cat(sprintf("Total files: %d\n", total_files))
cat(sprintf("Successfully trimmed: %d\n", processed_files))
cat(sprintf("Skipped (not ranger/no components): %d\n", skipped_files))
cat(sprintf("Errors: %d\n", error_files))
cat("\nStorage savings:\n")
cat(sprintf("Total size before: %.1f MB\n", total_size_before))
cat(sprintf("Total size after: %.1f MB\n", total_size_after))
cat(sprintf(
  "Total reduction: %.1f MB (%.1f%%)\n",
  total_size_before - total_size_after,
  ((total_size_before - total_size_after) / total_size_before) * 100
))
cat(sprintf("\nBackups created in: %s\n", backup_dir))

if (error_files > 0) {
  cat("\nWARNING: Some files had errors. Check the output above for details.\n")
  quit(status = 1)
} else {
  cat("\nAll files processed successfully!\n")
  quit(status = 0)
}
