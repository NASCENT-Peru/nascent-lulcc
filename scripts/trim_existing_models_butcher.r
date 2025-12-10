#!/usr/bin/env Rscript
# trim_existing_models_butcher.r
# Script to trim model .rds files using the tidymodels butcher package
# This provides a cleaner, more reliable approach than manual component removal

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 0) {
  cat(
    "Usage: Rscript trim_existing_models_butcher.r <input_dir> [backup_dir]\n"
  )
  cat("  input_dir: Directory containing .rds model files to process\n")
  cat(
    "  backup_dir: Optional directory to backup original files (default: <input_dir>_backup)\n"
  )
  cat("\nExample:\n")
  cat("  Rscript trim_existing_models_butcher.r /path/to/models\n")
  cat(
    "  Rscript trim_existing_models_butcher.r /path/to/models /path/to/backup\n"
  )
  quit(status = 1)
}

input_dir <- args[1]
backup_dir <- if (length(args) >= 2) args[2] else paste0(input_dir, "_backup")

# Validate input directory
if (!dir.exists(input_dir)) {
  cat("ERROR: Input directory does not exist:", input_dir, "\n")
  quit(status = 1)
}

# Check and install required packages with error handling
butcher_available <- FALSE
tryCatch(
  {
    if (!requireNamespace("butcher", quietly = TRUE)) {
      cat("Installing butcher package...\n")
      install.packages(
        "butcher",
        repos = "https://cloud.r-project.org/",
        dependencies = TRUE
      )
    }

    library(butcher)
    butcher_available <- TRUE
    cat("✓ Butcher package loaded successfully\n")
  },
  error = function(e) {
    cat("✗ Failed to install/load butcher package:", e$message, "\n")
    cat("Will use fallback manual trimming approach\n")
    butcher_available <- FALSE
  }
)

cat(sprintf("Butcher package available: %s\n", butcher_available))

# Create backup directory
if (!dir.exists(backup_dir)) {
  cat("Creating backup directory:", backup_dir, "\n")
  dir.create(backup_dir, recursive = TRUE, showWarnings = FALSE)
}

cat("\n========================================\n")
cat("BUTCHER Model Trimming Script\n")
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

# Function to trim model components using butcher
butcher_trim_model <- function(model_obj, filepath) {
  original_size <- as.numeric(object.size(model_obj)) / (1024^2)

  cat(sprintf("    Original object size: %.1f MB\n", original_size))

  # Check if this is a workflow or minimal model structure
  is_workflow <- inherits(model_obj, "workflow")
  is_minimal <- is.list(model_obj) &&
    "model" %in% names(model_obj) &&
    "recipe" %in% names(model_obj)

  trimmed_obj <- model_obj
  operations_performed <- c()

  if (is_workflow) {
    cat("    Detected workflow object - applying butcher operations...\n")

    # Apply butcher operations for workflows
    tryCatch(
      {
        # Remove the call (contains large environment)
        trimmed_obj <- butcher::axe_call(trimmed_obj)
        operations_performed <- c(operations_performed, "axe_call")
        cat("      ✓ Removed call objects\n")
      },
      error = function(e) cat("      - Could not remove call:", e$message, "\n")
    )

    tryCatch(
      {
        # Remove fitted values and residuals
        trimmed_obj <- butcher::axe_fitted(trimmed_obj)
        operations_performed <- c(operations_performed, "axe_fitted")
        cat("      ✓ Removed fitted values\n")
      },
      error = function(e) {
        cat("      - Could not remove fitted values:", e$message, "\n")
      }
    )

    tryCatch(
      {
        # Remove data (training data stored in object)
        trimmed_obj <- butcher::axe_data(trimmed_obj)
        operations_performed <- c(operations_performed, "axe_data")
        cat("      ✓ Removed training data\n")
      },
      error = function(e) cat("      - Could not remove data:", e$message, "\n")
    )

    tryCatch(
      {
        # Remove environment (can be large)
        trimmed_obj <- butcher::axe_env(trimmed_obj)
        operations_performed <- c(operations_performed, "axe_env")
        cat("      ✓ Removed environments\n")
      },
      error = function(e) {
        cat("      - Could not remove environment:", e$message, "\n")
      }
    )

    # Additional ranger-specific cleanup for workflows
    if (
      !is.null(trimmed_obj$fit) &&
        !is.null(trimmed_obj$fit$fit) &&
        !is.null(trimmed_obj$fit$fit$fit) &&
        !is.null(trimmed_obj$fit$fit$fit$predictions)
    ) {
      pred_size <- as.numeric(object.size(
        trimmed_obj$fit$fit$fit$predictions
      )) /
        (1024^2)
      trimmed_obj$fit$fit$fit$predictions <- NULL
      operations_performed <- c(
        operations_performed,
        sprintf("manual predictions removal (%.1f MB)", pred_size)
      )
      cat(sprintf(
        "      ✓ Manually removed predictions matrix: %.1f MB\n",
        pred_size
      ))
    }
  } else if (is_minimal) {
    cat(
      "    Detected minimal model structure - applying targeted butcher operations...\n"
    )

    # For minimal models, try to butcher the inner model component
    if (!is.null(trimmed_obj$model)) {
      tryCatch(
        {
          trimmed_obj$model <- butcher::axe_call(trimmed_obj$model)
          operations_performed <- c(operations_performed, "axe_call on model")
          cat("      ✓ Removed call from inner model\n")
        },
        error = function(e) {
          cat("      - Could not remove call from model:", e$message, "\n")
        }
      )

      tryCatch(
        {
          trimmed_obj$model <- butcher::axe_fitted(trimmed_obj$model)
          operations_performed <- c(operations_performed, "axe_fitted on model")
          cat("      ✓ Removed fitted values from inner model\n")
        },
        error = function(e) {
          cat(
            "      - Could not remove fitted values from model:",
            e$message,
            "\n"
          )
        }
      )

      tryCatch(
        {
          trimmed_obj$model <- butcher::axe_data(trimmed_obj$model)
          operations_performed <- c(operations_performed, "axe_data on model")
          cat("      ✓ Removed data from inner model\n")
        },
        error = function(e) {
          cat("      - Could not remove data from model:", e$message, "\n")
        }
      )

      tryCatch(
        {
          trimmed_obj$model <- butcher::axe_env(trimmed_obj$model)
          operations_performed <- c(operations_performed, "axe_env on model")
          cat("      ✓ Removed environment from inner model\n")
        },
        error = function(e) {
          cat(
            "      - Could not remove environment from model:",
            e$message,
            "\n"
          )
        }
      )

      # Manual cleanup for ranger predictions in minimal models
      if (
        !is.null(trimmed_obj$model$fit) &&
          !is.null(trimmed_obj$model$fit$predictions)
      ) {
        pred_size <- as.numeric(object.size(
          trimmed_obj$model$fit$predictions
        )) /
          (1024^2)
        trimmed_obj$model$fit$predictions <- NULL
        operations_performed <- c(
          operations_performed,
          sprintf("manual predictions removal (%.1f MB)", pred_size)
        )
        cat(sprintf(
          "      ✓ Manually removed predictions matrix: %.1f MB\n",
          pred_size
        ))
      }
    }

    # Also try to butcher the recipe if present
    if (!is.null(trimmed_obj$recipe)) {
      tryCatch(
        {
          trimmed_obj$recipe <- butcher::axe_call(trimmed_obj$recipe)
          operations_performed <- c(operations_performed, "axe_call on recipe")
          cat("      ✓ Removed call from recipe\n")
        },
        error = function(e) {
          cat("      - Could not remove call from recipe:", e$message, "\n")
        }
      )

      tryCatch(
        {
          trimmed_obj$recipe <- butcher::axe_data(trimmed_obj$recipe)
          operations_performed <- c(operations_performed, "axe_data on recipe")
          cat("      ✓ Removed data from recipe\n")
        },
        error = function(e) {
          cat("      - Could not remove data from recipe:", e$message, "\n")
        }
      )
    }
  } else {
    cat("    Unknown model structure - trying general butcher operations...\n")

    # Try general butcher operations on the object itself
    butcher_functions <- list(
      list(func = butcher::axe_call, name = "axe_call"),
      list(func = butcher::axe_fitted, name = "axe_fitted"),
      list(func = butcher::axe_data, name = "axe_data"),
      list(func = butcher::axe_env, name = "axe_env")
    )

    for (axe_info in butcher_functions) {
      tryCatch(
        {
          trimmed_obj <- axe_info$func(trimmed_obj)
          operations_performed <- c(operations_performed, axe_info$name)
          cat(sprintf("      ✓ Applied %s\n", axe_info$name))
        },
        error = function(e) {
          cat(sprintf(
            "      - Could not apply %s: %s\n",
            axe_info$name,
            e$message
          ))
        }
      )
    }
  }

  # Force garbage collection
  gc(verbose = FALSE)

  final_size <- as.numeric(object.size(trimmed_obj)) / (1024^2)
  reduction <- original_size - final_size
  reduction_pct <- if (original_size > 0) {
    (reduction / original_size) * 100
  } else {
    0
  }

  return(list(
    trimmed = length(operations_performed) > 0,
    reason = if (length(operations_performed) > 0) {
      paste(operations_performed, collapse = "; ")
    } else {
      "No butcher operations applicable"
    },
    original_size_mb = original_size,
    final_size_mb = final_size,
    reduction_mb = reduction,
    reduction_pct = reduction_pct,
    operations_performed = operations_performed,
    trimmed_obj = trimmed_obj
  ))
}

# Process each file
for (i in seq_along(rds_files)) {
  filepath <- rds_files[i]
  filename <- basename(filepath)
  relative_path <- gsub(paste0("^", input_dir, "/?"), "", filepath)

  cat(sprintf("\n[%d/%d] Processing: %s\n", i, total_files, relative_path))

  # Get original file size
  original_file_size <- get_file_size_mb(filepath)
  total_size_before <- total_size_before + original_file_size

  tryCatch(
    {
      # Load the model
      cat("  Loading model...")
      model_obj <- readRDS(filepath)
      cat(sprintf(
        " Done (%.1f MB in memory)\n",
        as.numeric(object.size(model_obj)) / (1024^2)
      ))

      # Apply butcher trimming
      cat("  Applying butcher operations...\n")
      trim_result <- butcher_trim_model(model_obj, filepath)
      model_obj <- trim_result$trimmed_obj

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

        # Force garbage collection before saving
        cat("  Forcing garbage collection...")
        gc(verbose = FALSE)
        cat(" Done\n")

        # Save trimmed model with compression
        cat("  Saving trimmed model...")
        saveRDS(model_obj, filepath, compress = "xz")
        cat(" Done\n")

        # Get new file size
        new_file_size <- get_file_size_mb(filepath)
        total_size_after <- total_size_after + new_file_size

        # Verify the saved file by loading it back
        cat("  Verifying saved file...")
        tryCatch(
          {
            verified_obj <- readRDS(filepath)
            verified_size <- as.numeric(object.size(verified_obj)) / (1024^2)
            cat(sprintf(" Done (%.1f MB in memory)\n", verified_size))

            # Clear the verification object
            rm(verified_obj)
            gc(verbose = FALSE)
          },
          error = function(e) {
            cat(sprintf(" ERROR: %s\n", e$message))
          }
        )

        cat(sprintf(
          "  ✓ BUTCHER TRIMMED: %.1f MB → %.1f MB (%.1f%% reduction)\n",
          trim_result$original_size_mb,
          trim_result$final_size_mb,
          trim_result$reduction_pct
        ))
        cat(sprintf(
          "    File size: %.1f MB → %.1f MB (%.1f%% reduction)\n",
          original_file_size,
          new_file_size,
          if (original_file_size > 0) {
            ((original_file_size - new_file_size) / original_file_size) * 100
          } else {
            0
          }
        ))
        cat(sprintf("    Operations: %s\n", trim_result$reason))

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
}

# Summary
cat("\n========================================\n")
cat("BUTCHER PROCESSING COMPLETE\n")
cat("========================================\n")
cat("End time:", format(Sys.time()), "\n")
cat(sprintf("Total files: %d\n", total_files))
cat(sprintf("Successfully trimmed: %d\n", processed_files))
cat(sprintf("Skipped: %d\n", skipped_files))
cat(sprintf("Errors: %d\n", error_files))
cat("\nStorage savings:\n")
cat(sprintf(
  "Total size before: %.1f MB (%.2f GB)\n",
  total_size_before,
  total_size_before / 1024
))
cat(sprintf(
  "Total size after: %.1f MB (%.2f GB)\n",
  total_size_after,
  total_size_after / 1024
))
cat(sprintf(
  "Total reduction: %.1f MB (%.2f GB, %.1f%%)\n",
  total_size_before - total_size_after,
  (total_size_before - total_size_after) / 1024,
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
