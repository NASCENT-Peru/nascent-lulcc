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

# Function to create a completely new minimal model object
create_minimal_model <- function(model_obj, filepath) {
  original_size <- as.numeric(object.size(model_obj)) / (1024^2)
  cat(sprintf("    Original object size: %.1f MB\n", original_size))

  # Check if this is a workflow or minimal model structure
  is_workflow <- inherits(model_obj, "workflow")
  is_minimal <- is.list(model_obj) &&
    "model" %in% names(model_obj) &&
    "recipe" %in% names(model_obj)

  if (!is_workflow && !is_minimal) {
    return(list(
      trimmed = FALSE,
      reason = "Unknown model structure - cannot create minimal version",
      original_size_mb = original_size,
      final_size_mb = original_size,
      reduction_mb = 0,
      reduction_pct = 0,
      operations_performed = character(0),
      trimmed_obj = model_obj
    ))
  }

  cat("    Creating completely new minimal object...\n")
  operations_performed <- c()

  # Create new minimal object structure
  new_obj <- list()

  if (is_workflow) {
    cat("      Extracting essential components from workflow...\n")

    # Extract recipe (but clean it)
    if (!is.null(model_obj$pre) && !is.null(model_obj$pre$actions)) {
      cat("        ✓ Extracting recipe\n")
      new_obj$recipe <- model_obj$pre$actions$recipe$recipe
      # Clean the recipe
      if (!is.null(new_obj$recipe)) {
        new_obj$recipe$template <- NULL # Remove training data
        new_obj$recipe$orig_lvls <- NULL # Remove original levels (can be large)
        operations_performed <- c(operations_performed, "cleaned recipe")
      }
    }

    # Extract model fit (but only essential parts)
    if (!is.null(model_obj$fit) && !is.null(model_obj$fit$fit)) {
      cat("        ✓ Extracting model fit\n")
      original_model <- model_obj$fit$fit

      # Create new minimal model structure
      new_model <- list()
      new_model$spec <- original_model$spec # Keep model specification

      # For the actual fit, only keep prediction-essential components
      if (!is.null(original_model$fit)) {
        ranger_fit <- original_model$fit

        # Keep only essential ranger components for prediction
        minimal_ranger <- list()
        if (!is.null(ranger_fit$forest)) {
          minimal_ranger$forest <- ranger_fit$forest # Essential for prediction
        }
        if (!is.null(ranger_fit$treetype)) {
          minimal_ranger$treetype <- ranger_fit$treetype
        }
        if (!is.null(ranger_fit$call)) {
          # Keep a minimal call without large environments
          minimal_ranger$call <- call("ranger")
        }
        if (!is.null(ranger_fit$num.trees)) {
          minimal_ranger$num.trees <- ranger_fit$num.trees
        }
        if (!is.null(ranger_fit$mtry)) {
          minimal_ranger$mtry <- ranger_fit$mtry
        }
        if (!is.null(ranger_fit$min.node.size)) {
          minimal_ranger$min.node.size <- ranger_fit$min.node.size
        }
        if (!is.null(ranger_fit$variable.importance)) {
          minimal_ranger$variable.importance <- ranger_fit$variable.importance
        }

        # Explicitly exclude large components
        # minimal_ranger$predictions <- NULL  (don't even copy)
        # minimal_ranger$inbag.counts <- NULL (don't even copy)

        new_model$fit <- minimal_ranger
        class(new_model$fit) <- class(ranger_fit)
        operations_performed <- c(
          operations_performed,
          "created minimal ranger fit"
        )

        pred_size <- if (!is.null(ranger_fit$predictions)) {
          as.numeric(object.size(ranger_fit$predictions)) / (1024^2)
        } else {
          0
        }

        inbag_size <- if (!is.null(ranger_fit$inbag.counts)) {
          as.numeric(object.size(ranger_fit$inbag.counts)) / (1024^2)
        } else {
          0
        }

        if (pred_size > 0) {
          operations_performed <- c(
            operations_performed,
            sprintf("excluded predictions (%.1f MB)", pred_size)
          )
          cat(sprintf(
            "        ✓ Excluded predictions matrix: %.1f MB\n",
            pred_size
          ))
        }
        if (inbag_size > 0) {
          operations_performed <- c(
            operations_performed,
            sprintf("excluded inbag.counts (%.1f MB)", inbag_size)
          )
          cat(sprintf("        ✓ Excluded inbag.counts: %.1f MB\n", inbag_size))
        }
      }

      class(new_model) <- class(original_model)
      new_obj$model <- new_model
    }
  } else if (is_minimal) {
    cat("      Extracting essential components from minimal structure...\n")

    # Copy and clean recipe
    if (!is.null(model_obj$recipe)) {
      cat("        ✓ Cleaning recipe\n")
      new_obj$recipe <- model_obj$recipe
      new_obj$recipe$template <- NULL
      new_obj$recipe$orig_lvls <- NULL
      operations_performed <- c(operations_performed, "cleaned recipe")
    }

    # Create minimal model
    if (!is.null(model_obj$model) && !is.null(model_obj$model$fit)) {
      cat("        ✓ Creating minimal model\n")
      original_fit <- model_obj$model$fit

      # Create minimal ranger object
      minimal_ranger <- list()
      if (!is.null(original_fit$forest)) {
        minimal_ranger$forest <- original_fit$forest
      }
      if (!is.null(original_fit$treetype)) {
        minimal_ranger$treetype <- original_fit$treetype
      }
      if (!is.null(original_fit$call)) {
        minimal_ranger$call <- call("ranger")
      }
      if (!is.null(original_fit$num.trees)) {
        minimal_ranger$num.trees <- original_fit$num.trees
      }
      if (!is.null(original_fit$mtry)) {
        minimal_ranger$mtry <- original_fit$mtry
      }
      if (!is.null(original_fit$min.node.size)) {
        minimal_ranger$min.node.size <- original_fit$min.node.size
      }
      if (!is.null(original_fit$variable.importance)) {
        minimal_ranger$variable.importance <- original_fit$variable.importance
      }

      class(minimal_ranger) <- class(original_fit)

      # Create new model wrapper
      new_model <- list()
      new_model$spec <- model_obj$model$spec
      new_model$fit <- minimal_ranger
      class(new_model) <- class(model_obj$model)

      new_obj$model <- new_model
      operations_performed <- c(
        operations_performed,
        "created minimal model structure"
      )

      # Report on excluded components
      pred_size <- if (!is.null(original_fit$predictions)) {
        as.numeric(object.size(original_fit$predictions)) / (1024^2)
      } else {
        0
      }

      inbag_size <- if (!is.null(original_fit$inbag.counts)) {
        as.numeric(object.size(original_fit$inbag.counts)) / (1024^2)
      } else {
        0
      }

      if (pred_size > 0) {
        operations_performed <- c(
          operations_performed,
          sprintf("excluded predictions (%.1f MB)", pred_size)
        )
        cat(sprintf(
          "        ✓ Excluded predictions matrix: %.1f MB\n",
          pred_size
        ))
      }
      if (inbag_size > 0) {
        operations_performed <- c(
          operations_performed,
          sprintf("excluded inbag.counts (%.1f MB)", inbag_size)
        )
        cat(sprintf("        ✓ Excluded inbag.counts: %.1f MB\n", inbag_size))
      }
    }
  }

  # Copy other essential top-level components if they exist
  for (name in names(model_obj)) {
    if (
      !name %in% c("model", "recipe", "pre", "fit") &&
        !is.null(model_obj[[name]])
    ) {
      # Only copy if it's small (< 1MB)
      component_size <- as.numeric(object.size(model_obj[[name]])) / (1024^2)
      if (component_size < 1.0) {
        new_obj[[name]] <- model_obj[[name]]
        operations_performed <- c(
          operations_performed,
          sprintf("copied %s", name)
        )
      } else {
        cat(sprintf(
          "        - Skipped large component %s (%.1f MB)\n",
          name,
          component_size
        ))
        operations_performed <- c(
          operations_performed,
          sprintf("excluded large %s (%.1f MB)", name, component_size)
        )
      }
    }
  }

  # Ensure proper class structure
  class(new_obj) <- class(model_obj)

  # Force garbage collection
  gc(verbose = FALSE)

  final_size <- as.numeric(object.size(new_obj)) / (1024^2)
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
      "No operations performed"
    },
    original_size_mb = original_size,
    final_size_mb = final_size,
    reduction_mb = reduction,
    reduction_pct = reduction_pct,
    operations_performed = operations_performed,
    trimmed_obj = new_obj
  ))
}

# Function to trim model components using butcher (fallback approach)
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

    # Also remove inbag.counts from workflows
    if (
      !is.null(trimmed_obj$fit) &&
        !is.null(trimmed_obj$fit$fit) &&
        !is.null(trimmed_obj$fit$fit$fit) &&
        !is.null(trimmed_obj$fit$fit$fit$inbag.counts)
    ) {
      inbag_size <- as.numeric(object.size(
        trimmed_obj$fit$fit$fit$inbag.counts
      )) /
        (1024^2)
      trimmed_obj$fit$fit$fit$inbag.counts <- NULL
      operations_performed <- c(
        operations_performed,
        sprintf("manual inbag.counts removal (%.1f MB)", inbag_size)
      )
      cat(sprintf(
        "      ✓ Manually removed inbag.counts: %.1f MB\n",
        inbag_size
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

      # Also remove inbag.counts if present (can be large)
      if (
        !is.null(trimmed_obj$model$fit) &&
          !is.null(trimmed_obj$model$fit$inbag.counts)
      ) {
        inbag_size <- as.numeric(object.size(
          trimmed_obj$model$fit$inbag.counts
        )) /
          (1024^2)
        trimmed_obj$model$fit$inbag.counts <- NULL
        operations_performed <- c(
          operations_performed,
          sprintf("manual inbag.counts removal (%.1f MB)", inbag_size)
        )
        cat(sprintf(
          "      ✓ Manually removed inbag.counts: %.1f MB\n",
          inbag_size
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

      # Apply minimal object creation (more aggressive than butcher)
      cat("  Creating minimal model object...\n")
      trim_result <- create_minimal_model(model_obj, filepath)

      # If minimal creation failed, try butcher as fallback
      if (!trim_result$trimmed) {
        cat("  Minimal creation failed, trying butcher operations...\n")
        trim_result <- butcher_trim_model(model_obj, filepath)
      }

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

        # Additional aggressive cleanup before saving
        cat("  Performing additional cleanup...")

        # Manual cleanup of known problematic components
        if (inherits(model_obj, "workflow")) {
          # Check for ranger predictions in workflow
          if (
            !is.null(model_obj$fit) &&
              !is.null(model_obj$fit$fit) &&
              !is.null(model_obj$fit$fit$fit)
          ) {
            if (!is.null(model_obj$fit$fit$fit$predictions)) {
              cat(" removing workflow predictions...")
              model_obj$fit$fit$fit$predictions <- NULL
            }
            if (!is.null(model_obj$fit$fit$fit$inbag.counts)) {
              cat(" removing inbag.counts...")
              model_obj$fit$fit$fit$inbag.counts <- NULL
            }
          }
        } else if (is.list(model_obj) && "model" %in% names(model_obj)) {
          # Check for ranger predictions in minimal structure
          if (!is.null(model_obj$model) && !is.null(model_obj$model$fit)) {
            if (!is.null(model_obj$model$fit$predictions)) {
              cat(" removing model predictions...")
              model_obj$model$fit$predictions <- NULL
            }
            if (!is.null(model_obj$model$fit$inbag.counts)) {
              cat(" removing inbag.counts...")
              model_obj$model$fit$inbag.counts <- NULL
            }
          }
        }

        cat(" Done\n")

        # Force garbage collection multiple times
        cat("  Multiple garbage collections...")
        for (i in 1:3) {
          gc(verbose = FALSE)
        }
        cat(" Done\n")

        # Get final object size before saving
        final_obj_size <- as.numeric(object.size(model_obj)) / (1024^2)
        cat(sprintf(
          "  Final object size before saving: %.1f MB\n",
          final_obj_size
        ))

        # Save trimmed model with maximum compression and timing
        cat(
          "  Saving trimmed model (this may take a while for large objects)..."
        )
        start_save_time <- Sys.time()

        # Try different compression approaches
        tryCatch(
          {
            saveRDS(model_obj, filepath, compress = "xz")
          },
          error = function(e) {
            cat(" xz failed, trying gzip...")
            saveRDS(model_obj, filepath, compress = "gzip")
          }
        )

        end_save_time <- Sys.time()
        save_duration <- as.numeric(difftime(
          end_save_time,
          start_save_time,
          units = "secs"
        ))
        cat(sprintf(" Done (%.1f seconds)\n", save_duration))

        # Get new file size
        new_file_size <- get_file_size_mb(filepath)
        total_size_after <- total_size_after + new_file_size

        # Check if file size actually changed
        file_size_change <- original_file_size - new_file_size
        if (abs(file_size_change) < 0.1) {
          cat(sprintf(
            "  ⚠️  WARNING: File size barely changed (%.1f MB reduction)\n",
            file_size_change
          ))
          cat(
            "     This suggests the large components were not successfully removed\n"
          )
        }

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
