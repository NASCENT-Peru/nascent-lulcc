#!/usr/bin/env Rscript
# trim_existing_models_aggressive.r
# Aggressive script to trim ALL large components from existing model .rds files
# This script comprehensively analyzes and removes large components from models

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 0) {
  cat(
    "Usage: Rscript trim_existing_models_aggressive.r <input_dir> [backup_dir]\n"
  )
  cat("  input_dir: Directory containing .rds model files to process\n")
  cat(
    "  backup_dir: Optional directory to backup original files (default: <input_dir>_backup)\n"
  )
  cat("\nExample:\n")
  cat("  Rscript trim_existing_models_aggressive.r /path/to/models\n")
  cat(
    "  Rscript trim_existing_models_aggressive.r /path/to/models /path/to/backup\n"
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

# Create backup directory
if (!dir.exists(backup_dir)) {
  cat("Creating backup directory:", backup_dir, "\n")
  dir.create(backup_dir, recursive = TRUE, showWarnings = FALSE)
}

cat("\n========================================\n")
cat("AGGRESSIVE Model Trimming Script\n")
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

# Function to recursively analyze object sizes
analyze_object_sizes <- function(
  obj,
  name = "object",
  threshold_mb = 1,
  max_depth = 6,
  current_depth = 0
) {
  if (current_depth >= max_depth) {
    return(NULL)
  }

  size_mb <- as.numeric(object.size(obj)) / (1024^2)
  results <- list()

  if (size_mb >= threshold_mb) {
    results[[name]] <- list(
      size_mb = size_mb,
      type = paste(class(obj), collapse = ","),
      length = if (is.null(length(obj))) 0 else length(obj),
      is_matrix = is.matrix(obj),
      is_list = is.list(obj)
    )

    # Recurse into list elements
    if (is.list(obj) && length(obj) > 0 && !is.matrix(obj)) {
      for (i in seq_along(obj)) {
        element_name <- if (is.null(names(obj))) {
          as.character(i)
        } else {
          names(obj)[i]
        }
        if (is.null(element_name) || element_name == "") {
          element_name <- as.character(i)
        }

        child_name <- paste0(name, "$", element_name)
        child_results <- analyze_object_sizes(
          obj[[i]],
          child_name,
          threshold_mb,
          max_depth,
          current_depth + 1
        )
        if (!is.null(child_results)) {
          results <- c(results, child_results)
        }
      }
    }
  }

  return(results)
}

# Function to safely remove object components by path
safe_remove_component <- function(obj, path_string) {
  # Parse the path (e.g., "fit$fit$predictions" -> c("fit", "fit", "predictions"))
  path_components <- strsplit(path_string, "\\$")[[1]]

  # Navigate to parent and remove the final component
  current_obj <- obj
  parent_path <- list()

  # Navigate to the parent of the target component
  if (length(path_components) > 1) {
    for (i in 1:(length(path_components) - 1)) {
      component <- path_components[i]
      if (component %in% names(current_obj)) {
        parent_path <- c(parent_path, component)
        current_obj <- current_obj[[component]]
      } else {
        return(list(
          obj = obj,
          removed = FALSE,
          size_mb = 0,
          reason = paste(
            "Path not found:",
            paste(path_components[1:i], collapse = "$")
          )
        ))
      }
    }
  }

  # Try to remove the final component
  final_component <- path_components[length(path_components)]
  if (final_component %in% names(current_obj)) {
    size_mb <- as.numeric(object.size(current_obj[[final_component]])) /
      (1024^2)

    # Create a copy and remove the component
    obj_copy <- obj

    # Navigate back to the parent in the copy and remove the component
    if (length(parent_path) == 0) {
      obj_copy[[final_component]] <- NULL
    } else {
      # Build the removal expression dynamically
      removal_expr <- paste0(
        "obj_copy",
        paste0("$", parent_path, collapse = ""),
        "$",
        final_component,
        " <- NULL"
      )
      eval(parse(text = removal_expr))
    }

    return(list(
      obj = obj_copy,
      removed = TRUE,
      size_mb = size_mb,
      reason = "Success"
    ))
  } else {
    return(list(
      obj = obj,
      removed = FALSE,
      size_mb = 0,
      reason = paste("Component not found:", final_component)
    ))
  }
}

# Function to aggressively trim model components
aggressive_trim_model <- function(model_obj, filepath) {
  original_size <- as.numeric(object.size(model_obj)) / (1024^2)

  cat(sprintf(
    "    Analyzing object structure (%.1f MB total)...\n",
    original_size
  ))

  # Analyze all components > 0.5MB
  large_components <- analyze_object_sizes(
    model_obj,
    "obj",
    threshold_mb = 0.5,
    max_depth = 8
  )

  if (length(large_components) > 0) {
    cat("    Large components found:\n")
    # Sort by size descending
    component_sizes <- sapply(large_components, function(x) x$size_mb)
    sorted_indices <- order(component_sizes, decreasing = TRUE)

    for (i in sorted_indices) {
      comp_name <- names(large_components)[i]
      comp_info <- large_components[[comp_name]]
      cat(sprintf(
        "      %s: %.1f MB (%s, len=%d, matrix=%s)\n",
        gsub("^obj\\$", "", comp_name),
        comp_info$size_mb,
        comp_info$type,
        comp_info$length,
        comp_info$is_matrix
      ))
    }
  }

  # Track what components we remove
  removed_components <- c()
  total_removed_size <- 0

  # Define aggressive removal targets - these are safe to remove for prediction
  aggressive_targets <- c(
    # Ranger model out-of-bag predictions (usually the largest!)
    "fit$fit$predictions",
    "model$fit$predictions",
    "model$model$fit$predictions",

    # In-bag counts matrices
    "fit$fit$inbag.counts",
    "model$fit$inbag.counts",
    "model$model$fit$inbag.counts",

    # Variable importance (can be large)
    "fit$fit$variable.importance",
    "model$fit$variable.importance",
    "model$model$fit$variable.importance",

    # Call objects
    "fit$fit$call",
    "model$fit$call",
    "model$model$fit$call",

    # Recipe templates (training data)
    "recipe$template",
    "model$recipe$template",
    "pre$actions$recipe$recipe$template",

    # Preprocessing molds (can be huge)
    "pre$mold",

    # Original levels
    "recipe$orig_lvls",
    "model$recipe$orig_lvls",
    "pre$actions$recipe$recipe$orig_lvls",

    # Other potentially large training artifacts
    "pre$actions$recipe$recipe$training",
    "model$recipe$training",
    "recipe$training"
  )

  # Try to remove each target
  for (target_path in aggressive_targets) {
    result <- safe_remove_component(model_obj, target_path)
    if (result$removed && result$size_mb > 0.1) {
      removed_components <- c(
        removed_components,
        sprintf("%s (%.1f MB)", target_path, result$size_mb)
      )
      total_removed_size <- total_removed_size + result$size_mb
      model_obj <- result$obj
      cat(sprintf("      ✓ Removed %s: %.1f MB\n", target_path, result$size_mb))
    }
  }

  # AGGRESSIVE: Remove any remaining large matrix/data components
  if (length(large_components) > 0) {
    for (comp_name in names(large_components)) {
      comp_info <- large_components[[comp_name]]

      # Skip essential components we must keep for prediction
      essential_patterns <- c(
        "forest",
        "spec",
        "levels",
        "independent.variable.names",
        "class.values",
        "treetype",
        "splitrule"
      )

      if (
        any(sapply(essential_patterns, function(p) {
          grepl(p, comp_name, ignore.case = TRUE)
        }))
      ) {
        next
      }

      # If it's a large matrix or list that looks like training data, remove it
      if (
        comp_info$size_mb > 5 &&
          (comp_info$is_matrix ||
            grepl(
              "template|mold|pred|inbag|importance|call|training|orig",
              comp_name,
              ignore.case = TRUE
            ))
      ) {
        path_string <- gsub("^obj\\$", "", comp_name)
        result <- safe_remove_component(model_obj, path_string)

        if (result$removed) {
          removed_components <- c(
            removed_components,
            sprintf("Large component %s (%.1f MB)", path_string, result$size_mb)
          )
          total_removed_size <- total_removed_size + result$size_mb
          model_obj <- result$obj
          cat(sprintf(
            "      ✓ Aggressively removed %s: %.1f MB\n",
            path_string,
            result$size_mb
          ))
        }
      }
    }
  }

  final_size <- as.numeric(object.size(model_obj)) / (1024^2)
  reduction <- original_size - final_size
  reduction_pct <- if (original_size > 0) {
    (reduction / original_size) * 100
  } else {
    0
  }

  return(list(
    trimmed = length(removed_components) > 0,
    reason = if (length(removed_components) > 0) {
      paste(removed_components, collapse = "; ")
    } else {
      "No removable components found"
    },
    original_size_mb = original_size,
    final_size_mb = final_size,
    reduction_mb = reduction,
    reduction_pct = reduction_pct,
    components_analyzed = length(large_components),
    total_removed_size = total_removed_size,
    removed_count = length(removed_components)
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

      # Aggressively trim the model
      cat("  Analyzing and trimming components...\n")
      trim_result <- aggressive_trim_model(model_obj, filepath)

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
          "  ✓ HEAVILY TRIMMED: %.1f MB → %.1f MB (%.1f%% reduction)\n",
          trim_result$original_size_mb,
          trim_result$final_size_mb,
          trim_result$reduction_pct
        ))
        cat(sprintf(
          "    File size: %.1f MB → %.1f MB\n",
          original_file_size,
          new_file_size
        ))
        cat(sprintf(
          "    Removed %d components totaling %.1f MB\n",
          trim_result$removed_count,
          trim_result$total_removed_size
        ))

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
cat("AGGRESSIVE PROCESSING COMPLETE\n")
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
