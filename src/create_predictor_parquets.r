#' Create partitioned predictor parquets for large sparse rasters (streaming approach)
#' @param config Project configuration
#' @param refresh_cache If TRUE, re-extract valid cell IDs from mask
#' @param mask_raster_path Path to mask raster
#' @param output_dir Output directory for parquet files
#' @param rows_per_block Number of raster rows to read per block
#' @param chunk_size Number of cells to process per write operation
#' @param verify_alignment If TRUE, verify predictor alignment with mask (recommended for first run)
#' @return List with valid cell IDs, number of cells, and time periods
create_predictor_parquets <- function(
  config = get_config(),
  refresh_cache = FALSE,
  mask_raster_path = list.files(
    config[["reg_dir"]],
    pattern = "regions.tif$",
    full.names = TRUE
  ),
  output_dir = file.path(config[["predictors_prepped_dir"]], "parquet_data"),
  rows_per_block = 1000,
  chunk_size = 2e6,
  verify_alignment = FALSE
) {
  pred_yaml_file <- config[["pred_table_path"]]
  pred_config <- yaml::yaml.load_file(pred_yaml_file)

  # Create directory structure
  ensure_dir(output_dir)
  ensure_dir(file.path(output_dir, "static"))
  ensure_dir(file.path(output_dir, "dynamic"))

  message("Step 1: Extracting valid cell IDs with region mapping\n")

  # Check if valid_cell_ids.rds exists
  valid_ids_parquet <- file.path(
    config[["data_basepath"]],
    "spatial_reference_grid",
    "valid_cell_ids_regions.parquet"
  )

  if (file.exists(valid_ids_parquet) && !refresh_cache) {
    cat("✓ Found existing valid_cell_ids.parquet, using...\n")
    valid_cell_data <- valid_ids_parquet # Store path, not data

    # Get summary stats without loading into memory
    ds <- arrow::open_dataset(valid_cell_data)
    n_cells <- ds %>% dplyr::count() %>% dplyr::collect() %>% dplyr::pull(n)
    n_regions <- ds %>%
      dplyr::distinct(region) %>%
      dplyr::count() %>%
      dplyr::collect() %>%
      dplyr::pull(n)

    cat(sprintf(
      "\n✓ Found %s valid cells across %d regions\n\n",
      format(n_cells, big.mark = ","),
      n_regions
    ))
  } else {
    cat("✓ Extracting valid cells with region values from mask raster...\n")
    valid_cell_data <- extract_valid_cells_with_region(
      mask_raster_path,
      rows_per_block,
      output_path = valid_ids_parquet,
      keep_in_memory = FALSE # Keep on disk to save memory
    )
    cat(sprintf(
      "\n✓ Results saved to: %s\n\n",
      basename(valid_cell_data)
    ))
  }

  # Verify mask alignment is internally consistent
  cat("Verifying mask cell ID consistency...\n")
  verify_mask_cell_ids(mask_raster_path, valid_cell_data)

  # Process static variables (PARTITIONED BY REGION)
  cat("Step 2: Processing static predictors (partitioned by region)\n")

  static_rasters <- extract_static_variables(pred_config)

  # Temporarily remove problematic variables
  static_rasters[["dist_to_airports"]] <- NULL
  static_rasters[["aspect"]] <- NULL
  static_rasters[["slope"]] <- NULL

  start_time <- Sys.time()
  if (length(static_rasters) > 0) {
    write_static_parquet_streaming_partitioned(
      cell_data = valid_cell_data,
      static_rasters = static_rasters,
      mask_raster_path = mask_raster_path,
      output_dir = file.path(output_dir, "static"),
      chunk_size = chunk_size,
      verify_alignment = verify_alignment
    )
  } else {
    cat("\nNo static variables found\n")
  }
  end_time <- Sys.time()
  cat(sprintf(
    "\nStatic processing time: %.2f minutes\n\n",
    as.numeric(difftime(end_time, start_time, units = "mins"))
  ))

  # Process dynamic variables (PARTITIONED BY SCENARIO AND REGION)
  cat("Step 3: Creating dynamic predictors by time period (partitioned)\n")

  period_structure <- reorganize_by_period(pred_config = pred_config)

  # Subset to only 2018 (remove to process all periods)
  period_structure <- period_structure["2018_2022"]

  time_periods <- names(period_structure)

  cat(sprintf(
    "\nFound %d time periods: %s\n\n",
    length(time_periods),
    paste(time_periods, collapse = ", ")
  ))

  # Process each time period
  for (period in time_periods) {
    cat(sprintf("Processing time period: %s\n", period))

    output_subdir <- file.path(output_dir, "dynamic", period)
    ensure_dir(output_subdir)

    write_dynamic_parquet_streaming_partitioned(
      cell_data = valid_cell_data,
      period_data = period_structure[[period]],
      mask_raster_path = mask_raster_path,
      output_dir = output_subdir,
      chunk_size = chunk_size,
      verify_alignment = verify_alignment
    )

    cat("\n")
  }

  message("Step 4: Creating metadata file\n")
  # Create metadata
  create_metadata(pred_config, output_dir, time_periods)

  list_output_files(output_dir)

  message("\nStep 5: Computing NA counts for each dataset\n")
  # checking for NAs across datasets
  ds_static <- arrow::open_dataset(file.path(output_dir, "static"))
  na_static <- compute_na_counts_streaming(ds_static, "Static predictors")

  # save
  saveRDS(
    na_static,
    file.path(output_dir, "static", "na_counts_static_predictors.rds")
  )

  # loop over periods
  for (period in time_periods) {
    ds_dynamic <- arrow::open_dataset(
      file.path(
        config[["predictors_prepped_dir"]],
        "parquet_data",
        "dynamic",
        period
      ),
      partitioning = hive_partition(region = int32(), scenario = utf8())
    )
    na_dynamic <- compute_na_counts_streaming(ds_dynamic, "Dynamic predictors")
    # save
    saveRDS(
      na_dynamic,
      file.path(
        output_dir,
        "dynamic",
        period,
        sprintf("na_counts_dynamic_predictors_%s.rds", period)
      )
    )
  }

  message("\nAll steps completed successfully.\n")

  # Prepare return value
  if (is.character(valid_cell_data)) {
    # It's a parquet path
    ds <- arrow::open_dataset(valid_cell_data)
    n_cells_final <- ds %>%
      dplyr::count() %>%
      dplyr::collect() %>%
      dplyr::pull(n)
  } else {
    # It's a data frame
    n_cells_final <- nrow(valid_cell_data)
  }

  return(invisible(list(
    valid_cell_data = valid_cell_data,
    n_cells = n_cells_final,
    time_periods = time_periods
  )))
}


#' Verify mask cell IDs are consistent
#'
#' @param mask_raster_path Path to mask raster
#' @param cell_data Either data frame or path to parquet file with cell_id and region columns
#' @param sample_size Number of cells to verify (default: 10000)
verify_mask_cell_ids <- function(
  mask_raster_path,
  cell_data,
  sample_size = 10000
) {
  mask <- terra::rast(mask_raster_path)

  # Handle both data frame and parquet path inputs
  if (is.character(cell_data)) {
    # It's a path to parquet file
    ds <- arrow::open_dataset(cell_data)
    n_cells <- ds %>% dplyr::count() %>% dplyr::collect() %>% dplyr::pull(n)

    # Sample cells to verify
    n_verify <- min(sample_size, n_cells)

    cat(sprintf(
      "  Verifying %s sampled cell IDs...\n",
      format(n_verify, big.mark = ",")
    ))

    # Sample from the dataset
    sample_data <- ds %>%
      dplyr::slice_sample(n = n_verify) %>%
      dplyr::collect()
  } else {
    # It's a data frame
    n_cells <- nrow(cell_data)
    n_verify <- min(sample_size, n_cells)

    cat(sprintf(
      "  Verifying %s sampled cell IDs...\n",
      format(n_verify, big.mark = ",")
    ))

    verify_idx <- sample(n_cells, n_verify)
    sample_data <- cell_data[verify_idx, ]
  }

  mismatches <- 0
  for (i in seq_len(nrow(sample_data))) {
    cell_id <- sample_data$cell_id[i]
    expected_region <- sample_data$region[i]

    # Extract using cell ID
    actual_region <- mask[cell_id][1, 1]

    if (
      !isTRUE(all.equal(as.numeric(expected_region), as.numeric(actual_region)))
    ) {
      mismatches <- mismatches + 1
      if (mismatches <= 5) {
        # Show first 5 mismatches
        warning(sprintf(
          "Cell ID mismatch at cell_id=%d: expected region %d, got %s",
          cell_id,
          expected_region,
          actual_region
        ))
      }
    }
  }

  if (mismatches > 0) {
    stop(sprintf(
      "Found %d/%d cell ID mismatches! Mask cell IDs are not consistent.",
      mismatches,
      n_verify
    ))
  }

  cat(sprintf(
    "  ✓ All %s sampled cells verified successfully\n\n",
    format(n_verify, big.mark = ",")
  ))
}


#' Verify predictor raster alignment with mask
#'
#' @param predictor_path Path to predictor raster
#' @param mask_raster_path Path to mask raster
#' @param cell_data Either data frame or path to parquet file with cell_id, row, col columns
#' @param sample_size Number of cells to verify
#' @return TRUE if aligned, stops with error if not
verify_predictor_alignment <- function(
  predictor_path,
  mask_raster_path,
  cell_data,
  sample_size = 1000
) {
  mask <- terra::rast(mask_raster_path)
  pred <- terra::rast(predictor_path)

  # Check basic raster properties
  if (!terra::compareGeom(mask, pred, stopOnError = FALSE)) {
    # Get detailed comparison
    mask_ext <- terra::ext(mask)
    pred_ext <- terra::ext(pred)
    mask_res <- terra::res(mask)
    pred_res <- terra::res(pred)
    mask_crs <- terra::crs(mask)
    pred_crs <- terra::crs(pred)

    error_msg <- sprintf(
      "\n  Raster geometry mismatch for: %s\n  Mask: extent=%s, res=%s, crs=%s\n  Pred: extent=%s, res=%s, crs=%s",
      basename(predictor_path),
      paste(as.vector(mask_ext), collapse = ","),
      paste(mask_res, collapse = ","),
      mask_crs,
      paste(as.vector(pred_ext), collapse = ","),
      paste(pred_res, collapse = ","),
      pred_crs
    )
    stop(error_msg)
  }

  # Handle both data frame and parquet path inputs
  if (is.character(cell_data)) {
    # It's a path to parquet file
    ds <- arrow::open_dataset(cell_data)
    n_cells <- ds %>% dplyr::count() %>% dplyr::collect() %>% dplyr::pull(n)
    n_verify <- min(sample_size, n_cells)

    # Memory-efficient sampling: take random row_idx values first
    # then filter to just those rows
    set.seed(123) # For reproducibility
    sample_indices <- sort(sample(n_cells, n_verify))

    # Split into chunks to avoid loading too much at once
    chunk_size <- 100
    n_chunks <- ceiling(n_verify / chunk_size)

    sample_data_list <- list()
    for (i in seq_len(n_chunks)) {
      start_idx <- (i - 1) * chunk_size + 1
      end_idx <- min(i * chunk_size, n_verify)
      chunk_indices <- sample_indices[start_idx:end_idx]

      chunk <- ds %>%
        dplyr::filter(row_idx %in% chunk_indices) %>%
        dplyr::select(cell_id, region) %>%
        dplyr::collect()

      sample_data_list[[i]] <- chunk
    }

    sample_data <- dplyr::bind_rows(sample_data_list)
  } else {
    # It's a data frame
    n_cells <- nrow(cell_data)
    n_verify <- min(sample_size, n_cells)
    verify_idx <- sample(n_cells, n_verify)
    sample_data <- cell_data[verify_idx, c("cell_id", "region")]
  }

  # Extract from both using the SAME cell IDs
  mask_vals <- mask[sample_data$cell_id]
  pred_vals <- pred[sample_data$cell_id]

  # Check that mask values match what we expect (should all be valid regions)
  mask_expected <- sample_data$region

  mismatches <- 0
  for (i in seq_len(nrow(sample_data))) {
    if (
      !is.na(mask_vals[i, 1]) &&
        !isTRUE(all.equal(
          as.numeric(mask_vals[i, 1]),
          as.numeric(mask_expected[i])
        ))
    ) {
      mismatches <- mismatches + 1
      if (mismatches <= 3) {
        warning(sprintf(
          "  Cell alignment issue at cell_id=%d: mask returned %s, expected %d",
          sample_data$cell_id[i],
          mask_vals[i, 1],
          mask_expected[i]
        ))
      }
    }
  }

  if (mismatches > 0) {
    stop(sprintf(
      "Found %d/%d cell alignment mismatches for %s!",
      mismatches,
      n_verify,
      basename(predictor_path)
    ))
  }

  return(TRUE)
}


#' Write static predictors with streaming and partitioning by region
#' @param cell_data Either data frame or path to parquet file with cell_id, row, col, and region columns
#' @param static_rasters Named list of static raster file paths
#' @param mask_raster_path Path to mask raster for verification
#' @param output_dir Output directory for partitioned dataset
#' @param chunk_size Number of cells to process per write operation
#' @param verify_alignment If TRUE, verify each predictor's alignment
#' @return NULL
write_static_parquet_streaming_partitioned <- function(
  cell_data,
  static_rasters,
  mask_raster_path,
  output_dir,
  chunk_size,
  verify_alignment = TRUE
) {
  library(arrow)

  # Handle both data frame and parquet path inputs
  if (is.character(cell_data)) {
    # It's a path to parquet file - open as dataset
    cell_ds <- arrow::open_dataset(cell_data)
    n_cells <- cell_ds %>%
      dplyr::count() %>%
      dplyr::collect() %>%
      dplyr::pull(n)
    regions <- cell_ds %>%
      dplyr::distinct(region) %>%
      dplyr::collect() %>%
      dplyr::pull(region) %>%
      sort()
  } else {
    # It's a data frame
    n_cells <- nrow(cell_data)
    regions <- sort(unique(cell_data$region))
  }

  n_chunks <- ceiling(n_cells / chunk_size)

  cat(sprintf(
    "\nWriting %d variables in %d chunks (streaming, partitioned by region)...\n",
    length(static_rasters),
    n_chunks
  ))

  cat(sprintf(
    "  Found %d regions: %s\n",
    length(regions),
    paste(regions, collapse = ", ")
  ))

  # Verify alignment of each predictor (sample of cells)
  if (verify_alignment) {
    cat("\n  Verifying predictor alignment with mask...\n")
    for (var_name in names(static_rasters)) {
      cat(sprintf("    Checking: %s...", var_name))
      verify_predictor_alignment(
        static_rasters[[var_name]],
        mask_raster_path,
        cell_data,
        sample_size = 1000
      )
      cat(" ✓\n")
    }
    cat("  All predictors verified!\n\n")
  }

  # Initialize writer storage
  writers <- list()
  sinks <- list()
  schema <- NULL

  # Process chunks
  for (chunk_idx in seq_len(n_chunks)) {
    start_idx <- (chunk_idx - 1) * chunk_size + 1
    end_idx <- min(chunk_idx * chunk_size, n_cells)

    cat(sprintf(
      "\r  Chunk %d/%d (%s cells)     ",
      chunk_idx,
      n_chunks,
      format(end_idx - start_idx + 1, big.mark = ",")
    ))

    # Get chunk rows
    if (is.character(cell_data)) {
      # Use row_idx for efficient filtering in Arrow
      chunk_rows <- cell_ds %>%
        dplyr::filter(row_idx >= start_idx & row_idx <= end_idx) %>%
        dplyr::collect()
    } else {
      chunk_rows <- cell_data[start_idx:end_idx, , drop = FALSE]
    }

    # Build chunk data frame with region
    chunk_df <- data.frame(
      cell_id = as.numeric(chunk_rows$cell_id),
      region = as.integer(chunk_rows$region),
      stringsAsFactors = FALSE
    )

    # Add static variables using consistent cell IDs
    for (var_name in names(static_rasters)) {
      r <- terra::rast(static_rasters[[var_name]])

      # Use cell IDs directly - they're guaranteed to be from terra's system
      vals <- r[chunk_rows$cell_id]

      if (is.data.frame(vals) || is.list(vals)) {
        vals <- vals[[1]]
      }

      chunk_df[[var_name]] <- vals
    }

    # Split chunk by region and write to appropriate partition
    for (region in unique(chunk_df$region)) {
      region_chunk <- chunk_df[chunk_df$region == region, , drop = FALSE]

      # Create writer for this region on first encounter
      if (is.null(writers[[as.character(region)]])) {
        # Create schema from first chunk
        if (is.null(schema)) {
          tbl <- arrow::Table$create(region_chunk)
          schema <- tbl$schema
        }

        # Create partition directory
        region_dir <- file.path(output_dir, sprintf("region=%d", region))
        ensure_dir(region_dir)

        # Create output file path
        region_file <- file.path(region_dir, "static_predictors.parquet")

        # Create writer
        sinks[[as.character(region)]] <- arrow::FileOutputStream$create(
          region_file
        )
        writers[[as.character(region)]] <- ParquetFileWriter$create(
          schema = schema,
          sink = sinks[[as.character(region)]],
          properties = ParquetWriterProperties$create(
            names(schema),
            compression = "zstd",
            compression_level = 9
          )
        )
      }

      # Write region chunk
      writers[[as.character(region)]]$WriteTable(
        arrow::Table$create(region_chunk),
        chunk_size = nrow(region_chunk)
      )

      rm(region_chunk)
    }

    rm(chunk_df, chunk_rows)
    gc(verbose = FALSE)
  }

  cat("\n  Closing writers...\n")

  # Close all writers
  for (region in names(writers)) {
    writers[[region]]$Close()
    sinks[[region]]$close()
  }

  cat("✓ Static partitioned dataset created successfully\n")
  cat(sprintf("  Location: %s\n", output_dir))
  cat(sprintf("  Partitions: %d regions\n", length(regions)))
}


#' Write dynamic predictors with streaming and partitioning by scenario and region
#' @param cell_data Either data frame or path to parquet file with cell_id, row, col, and region columns
#' @param period_data Named list of scenarios, each containing named list of variable raster paths
#' @param mask_raster_path Path to mask raster for verification
#' @param output_dir Output directory for partitioned dataset
#' @param chunk_size Number of cells to process per write operation
#' @param verify_alignment If TRUE, verify each predictor's alignment
write_dynamic_parquet_streaming_partitioned <- function(
  cell_data,
  period_data,
  mask_raster_path,
  output_dir,
  chunk_size,
  verify_alignment = TRUE
) {
  library(arrow)

  # Handle both data frame and parquet path inputs
  if (is.character(cell_data)) {
    # It's a path to parquet file - open as dataset
    cell_ds <- arrow::open_dataset(cell_data)
    n_cells <- cell_ds %>%
      dplyr::count() %>%
      dplyr::collect() %>%
      dplyr::pull(n)
    regions <- cell_ds %>%
      dplyr::distinct(region) %>%
      dplyr::collect() %>%
      dplyr::pull(region) %>%
      sort()
  } else {
    # It's a data frame
    n_cells <- nrow(cell_data)
    regions <- sort(unique(cell_data$region))
  }

  n_chunks <- ceiling(n_cells / chunk_size)
  scenarios <- names(period_data)

  cat(sprintf(
    "  %d scenarios: %s\n",
    length(scenarios),
    paste(scenarios, collapse = ", ")
  ))
  cat(sprintf(
    "  %d regions: %s\n",
    length(regions),
    paste(regions, collapse = ", ")
  ))

  # Verify alignment for first scenario (assume all scenarios have same alignment)
  if (verify_alignment && length(scenarios) > 0) {
    cat("\n  Verifying predictor alignment with mask (first scenario)...\n")
    first_scenario <- scenarios[1]
    for (var_name in names(period_data[[first_scenario]])) {
      cat(sprintf("    Checking: %s...", var_name))
      verify_predictor_alignment(
        period_data[[first_scenario]][[var_name]],
        mask_raster_path,
        cell_data,
        sample_size = 1000
      )
      cat(" ✓\n")
    }
    cat("  All predictors verified!\n\n")
  }

  # Initialize writer storage (scenario -> region -> writer)
  writers <- list()
  sinks <- list()
  schema <- NULL

  for (scenario in scenarios) {
    cat(sprintf("  Scenario: %s\n", scenario))

    var_names <- names(period_data[[scenario]])
    cat(sprintf(
      "    %d variables: %s\n",
      length(var_names),
      paste(var_names, collapse = ", ")
    ))

    for (chunk_idx in seq_len(n_chunks)) {
      start_idx <- (chunk_idx - 1) * chunk_size + 1
      end_idx <- min(chunk_idx * chunk_size, n_cells)

      cat(sprintf("\r    Chunk %d/%d     ", chunk_idx, n_chunks))

      # Get chunk rows
      if (is.character(cell_data)) {
        # Use row_idx for efficient filtering in Arrow
        chunk_rows <- cell_ds %>%
          dplyr::filter(row_idx >= start_idx & row_idx <= end_idx) %>%
          dplyr::collect()
      } else {
        chunk_rows <- cell_data[start_idx:end_idx, , drop = FALSE]
      }

      # Build chunk for this scenario
      chunk_df <- data.frame(
        cell_id = as.numeric(chunk_rows$cell_id),
        scenario = scenario,
        region = as.integer(chunk_rows$region),
        stringsAsFactors = FALSE
      )

      # Add dynamic variables using consistent cell IDs
      for (var_name in var_names) {
        r <- terra::rast(period_data[[scenario]][[var_name]])

        # Use cell IDs directly
        vals <- r[chunk_rows$cell_id]

        if (is.data.frame(vals) || is.list(vals)) {
          vals <- vals[[1]]
        }
        chunk_df[[var_name]] <- vals
      }

      # Split chunk by region and write to appropriate partition
      for (region in unique(chunk_df$region)) {
        region_chunk <- chunk_df[chunk_df$region == region, , drop = FALSE]

        # Create unique key for this scenario-region combination
        writer_key <- sprintf("%s_%d", scenario, region)

        # Create writer for this scenario-region combination on first encounter
        if (is.null(writers[[writer_key]])) {
          # Create schema from first chunk
          if (is.null(schema)) {
            tbl <- arrow::Table$create(region_chunk)
            schema <- tbl$schema
          }

          # Create nested partition directory (scenario=X/region=Y)
          scenario_dir <- file.path(
            output_dir,
            sprintf("scenario=%s", scenario)
          )
          region_dir <- file.path(scenario_dir, sprintf("region=%d", region))
          ensure_dir(region_dir)

          # Create output file path
          partition_file <- file.path(region_dir, "dynamic_predictors.parquet")

          # Create writer
          sinks[[writer_key]] <- arrow::FileOutputStream$create(partition_file)
          writers[[writer_key]] <- ParquetFileWriter$create(
            schema = schema,
            sink = sinks[[writer_key]],
            properties = ParquetWriterProperties$create(
              names(schema),
              compression = "zstd",
              compression_level = 9
            )
          )
        }

        # Write region chunk to appropriate partition
        writers[[writer_key]]$WriteTable(
          arrow::Table$create(region_chunk),
          chunk_size = nrow(region_chunk)
        )

        rm(region_chunk)
      }

      rm(chunk_df, chunk_rows)
      gc(verbose = FALSE)
    }
    cat("\n")
  }

  cat("  Closing writers...\n")

  # Close all writers
  for (writer_key in names(writers)) {
    writers[[writer_key]]$Close()
    sinks[[writer_key]]$close()
  }

  cat(sprintf(
    "  ✓ Dynamic partitioned dataset created\n"
  ))
  cat(sprintf("  Location: %s\n", output_dir))
  cat(sprintf(
    "  Partitions: %d scenarios × %d regions = %d total\n",
    length(scenarios),
    length(regions),
    length(scenarios) * length(regions)
  ))
}


#' Reorganize dynamic config from flat variable list to period->scenario->variable
#' @param pred_config Predictor configuration list
#' @return Nested list organized by period and scenario
reorganize_by_period <- function(pred_config) {
  period_structure <- list()

  for (var_key in names(pred_config)) {
    var_config <- pred_config[[var_key]]

    if (var_config$static_or_dynamic == "static") {
      next
    }

    period <- as.character(var_config$period)
    scenario <- if (
      is.null(var_config$scenario_variant) ||
        is.na(var_config$scenario_variant)
    ) {
      "baseline"
    } else {
      as.character(var_config$scenario_variant)
    }

    raster_path <- file.path(config[["data_basepath"]], var_config$path)

    var_name <- if (
      !is.null(var_config$base_name) && !is.na(var_config$base_name)
    ) {
      var_config$base_name
    } else {
      var_key
    }

    if (is.null(period_structure[[period]])) {
      period_structure[[period]] <- list()
    }
    if (is.null(period_structure[[period]][[scenario]])) {
      period_structure[[period]][[scenario]] <- list()
    }

    period_structure[[period]][[scenario]][[var_name]] <- raster_path
  }

  return(period_structure)
}


#' Extract static variables from flat config
#' @param pred_config Predictor configuration list
#' @return Named list of static variable paths
extract_static_variables <- function(pred_config) {
  static_rasters <- list()

  for (var_key in names(pred_config)) {
    var_config <- pred_config[[var_key]]

    if (var_config$static_or_dynamic == "static") {
      var_name <- if (
        !is.null(var_config$base_name) && !is.na(var_config$base_name)
      ) {
        var_config$base_name
      } else {
        var_key
      }

      if (!is.null(static_rasters[[var_name]])) {
        stop(sprintf(
          "Duplicate base_name '%s' found for static variables. Check YAML entries: %s",
          var_name,
          var_key
        ))
      }

      if (
        !is.null(var_config$grouping) &&
          var_config$grouping == "soil"
      ) {
        if (!grepl("100-200cm", var_name)) {
          next
        }
      }

      if (is.null(var_config$path) || is.na(var_config$path)) {
        next
      }

      rel_path <- file.path(config[["data_basepath"]], var_config$path)
      static_rasters[[var_name]] <- rel_path
    }
  }

  return(static_rasters)
}


#' Create metadata parquet file
#' @param pred_config Predictor configuration list
#' @param output_dir Output directory
#' @param time_periods Vector of time period names
create_metadata <- function(
  pred_config,
  output_dir,
  time_periods
) {
  static_rasters <- extract_static_variables(pred_config)

  if (length(static_rasters) > 0) {
    static_meta <- data.frame(
      file_name = "static (partitioned by region)",
      type = "static",
      period = NA_character_,
      n_variables = length(static_rasters),
      variables = paste(names(static_rasters), collapse = ","),
      partitioning = "region",
      stringsAsFactors = FALSE
    )
  } else {
    static_meta <- data.frame()
  }

  if (length(time_periods) > 0) {
    dynamic_meta <- data.frame(
      file_name = file.path("dynamic", time_periods, "(partitioned)"),
      type = "dynamic",
      period = time_periods,
      n_variables = NA_integer_,
      variables = NA_character_,
      partitioning = "scenario, region",
      stringsAsFactors = FALSE
    )

    period_structure <- reorganize_by_period(pred_config)

    for (i in seq_along(time_periods)) {
      period <- time_periods[i]
      first_scenario <- period_structure[[period]][[names(period_structure[[
        period
      ]])[1]]]

      dynamic_meta$n_variables[i] <- length(first_scenario)
      dynamic_meta$variables[i] <- paste(names(first_scenario), collapse = ",")
    }

    metadata <- rbind(static_meta, dynamic_meta)
  } else {
    metadata <- static_meta
  }

  arrow::write_parquet(metadata, file.path(output_dir, "metadata.parquet"))

  cat("\n✓ Metadata created\n")
}


#' List all output files with sizes
#' @param output_dir Output directory path
list_output_files <- function(output_dir) {
  cat("\nCreated files:\n")
  files <- list.files(output_dir, recursive = TRUE, full.names = TRUE)
  files <- files[!dir.exists(files)] # Exclude directories

  total_size <- 0
  for (f in files) {
    size_mb <- file.info(f)$size / 1024^2
    total_size <- total_size + size_mb
    cat(sprintf(
      "  %s (%.1f MB)\n",
      gsub(paste0(output_dir, "/"), "", f),
      size_mb
    ))
  }

  cat(sprintf("\nTotal size: %.1f MB\n", total_size))
}


#' Compute NA counts for each column in an Arrow dataset (streaming)
#' @param ds Arrow dataset
#' @param dataset_name Name of dataset (for messages)
#' @param sample_cols Optional vector of column names to restrict analysis
#' @return Data frame with NA counts and proportions per column
compute_na_counts_streaming <- function(
  ds,
  dataset_name = "dataset",
  sample_cols = NULL
) {
  message(sprintf("\n[INFO] Computing NA counts for %s...", dataset_name))

  # List all columns in dataset
  all_cols <- ds$schema$names

  # Optionally restrict to a subset (e.g. from collinearity results)
  if (!is.null(sample_cols)) {
    all_cols <- dplyr::intersect(all_cols, sample_cols)
  }

  results <- purrr::map_dfr(all_cols, function(colname) {
    message(sprintf("  -> Checking column: %s", colname))

    # Summarise missing values for this column
    na_summary <- ds %>%
      dplyr::summarise(
        na_count = sum(is.na(!!rlang::sym(colname))),
        total_rows = n()
      ) %>%
      dplyr::collect()

    tibble::tibble(
      column = colname,
      na_count = na_summary$na_count,
      total_rows = na_summary$total_rows,
      na_proportion = na_summary$na_count / na_summary$total_rows
    )
  })

  message("[INFO] NA counting complete.")
  results %>% dplyr::arrange(desc(na_proportion))
}
