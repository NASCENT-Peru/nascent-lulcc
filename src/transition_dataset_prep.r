#' transition_dataset_creation
#'
#' Script for gathering the layers of LULC (dependent variable) for each
#' historic period then separating into viable LULC transitions at the scale of
#' Peru and regions
#'
#' @param config list of configuration parameters
#' @author Ben Black
#' @export

transition_dataset_prep <- function(
  config = get_config(),
  refresh_cache = FALSE,
  mask_raster_path = list.files(
    config[["reg_dir"]],
    pattern = "regions.tif$",
    full.names = TRUE
  ),
  rows_per_block = 1000,
  verify_alignment = FALSE
) {
  ### =========================================================================
  ### A- Preparation
  ### =========================================================================

  # create save dirs
  purrr::walk(
    file.path(
      config[["trans_pre_pred_filter_dir"]],
      config[["data_periods"]]
    ),
    ensure_dir
  )

  ### =========================================================================
  ### B- Load data and dplyr::filter for each time period
  ### =========================================================================

  # The predictor data table is used to identify the file names of variables that
  # are to be included in the stack for each time period

  # Load existing DEM raster and predictor YAML
  pred_yaml_file <- config[["pred_table_path"]]
  pred_table <- yaml::yaml.load_file(pred_yaml_file)

  # Create data frame of LULC file paths for each period and combine with the regions path
  lulc_raster_paths <- data.frame(matrix(ncol = 2, nrow = 4))
  colnames(lulc_raster_paths) <- c("File_name", "Layer_name")
  lulc_raster_paths["File_name"] <- as.data.frame(
    list.files(
      config[["aggregated_lulc_dir"]],
      pattern = ".tif$",
      full.names = TRUE
    )
  )

  # extract everything that begins with / and runs to the end of the string.
  lulc_raster_paths[["Layer_name"]] <-
    lulc_raster_paths[["File_name"]] |>
    stringr::str_extract("(?<=/)[^/]*$") |>
    stringr::str_remove(".tif$")

  # Create regexes for LULC periods
  lulc_period_regexes <- lapply(config[["data_periods"]], function(x) {
    stringr::str_replace(x, pattern = "_", "|")
  })
  names(lulc_period_regexes) <- config[["data_periods"]]

  # filter LULC files by period
  lulc_paths_by_period <- lapply(lulc_period_regexes, function(x) {
    dplyr::filter(lulc_raster_paths, grepl(x, File_name))
  })

  # Change layer names to Initial and Final for easier splitting later
  lulc_paths_by_period <- lapply(lulc_paths_by_period, function(x) {
    x$Layer_name[1] <- "Initial_class"
    x$Layer_name[2] <- "Final_class"
    return(x)
  })

  # NOTE: We no longer add region_path here since we'll use pre-extracted regions
  combined_paths_by_period <- lulc_paths_by_period

  # read in all rasters in the list to check compatibility before stacking
  rasters_by_periods <- purrr::map(combined_paths_by_period, function(x) {
    raster_list <- purrr::map(x$File_name, function(raster_file_name) {
      r <- terra::rast(raster_file_name)
    })
    names(raster_list) <- x$Layer_name
    purrr::compact(raster_list) # drop NULLs
  })
  names(rasters_by_periods) <- config[["data_periods"]]

  ### =========================================================================
  ### C- Confirm Raster compatibility for stacking
  ### =========================================================================

  # Use a function to test rasters in the list against an 'exemplar'
  # which has the extent, crs and resolution that we want
  # in this case the Ref_grid file used for re-sampling some of the predictors.

  purrr::walk(
    rasters_by_periods,
    lulcc.TestRasterCompatibility,
    exemplar_raster = terra::rast(config[["ref_grid_path"]])
  )

  # Create SpatRaster stacks for each time period.
  rasterstacks_by_periods <- mapply(
    function(raster_list, period_name) {
      # Combine layers into a single SpatRaster
      raster_stack_for_period <- terra::rast(raster_list)
    },
    raster_list = rasters_by_periods,
    period_name = names(rasters_by_periods),
    SIMPLIFY = FALSE
  )

  rm(rasters_by_periods)

  ### =========================================================================
  ### C.1- Load valid cell data (SAME SOURCE AS PREDICTORS)
  ### =========================================================================

  # Use the SAME cached valid cell data as the predictor script
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
    stop(
      "Valid cell IDs parquet not found at: ",
      valid_ids_parquet,
      "\nPlease run create_predictor_parquets() first to generate this file."
    )
  }

  # Verify mask alignment is internally consistent
  cat("Verifying mask cell ID consistency...\n")
  verify_mask_cell_ids_transitions(mask_raster_path, valid_cell_data)

  periods <- config[["data_periods"]]
  # Subset to specific period if needed (remove this line to process all)
  periods <- periods[3]

  # Apply function to prepare parquet files for each period
  message("Starting data extraction for each transition period")
  purrr::walk(
    periods,
    process_period_transitions,
    config = config,
    rasterstacks_by_periods = rasterstacks_by_periods,
    valid_cell_data = valid_cell_data,
    mask_raster_path = mask_raster_path,
    verify_alignment = verify_alignment
  )
  message("Preparation of transition datasets complete")
}


#' Verify mask cell IDs are consistent (for transitions)
#'
#' @param mask_raster_path Path to mask raster
#' @param cell_data Either data frame or path to parquet file with cell_id and region columns
#' @param sample_size Number of cells to verify (default: 10000)
verify_mask_cell_ids_transitions <- function(
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

    # Memory-efficient sampling: take random row_idx values first
    set.seed(123) # For reproducibility
    sample_indices <- sort(sample(n_cells, n_verify))

    # Split into chunks to avoid loading too much at once
    chunk_size <- 1000
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


#' Verify LULC raster alignment with mask
#'
#' @param lulc_raster Path to LULC raster (initial or final)
#' @param mask_raster_path Path to mask raster
#' @param cell_data Either data frame or path to parquet file
#' @param sample_size Number of cells to verify
#' @return TRUE if aligned, stops with error if not
verify_lulc_alignment <- function(
  lulc_raster,
  mask_raster_path,
  cell_data,
  sample_size = 1000
) {
  mask <- terra::rast(mask_raster_path)
  lulc <- lulc_raster # Already a SpatRaster object

  # Check basic raster properties
  if (!terra::compareGeom(mask, lulc, stopOnError = FALSE)) {
    # Get detailed comparison
    mask_ext <- terra::ext(mask)
    lulc_ext <- terra::ext(lulc)
    mask_res <- terra::res(mask)
    lulc_res <- terra::res(lulc)
    mask_crs <- terra::crs(mask)
    lulc_crs <- terra::crs(lulc)

    error_msg <- sprintf(
      "\n  Raster geometry mismatch for LULC\n  Mask: extent=%s, res=%s, crs=%s\n  LULC: extent=%s, res=%s, crs=%s",
      paste(as.vector(mask_ext), collapse = ","),
      paste(mask_res, collapse = ","),
      mask_crs,
      paste(as.vector(lulc_ext), collapse = ","),
      paste(lulc_res, collapse = ","),
      lulc_crs
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
  lulc_vals <- lulc[sample_data$cell_id]

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
      "Found %d/%d cell alignment mismatches for LULC raster!",
      mismatches,
      n_verify
    ))
  }

  return(TRUE)
}


#' Efficiently process transitions for one period and write partitioned parquets by region
#' (one row per cell, one column per transition, partitioned by region)
#'
#' @param period Character period label (e.g. "2020_2030")
#' @param rasterstacks_by_periods Named list of terra rasters for each period
#' @param config Named list of configuration parameters
#' @param valid_cell_data Either data frame or path to parquet with cell_id and region columns
#' @param mask_raster_path Path to mask raster for verification
#' @param verify_alignment If TRUE, verify LULC raster alignment
#' @param chunk_size Number of cells to process per chunk (default 1e6)
#'
#' @return NULL (writes partitioned parquet files)
process_period_transitions <- function(
  period,
  rasterstacks_by_periods,
  config,
  valid_cell_data,
  mask_raster_path,
  verify_alignment = FALSE,
  chunk_size = 1e6
) {
  library(terra)
  library(arrow)

  message("\n=== Processing transitions for period: ", period, " ===")

  # ---- Load rasters ----
  r_stack <- rasterstacks_by_periods[[paste(period)]]
  init_r <- r_stack$Initial_class
  fin_r <- r_stack$Final_class

  # ---- Verify alignment ----
  if (verify_alignment) {
    cat("\n  Verifying LULC raster alignment with mask...\n")
    cat("    Checking Initial LULC...")
    verify_lulc_alignment(
      init_r,
      mask_raster_path,
      valid_cell_data,
      sample_size = 1000
    )
    cat(" ✓\n")

    cat("    Checking Final LULC...")
    verify_lulc_alignment(
      fin_r,
      mask_raster_path,
      valid_cell_data,
      sample_size = 1000
    )
    cat(" ✓\n")
    cat("  All LULC rasters verified!\n\n")
  }

  regionalization <- isTRUE(config[["regionalization"]])

  viable_trans_list <- readRDS(config[["viable_transitions_lists"]])[[paste(
    period
  )]]
  trans_names <- viable_trans_list$Trans_name

  # ---- Prepare output ----
  out_dir <- file.path(config[["trans_pre_pred_filter_dir"]], period)
  if (!dir.exists(out_dir)) {
    dir.create(out_dir, recursive = TRUE)
  }

  # Handle both data frame and parquet path inputs
  if (is.character(valid_cell_data)) {
    # It's a path to parquet file - open as dataset
    cell_ds <- arrow::open_dataset(valid_cell_data)
    n_valid <- cell_ds %>%
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
    n_valid <- nrow(valid_cell_data)
    regions <- sort(unique(valid_cell_data$region))
  }

  n_chunks <- ceiling(n_valid / chunk_size)

  message(
    "Valid cells: ",
    format(n_valid, big.mark = ","),
    " (",
    n_chunks,
    " chunks of ",
    format(chunk_size, big.mark = ","),
    ")"
  )
  message("Regions: ", paste(regions, collapse = ", "))
  message("Transitions: ", nrow(viable_trans_list))
  message("→ Writing to: ", out_dir)

  # Initialize writer storage (region -> writer)
  writers <- list()
  sinks <- list()
  schema <- NULL

  # ---- Chunk processing ----
  for (chunk_idx in seq_len(n_chunks)) {
    start_idx <- (chunk_idx - 1) * chunk_size + 1
    end_idx <- min(chunk_idx * chunk_size, n_valid)

    cat(sprintf(
      "\r  Processing chunk %d/%d (%s cells)...     ",
      chunk_idx,
      n_chunks,
      format(end_idx - start_idx + 1, big.mark = ",")
    ))

    # Get chunk rows
    if (is.character(valid_cell_data)) {
      # Use row_idx for efficient dplyr::filtering in Arrow
      chunk_rows <- cell_ds %>%
        dplyr::filter(row_idx >= start_idx & row_idx <= end_idx) %>%
        dplyr::collect()
    } else {
      chunk_rows <- valid_cell_data[start_idx:end_idx, , drop = FALSE]
    }

    # Extract raster values using pre-identified cell IDs
    chunk_ids <- chunk_rows$cell_id
    init_vals <- init_r[chunk_ids]
    fin_vals <- fin_r[chunk_ids]

    # Handle data frame vs vector returns from terra
    if (is.data.frame(init_vals) || is.list(init_vals)) {
      init_vals <- init_vals[[1]]
    }
    if (is.data.frame(fin_vals) || is.list(fin_vals)) {
      fin_vals <- fin_vals[[1]]
    }

    # Use pre-extracted region values from valid_cell_data
    region_vals <- chunk_rows$region

    # ---- Build transition columns ----
    trans_mat <- matrix(
      NA_integer_,
      nrow = length(chunk_ids),
      ncol = nrow(viable_trans_list)
    )
    colnames(trans_mat) <- trans_names

    for (i in seq_len(nrow(viable_trans_list))) {
      from_class <- viable_trans_list$From.[i]
      to_class <- viable_trans_list$To.[i]

      # Transition logic:
      #   1 → cell was from_class and changed to to_class
      #   0 → cell was from_class but did NOT change to to_class
      #   NA → cell was not from_class at all
      trans_mat[, i] <- ifelse(
        init_vals == from_class,
        ifelse(fin_vals == to_class, 1L, 0L),
        NA_integer_
      )
    }

    # ---- Assemble chunk ----
    chunk_df <- data.frame(
      cell_id = as.numeric(chunk_ids),
      region = as.integer(region_vals),
      stringsAsFactors = FALSE
    )
    chunk_df <- cbind(chunk_df, as.data.frame(trans_mat))

    # ---- Split by region and write to partitions ----
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
        region_dir <- file.path(out_dir, sprintf("region=%d", region))
        ensure_dir(region_dir)

        # Create output file path
        region_file <- file.path(
          region_dir,
          paste0("transitions_", period, ".parquet")
        )

        # Create writer
        sinks[[as.character(region)]] <- arrow::FileOutputStream$create(
          region_file
        )
        writers[[as.character(region)]] <- arrow::ParquetFileWriter$create(
          schema = schema,
          sink = sinks[[as.character(region)]],
          properties = arrow::ParquetWriterProperties$create(
            names(schema),
            compression = "zstd",
            compression_level = 9
          )
        )
      }

      # Write region chunk to appropriate partition
      writers[[as.character(region)]]$WriteTable(
        arrow::Table$create(region_chunk),
        chunk_size = nrow(region_chunk)
      )

      rm(region_chunk)
    }

    rm(chunk_df, trans_mat, init_vals, fin_vals, chunk_rows)
    gc(verbose = FALSE)
  }

  cat("\n")

  # ---- Close writers ----
  cat("  Closing writers...\n")
  for (region in names(writers)) {
    writers[[region]]$Close()
    sinks[[region]]$close()
  }

  message("✓ Transition parquet writing complete.")
  message(sprintf("  Location: %s", out_dir))
  message(sprintf("  Partitions: %d regions", length(regions)))
  message("=== Completed all transitions for period ", period, " ===\n")

  # ---- Sanity check ----
  message("Performing sanity check on written files...")
  parquet_check <- sanity_check_transitions_partitioned(
    transitions_dir = out_dir,
    regions_json_path = if (regionalization) {
      file.path(config[["reg_dir"]], "regions.json")
    } else {
      NULL
    }
  )

  # save results
  sanity_output_path <- file.path(
    out_dir,
    paste0("check_transitions_", period, ".csv")
  )
  readr::write_csv(parquet_check, sanity_output_path)
  message("✓ Sanity check complete\n")
}


#' Sanity check for partitioned transition parquet files
#'
#' @param transitions_dir Directory containing partitioned transition files
#' @param regions_json_path Optional path to regions.json file
#' @return Data frame with sanity check results
sanity_check_transitions_partitioned <- function(
  transitions_dir,
  regions_json_path = NULL
) {
  library(arrow)

  # Open the partitioned dataset
  ds <- arrow::open_dataset(
    transitions_dir,
    partitioning = arrow::hive_partition(region = arrow::int32())
  )

  # Collect basic statistics
  results <- list()

  # Count total rows
  results$total_rows <- ds |>
    dplyr::count() |>
    dplyr::collect() |>
    dplyr::pull(n)

  # Count rows per region
  results$rows_per_region <- ds |>
    dplyr::group_by(region) |>
    dplyr::count() |>
    dplyr::collect()

  # Check for duplicate cell_ids within regions
  duplicates <- ds |>
    dplyr::group_by(region, cell_id) |>
    dplyr::count() |>
    dplyr::filter(n > 1) |>
    dplyr::collect()

  results$has_duplicates <- nrow(duplicates) > 0
  results$n_duplicates <- nrow(duplicates)

  # Get transition column names (exclude cell_id and region)
  schema_names <- names(ds$schema)
  trans_cols <- setdiff(schema_names, c("cell_id", "region"))

  results$n_transition_cols <- length(trans_cols)

  # Check for NAs in transition columns (sample to avoid memory issues)
  # For large datasets, we'll sample 1 million rows
  total_rows <- results$total_rows
  if (total_rows > 1e6) {
    cat("  Sampling 1M rows for NA check (dataset is large)...\n")
    na_counts <- ds |>
      dplyr::select(dplyr::all_of(trans_cols)) |>
      head(1e6) |>
      dplyr::summarise(dplyr::across(dplyr::everything(), ~ sum(is.na(.)))) |>
      dplyr::collect()
  } else {
    na_counts <- ds |>
      dplyr::select(dplyr::all_of(trans_cols)) |>
      dplyr::summarise(dplyr::across(dplyr::everything(), ~ sum(is.na(.)))) |>
      dplyr::collect()
  }

  results$na_summary <- data.frame(
    transition = names(na_counts),
    n_na = as.numeric(na_counts[1, ]),
    stringsAsFactors = FALSE
  )

  # Create summary data frame
  summary_df <- data.frame(
    metric = c(
      "Total rows",
      "Number of regions",
      "Has duplicates",
      "Number of duplicates",
      "Number of transitions"
    ),
    value = c(
      results$total_rows,
      nrow(results$rows_per_region),
      results$has_duplicates,
      results$n_duplicates,
      results$n_transition_cols
    ),
    stringsAsFactors = FALSE
  )

  return(summary_df)
}
