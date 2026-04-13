#' Transition identification
#'
#' Transition_identification: Using land use data from calibration (historic) periods to identify
#' LULC transitions to be modeled in future simulations
#'
#' @author Ben Black

transition_identification <- function(
  config = get_config()
) {
  message("Starting transition identification")

  ### =========================================================================
  ### A- Preparation
  ### =========================================================================

  lulc_files <- fs::dir_ls(
    config[["aggregated_lulc_dir"]],
    glob = "*.tif",
    recurse = TRUE
  )
  message(sprintf("Found %d LULC raster files", length(lulc_files)))

  # Extract years from LULC filenames
  lulc_years <- stringr::str_extract(lulc_files, "\\d{4}")

  # Read aggregation scheme externally provided
  scheme <- jsonlite::fromJSON(
    config[["lulc_aggregation_path"]],
    simplifyVector = FALSE
  )

  # remove 'original_classes' from scheme to avoid confusion
  scheme <- lapply(scheme, function(x) {
    x$original_classes <- NULL
    x$nhood_class <- NULL
    x$colour <- NULL
    return(x)
  })

  # Prepare LULC classes
  lulc_classes <- do.call(rbind, lapply(scheme, as.data.frame))
  message(sprintf("LULC scheme loaded: %d classes", nrow(lulc_classes)))

  # Directory for transition rate tables
  ensure_dir(config[["trans_rates_raw_dir"]])

  ### =========================================================================
  ### B- Calculate historic areal change for LULC classes
  ### =========================================================================

  message("Loading LULC rasters...")
  # Load LULC rasters with terra
  lulc_rasters <- purrr::map(
    lulc_files,
    terra::rast
  )
  names(lulc_rasters) <- lulc_years

  message("Computing pixel counts per LULC class...")
  # Compute area (pixel counts) for each LULC raster
  lulc_areas <- lapply(lulc_rasters, terra::freq, bylayer = FALSE)

  # Merge area tables by LULC class value
  suppressWarnings(
    # suppress warning about col names being repeated
    lulc_areal_change <- Reduce(
      function(x, y) merge(x, y, by = "value"),
      lulc_areas
    )
  )
  # Correctly rename all count columns with the year names
  names(lulc_areal_change)[2:ncol(lulc_areal_change)] <- names(lulc_areas)

  # Add LULC numeric values (Aggregated_ID) to the table
  lulc_areal_change |>
    dplyr::left_join(
      lulc_classes,
      by = "value"
    ) |>
    write.csv(
      file.path(
        config[["trans_rates_raw_dir"]],
        "lulc_historic_areal_change.csv"
      ),
      row.names = FALSE
    )
  message("Wrote historic areal change CSV")

  ### =========================================================================
  ### C- Preparing Transition tables for each calibration period
  ### =========================================================================

  # Dinamica produces tables of net transition rates (i.e. percentage of land that will
  # change from one state to another) These net rates of change are then used to
  # calculate gross rates of change during the CA allocation process by first
  # calculating areal change based on the current simulated landscape and then dividing
  # to number of cells that must transition

  # The net transition rate tables are calculated as the
  # difference between two initial and final historical landscape maps.

  # Transition tables produced by Dinamica contain all possible transitions so they need
  # to be subset to only transitions we want to model

  # Importantly Dinamica only accepts net transition rate tables in a very specific
  # format So in preparing tables for each scenarios future time points then we need to
  # stick to this

  # The order of the rows in the table is also crucial because the probability maps
  # produced for each transition need to be named according to their row number so the
  # value is associated correctly in allocation.

  # create a list combining the correct LULC years for each transition period
  lulc_change_periods <- c()
  for (i in 1:(length(lulc_years) - 1)) {
    lulc_change_periods[[i]] <- c(lulc_years[i], lulc_years[i + 1])
  }
  names(lulc_change_periods) <- sapply(
    lulc_change_periods,
    function(x) paste(x[1], x[2], sep = "_")
  )

  message(sprintf(
    "Computing transition matrices for %d periods: %s",
    length(lulc_change_periods),
    paste(names(lulc_change_periods), collapse = ", ")
  ))
  # Run function over each period
  mapply(
    compute_trans_rates_by_period,
    raster_combo = lulc_change_periods,
    period_name = names(lulc_change_periods),
    MoreArgs = list(
      raster_stack = lulc_rasters,
      trans_rates_dir = config[["trans_rates_raw_dir"]]
    ),
    SIMPLIFY = FALSE
  )
  message("Transition matrices written")

  ### =========================================================================
  ### C- Subsetting to Viable Transitions
  ### =========================================================================

  # Load single-step net transition tables produced for historic periods
  calibration_tables <- lapply(
    list.files(
      config[["trans_rates_raw_dir"]],
      full.names = TRUE,
      pattern = "_trans_table"
    ),
    read.csv
  )
  names(calibration_tables) <- gsub(
    "calibration_|_trans_table.csv",
    "",
    list.files(
      config[["trans_rates_raw_dir"]],
      full.names = FALSE,
      pattern = "_trans_table.csv$"
    )
  )

  viable_trans_by_period <- lapply(
    calibration_tables,
    function(x) {
      x$Initial_class <- sapply(
        x$From.,
        function(y) lulc_classes[lulc_classes$value == y, "class_name"]
      )
      x$Final_class <- sapply(
        x$To.,
        function(y) lulc_classes[lulc_classes$value == y, "class_name"]
      )
      x$Trans_name <- paste(x$Initial_class, x$Final_class, sep = "-")

      x$Trans_ID <- sprintf("%02d", seq_len(nrow(x)))
      return(x)
    }
  )

  message(sprintf(
    "Loaded single-step transition tables for %d periods",
    length(calibration_tables)
  ))

  # =========================================================================
  # Build unified wide-format CSV (replaces both the old RDS and the
  # external historic_lulc_transition_rates.csv)
  # =========================================================================

  use_regions <- isTRUE(config[["regionalization"]])

  # Build one data.frame per period then merge into wide format
  whole_map_dfs <- mapply(
    build_period_rates,
    period_name = names(viable_trans_by_period),
    period_df = viable_trans_by_period,
    MoreArgs = list(lulc_classes = lulc_classes),
    SIMPLIFY = FALSE
  )

  whole_map_csv <- Reduce(
    function(x, y) {
      merge(
        x,
        y,
        by = c("region_name", "from_lulc", "to_lulc", "From.", "To."),
        all = TRUE
      )
    },
    whole_map_dfs
  )

  message("Building whole-map transition rates (wide format)...")

  if (use_regions) {
    message("Computing regional rates...")

    regions_raster_path <- list.files(
      config[["reg_dir"]],
      pattern = "regions.tif$",
      full.names = TRUE
    )
    regions_raster <- terra::rast(regions_raster_path)
    regions_json <- jsonlite::fromJSON(
      file.path(config[["reg_dir"]], "regions.json")
    )

    # Create a data.frame of viable transitions (From., To.) to filter regional crosstabs
    trans_from_to_vals <- whole_map_csv[, c("From.", "To.")]

    regional_dfs <- mapply(
      compute_regional_trans_rates,
      raster_combo = lulc_change_periods,
      period_name = names(lulc_change_periods),
      MoreArgs = list(
        raster_stack = lulc_rasters,
        regions_raster = regions_raster,
        region_ids = regions_json$value,
        region_labels = regions_json$pretty,
        lulc_classes = lulc_classes,
        viable_pairs = trans_from_to_vals
      ),
      SIMPLIFY = FALSE
    )

    regional_csv <- Reduce(
      function(x, y) {
        merge(
          x,
          y,
          by = c("region_name", "from_lulc", "to_lulc", "From.", "To."),
          all = TRUE
        )
      },
      regional_dfs
    )

    viable_trans_csv <- rbind(whole_map_csv, regional_csv)
  } else {
    viable_trans_csv <- whole_map_csv
  }

  message(sprintf(
    "Writing viable transitions CSV: %d rows, %d periods",
    nrow(viable_trans_csv),
    length(lulc_change_periods)
  ))
  write.csv(
    viable_trans_csv,
    config[["viable_transitions_lists"]],
    row.names = FALSE
  )
  message("Transition identification complete")
}

# instantiate function to produce net percentage transition tables and save each to file
#' compute_trans_rates_by_period
#' Compute net transition rates for a given period by comparing two LULC rasters and save the resulting transition table as a CSV file. The function extracts the relevant rasters for the specified years, computes the crosstab of transitions, converts to percentage changes, and formats the output to match Dinamica's expected input for transition tables.
#' @param raster_combo A vector of two elements specifying the years of the LULC rasters to compare (e.g., c("1996", "2001")).
#' @param raster_stack A named list of LULC rasters, where names correspond to years (e.g., "1996", "2001").
#' @param period_name Name of the period (e.g., "1996_2001") used to name the output CSV file and the rate column in the output.
#' @param trans_rates_dir Directory where the resulting transition table CSV will be saved. The output file will be named "calibration_{period_name
#' }_trans_table.csv".
compute_trans_rates_by_period <- function(
  raster_combo,
  raster_stack,
  period_name,
  trans_rates_dir
) {
  # Extract rasters by year, drop RATs and rename
  r1 <- raster_stack[[grep(raster_combo[1], names(raster_stack))]]
  levels(r1) <- NULL
  names(r1) <- "from_lulc"

  r2 <- raster_stack[[grep(raster_combo[2], names(raster_stack))]]
  levels(r2) <- NULL
  names(r2) <- "to_lulc"

  # Produce transition matrix of areal changes for the whole period
  # Remove RATs from both rasters

  # Now do the crosstab with numerical values
  trans_rates_for_period <- terra::crosstab(c(r1, r2), long = FALSE)

  # Convert to percentage changes
  sum01 <- apply(trans_rates_for_period, MARGIN = 1, FUN = sum)
  percchange <- sweep(
    trans_rates_for_period,
    MARGIN = 1,
    STATS = sum01,
    FUN = "/"
  )
  perchange_for_period <- as.data.frame(percchange)

  # Convert columns to numeric
  perchange_for_period$from_lulc <- as.numeric(as.character(
    perchange_for_period$from_lulc
  ))
  perchange_for_period$to_lulc <- as.numeric(as.character(
    perchange_for_period$to_lulc
  ))

  # Remove persistence and zero rows
  perchange_for_period <- perchange_for_period[
    perchange_for_period$from_lulc != perchange_for_period$to_lulc,
  ]
  perchange_for_period <- perchange_for_period[
    perchange_for_period$Freq != 0,
  ]

  # Sort by initial class
  perchange_for_period <- perchange_for_period[
    order(perchange_for_period$from_lulc),
  ]

  # alter column names to match Dinamica trans tables
  colnames(perchange_for_period) <- c("From*", "To*", "Rate")
  # Save table
  readr::write_csv(
    perchange_for_period,
    file.path(
      trans_rates_dir,
      paste0("calibration_", period_name, "_trans_table.csv")
    )
  )
}


#' build_period_rates
#' Build a data.frame of transition rates for a given period, including both transition and persistence rows.
#' This is used to create the whole-map transition rates CSV, which is then merged with regional rates.
#' The output format is designed to match the expected input for Dinamica's transition tables, with columns for region_name, from_lulc, to_lulc, From., To., and the period-specific rate column.
#' @param period_name Name of the period (e.g., "1996_2001") used to name the rate column.
#' @param period_df Data.frame containing the viable transitions for the period, with columns From., To., and Rate.
#' @param lulc_classes Data.frame mapping numeric LULC values to class names, used
#' to add from_lulc and to_lulc names to the output.
#' @return A data.frame with transition and persistence rows for the period, including region_name = "whole_map".
#' The transition rows are taken directly from period_df, while the persistence rows are calculated as 1 - sum of off-diagonal rates for each class.
#' The output is designed to be merged with regional transition rates later, so it includes all combinations of from_lulc and to_lulc for the classes in the period_df.
#' The rate column is named according to the period_name (e.g., rate_1996_2001).
#' This function ensures that the output format is consistent with Dinamica's requirements for transition tables, facilitating later use in the CA allocation process.
#' Note that the output includes only the classes that appear in the period_df, so if a class does not have any transitions (i.e., it is not present in the From. column), it will not have a persistence row in the output.
#' This function is called within the main transition_identification function to build the whole-map transition rates before merging with regional rates.
#' The output of this function is crucial for ensuring that the transition rates are correctly formatted and include both transition and persistence information for use in future simulations.
build_period_rates <- function(period_name, period_df, lulc_classes) {
  rate_col <- paste0("rate_", period_name)

  # Transition rows
  trans_rows <- data.frame(
    from_lulc = period_df$Initial_class,
    to_lulc = period_df$Final_class,
    From. = period_df[["From."]],
    To. = period_df[["To."]],
    stringsAsFactors = FALSE
  )
  trans_rows[[rate_col]] <- period_df$Rate

  # Persistence rows: rate = 1 - sum(off-diagonal rates for that class)
  off_diag_sums <- tapply(period_df$Rate, period_df[["From."]], sum)
  unique_from_vals <- unique(period_df[["From."]])

  persist_rows <- data.frame(
    From. = unique_from_vals,
    stringsAsFactors = FALSE
  )
  persist_rows$from_lulc <- sapply(
    persist_rows$From.,
    function(v) lulc_classes[lulc_classes$value == v, "class_name"]
  )
  persist_rows$to_lulc <- persist_rows$from_lulc
  persist_rows$To. <- persist_rows$From.
  persist_rows[[rate_col]] <- 1 -
    off_diag_sums[as.character(unique_from_vals)]

  all_rows <- rbind(
    trans_rows,
    persist_rows[, c("from_lulc", "to_lulc", "From.", "To.", rate_col)]
  )
  all_rows$region_name <- "whole_map"
  all_rows
}

#' compute_regional_trans_rates
#' Compute regional transition rates for a given period by masking LULC rasters with region
#' masks and calculating crosstabs for each region. The output is a data.frame with transition rates for each region, including only viable transitions.
#' @param raster_combo A vector of two elements specifying the years of the LULC rasters to compare (e.g., c("1996", "2001")).
#' @param raster_stack A named list of LULC rasters, where names correspond to years (e.g., "1996", "2001").
#' @param regions_raster A raster where each pixel value corresponds to a region ID, used to mask the LULC rasters for regional calculations.
#' @param region_ids A vector of numeric region IDs corresponding to the values in regions_raster.
#' @param region_labels A vector of human-readable region labels corresponding to the region_ids, used in the output data.frame.
#' @param lulc_classes A data.frame mapping numeric LULC values to class names, used to add from_lulc and to_lulc names to the output.
#' @param viable_pairs A data.frame with columns From. and To. specifying the numeric LULC class transitions that are considered viable and should be included in the output.
#' @param period_name Name of the period (e.g., "1996_2001") used to name the rate column in the output.
#' @return A data.frame with columns region_name, from_lulc, to_luc, From., To., and rate_{period_name}, containing the transition rates for each region. The output includes only the transitions that are either persistence (from_lulc == to_lulc) or are in the viable_pairs list. The transition rates are calculated as the percentage of pixels that transition from one class to another within each region, based on the masked LULC rasters for the specified period.
compute_regional_trans_rates <- function(
  raster_combo,
  raster_stack,
  regions_raster,
  region_ids,
  region_labels,
  lulc_classes,
  viable_pairs, # data.frame: From. (numeric) / To. (numeric) of viable transitions
  period_name
) {
  r1 <- raster_stack[[grep(raster_combo[1], names(raster_stack))]]
  levels(r1) <- NULL
  names(r1) <- "from_lulc"

  r2 <- raster_stack[[grep(raster_combo[2], names(raster_stack))]]
  levels(r2) <- NULL
  names(r2) <- "to_lulc"

  rate_col <- paste0("rate_", period_name)

  results <- lapply(seq_along(region_ids), function(i) {
    reg_id <- region_ids[i]
    reg_label <- region_labels[i]

    region_mask <- regions_raster == reg_id
    r1_m <- terra::mask(r1, region_mask, maskvalue = FALSE)
    r2_m <- terra::mask(r2, region_mask, maskvalue = FALSE)

    trans_mat <- terra::crosstab(c(r1_m, r2_m), long = FALSE)
    row_sums <- apply(trans_mat, MARGIN = 1, FUN = sum)
    percchange <- sweep(trans_mat, MARGIN = 1, STATS = row_sums, FUN = "/")
    df <- as.data.frame(percchange)

    df$from_val <- as.numeric(as.character(df$from_lulc))
    df$to_val <- as.numeric(as.character(df$to_lulc))
    df <- df[df$Freq != 0, ] # drop true-zero cells

    if (nrow(df) == 0) {
      return(NULL)
    }

    # Add class names
    df$from_lulc_name <- sapply(df$from_val, function(v) {
      lulc_classes[lulc_classes$value == v, "class_name"]
    })
    df$to_lulc_name <- sapply(df$to_val, function(v) {
      lulc_classes[lulc_classes$value == v, "class_name"]
    })

    # Keep persistence rows + only whole-map-viable off-diagonal transitions
    is_persist <- df$from_val == df$to_val
    is_viable <- paste(df$from_val, df$to_val) %in%
      paste(viable_pairs$From., viable_pairs$To.)
    df <- df[is_persist | is_viable, ]

    if (nrow(df) == 0) {
      return(NULL)
    }

    result <- data.frame(
      region_name = reg_label,
      from_lulc = df$from_lulc_name,
      to_lulc = df$to_lulc_name,
      From. = df$from_val,
      To. = df$to_val,
      stringsAsFactors = FALSE
    )
    result[[rate_col]] <- df$Freq
    result
  })

  do.call(rbind, Filter(Negate(is.null), results))
}
