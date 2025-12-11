#' Calculate_allocation_parameters:
#'
#' Using land use data from calibration (historic) periods to calculate parameters for
#' Dinamica's Patcher and Expander algorithmns Mean patch size, patch size variance,
#' patch isometry, percentage of transitons in new patches vs. expansion of existing
#' patches
#'
#' @param config a list of configuration params
#'
#' @details
#' 1. Estimate values for the allocation parameters and then apply random perturbation
#' to generate sets of values to test with monte-carlo simulation
#' 2. perform simulations with all parameter sets
#' 3. Identify best performing parameter sets and save copies of tables
#' to be used in scenario simulations
#'
#' @author Ben Black

calibrate_allocation_parameters <- function(config = get_config()) {
  # vector years of lulc data
  lulc_years <-
    list.files(
      config[["aggregated_lulc_dir"]],
      full.names = FALSE,
      pattern = ".tif"
    ) |>
    stringr::str_extract("\\d{4}")

  # Load list of historic lulc rasters
  lulc_rasters <- lapply(
    list.files(
      config[["aggregated_lulc_dir"]],
      full.names = TRUE,
      pattern = ".tif"
    ),
    raster::raster
  )
  names(lulc_rasters) <- lulc_years

  # create folders for results
  ensure_dir(config[["simulation_param_dir"]])
  ensure_dir(file.path(config[["calibration_param_dir"]], "periodic"))

  # loop over config$data_periods to create list of raster combinations
  lulc_change_periods <- lapply(
    config[["data_periods"]],
    function(period) {
      years <- as.numeric(stringr::str_split(period, "_")[[1]])
      c(
        paste0(years[1]),
        paste0(years[2])
      )
    }
  )
  names(lulc_change_periods) <- config[["data_periods"]]

  #' Function to calculate allocation parameters for a given time period
  #' @param comparison_years vector of the names of the two years to compare
  #' @param lulc_rasters list of rasters
  #' @param period_name name of the time period
  #' @param use_regions logical, whether to calculate parameters by region
  #' @return data frame of allocation parameters
  calculate_allocation_params_for_periods <- function(
    comparison_years,
    lulc_rasters,
    period_name,
    use_regions = isTRUE(config[["regionalization"]])
  ) {
    # seperate rasters required for time period
    yr1 <- lulc_rasters[[grep(comparison_years[1], names(lulc_rasters))]]
    yr2 <- lulc_rasters[[grep(comparison_years[2], names(lulc_rasters))]]

    # --- Set up regions ---
    if (use_regions) {
      regions <- jsonlite::fromJSON(file.path(
        config[["reg_dir"]],
        "regions.json"
      ))
      region_names <- regions$label
      message(sprintf(
        "Processing %d regions: %s\n",
        length(region_names),
        paste(region_names, collapse = ", ")
      ))
      # load the region raster
      region_rast <- terra::rast(list.files(
        config[["reg_dir"]],
        pattern = "regions.tif$",
        full.names = TRUE
      ))
    } else {
      region_names <- "National extent"
      message("Processing national extent (no regionalization)\n")
    }

    # load summary of transition feature selection
    transitions <- readRDS(
      file.path(
        config[["feature_selection_dir"]],
        sprintf(
          "transition_feature_selection_summary_%s.rds",
          period_name
        )
      )
    )

    # Filter out transitions that failed feature selection
    transitions <- transitions %>%
      dplyr::filter(
        !is.na(selected_predictors) &
          selected_predictors != "" &
          nchar(trimws(selected_predictors)) > 0
      )

    # Loop over transitions
    results <- furrr::future_map_dfr(
      seq_len(nrow(transitions)),
      .options = furrr::furrr_options(seed = TRUE),
      function(i) {
        # Identify cells in the rasters according to the 'From' and 'To' values
        r1 <- raster::Which(yr1 == transitions[i, c("From.")])
        r2 <- raster::Which(yr2 == transitions[i, c("To.")])
        Final_class_in_yr1 <- raster::Which(yr1 == transitions[i, c("To.")])

        # multiply rasters to identify transition cells
        r <- r1 * r2

        # TODO why do we identify patches when we immediately discard their ID? does this drop single-cell patches?
        # identify patches of the final land use class in the yr1 raster
        Final_yr1_patches <- raster::clump(Final_class_in_yr1, directions = 8)
        # convert values (Patch IDs) above 0 to 1 (i.e. binary in patch (1) outside patch (0))
        Final_yr1_patches[Final_yr1_patches > 0] <- 1
        Final_yr1_patches[is.na(Final_yr1_patches[])] <- 0

        # identify patches of the final land use class in the yr2 raster
        Final_yr2_patches <- raster::clump(r2, directions = 8)
        # convert values (Patch IDs) above 0 to 2 (i.e. binary in patch (10) outside patch (0))
        Final_yr2_patches[Final_yr2_patches > 0] <- 10
        Final_yr2_patches[is.na(Final_yr2_patches[])] <- 0

        # Add rasters together so that:
        # 0= not in patch in either year
        # 1 = in patch in year 1
        # 10 = in patch in year 2 only (i.e. new patch)
        # 11 = in patch in both years
        yr1_yr2_patches <- Final_yr1_patches + Final_yr2_patches

        # Multiply this raster by the raster of transition cells to select only
        # the transitoon cells in year 2 patches
        Trans_cells_yr_patches <- r * yr1_yr2_patches

        # test to see which 10 valued cells are adjacent to patch cells in yr1
        # i.e. they represent expansion and not new patches.

        # create a new raster to store results in
        expansion_or_new <- Trans_cells_yr_patches

        # Get cell numbers for those in patches in yr2 only
        patchcells <- which(raster::values(Trans_cells_yr_patches) == 10)

        # loop over cells in patchs
        for (cell in patchcells) {
          # TODO this is a relatively hot loop and may be pushed to some vectorized operations
          # get the cell numbers of adjacent cells
          ncells <- raster::adjacent(
            Trans_cells_yr_patches,
            cell = cell,
            direction = 8,
            include = FALSE,
            pairs = FALSE
          )

          # sum up the values in the adjacent cells in the year 1 patches,
          # a value of >=1 indicates that the new patch cells are directly adjacent to
          # old patch cells and hence represent expansion
          # a value of <1 indicates no adjacent old patch cells and hence new patch
          Y_N <- if (sum(Final_yr1_patches[ncells], na.rm = TRUE) >= 1) {
            10
          } else {
            5
          }

          expansion_or_new[cell] <- Y_N
        }

        # calculate the proportions of transition cells
        # that exist in new patches vs. expansion
        perc_expander <- (raster::freq(expansion_or_new, value = 10) /
          raster::freq(r, value = 1) *
          100)
        perc_patcher <- (raster::freq(expansion_or_new, value = 5) /
          raster::freq(r, value = 1) *
          100)

        # Calculate class statistics for patchs in rasters
        # FIXME this is a pretty broken package, see if we can replace these guesses
        # with some other package's patch metrics.
        cl.data <- SDMTools::ClassStat(
          r,
          bkgd = 0,
          cellsize = raster::res(r)[1]
        )

        # Mean patch area
        mpa <- cl.data$mean.patch.area / 10000

        # Standard Deviation patch area
        sda <- cl.data$sd.patch.area / 10000

        # Patch Isometry
        iso <- cl.data$aggregation.index / 70

        # Combine results
        result <- tibble::tibble_row(
          "From*" = transitions[i, 1],
          "To*" = transitions[i, 2],
          " Mean_Patch_Size" = mpa,
          "Patch_Size_Variance" = sda,
          "Patch_Isometry" = iso,
          "Perc_expander" = perc_expander,
          "Perc_patcher" = perc_patcher
        )
        result
      }
    ) |>
      dplyr::bind_rows()

    # better to save seperate tables for the patch related parameters vs.
    # the % expansion params to eliminate the need to seperate when loading into Dinamica
    # save
    readr::write_csv(
      results,
      file = file.path(
        config[["calibration_param_dir"]],
        "periodic",
        paste0("allocation_parameters_", period_name, ".csv")
      )
    )

    results
  }

  # Apply function
  Allocation_params_by_period <- mapply(
    calculate_allocation_params_for_periods,
    comparison_years = lulc_change_periods,
    period_name = names(lulc_change_periods),
    MoreArgs = list(lulc_rasters = lulc_rasters),
    SIMPLIFY = FALSE
  )

  # C - Creating patch size parameter tables for calibration ####

  # Whilst we have estimated values of percentage of transitions corresponding to
  # expansion of existing patches vs. occurring in new patches, we don't know how
  # accurate these are so it is desirable to perform calibration by monte-carlo
  # simulation using random permutations of these values

  # IMPORTANT
  # For Dinamica the % expansion values must be expressed as decimals
  # so they are converted in this loop

  # First create a lookup table to control looping over the simulations
  calibration_control_table <- data.frame(matrix(ncol = 11, nrow = 0))
  colnames(calibration_control_table) <- c(
    "simulation_num.",
    "scenario_id.string",
    "simulation_id.string",
    "model_mode.string",
    "scenario_start.real",
    "scenario_end.real",
    "step_length.real",
    "parallel_tpc.string",
    "spatial_interventions.string",
    "deterministic_trans.string",
    "completed.string"
  )

  # reload allocation parameter tables
  Allocation_params_by_period <- lapply(
    list.files(
      file.path(config[["calibration_param_dir"]], "periodic"),
      full.names = TRUE
    ),
    read.csv
  )
  names(Allocation_params_by_period) <- config[["data_periods"]]

  # reloading also causes r to mess up the column names, readjust
  # Adjust col names
  Allocation_params_by_period <- lapply(
    Allocation_params_by_period,
    function(params_period) {
      colnames(params_period) <- c(
        "From*",
        "To*",
        " Mean_Patch_Size",
        "Patch_Size_Variance",
        "Patch_Isometry",
        "Perc_expander",
        "Perc_patcher"
      )
      params_period$Perc_expander <- params_period$Perc_expander / 100
      params_period$Perc_patcher <- params_period$Perc_patcher / 100
      params_period
    }
  )

  # because we are most interested in calibrating the patch size values for the
  # time period that will be used in simulations we will run the calibration using
  # the params from the most recent data period
  # we have initially agreed to use 5 year time steps
  scenario_start <- 2010
  scenario_end <- 2020
  step_length <- 5

  # vector sequence of time points and suffix
  Time_steps <- seq(scenario_start, scenario_end, step_length)

  # seperate vector of time points into those relevant for each calibration period
  Time_points_by_period <- lapply(config[["data_periods"]], function(period) {
    dates <- as.numeric(stringr::str_split(period, "_")[[1]])
    Time_steps[
      sapply(Time_steps, function(year) {
        if (year > dates[1] & year <= dates[2]) {
          TRUE
        } else if ((scenario_end - dates[2]) < step_length) {
          TRUE
        } else {
          FALSE
        }
      })
    ]
  })
  names(Time_points_by_period) <- config[["data_periods"]]

  # remove any time periods that are empty
  Time_points_by_period <- Time_points_by_period[
    lapply(Time_points_by_period, length) > 0
  ]

  # subset the list of allocation params tables by the names of the time_periods
  Allocation_params_by_period <- Allocation_params_by_period[names(
    Time_points_by_period
  )]

  # create seperate files of the estimated allocation parameters for each time point
  # under the ID: v1

  # loop over the list of years for each time point saving a copy of the
  # corresponding parameter table foreach one
  ensure_dir(file.path(config[["calibration_param_dir"]], "v1"))
  sapply(seq_along(Time_points_by_period), function(period_indices) {
    sapply(Time_points_by_period[[period_indices]], function(x) {
      file_name <- file.path(
        config[["calibration_param_dir"]],
        "v1",
        paste0(
          "allocation_param_table_",
          x,
          ".csv"
        )
      )
      readr::write_csv(
        Allocation_params_by_period[[period_indices]],
        file = file_name
      )
    })
  })

  # create a sequence of names for the number of monte-carlo simulations
  mc_sims <- sapply(seq(2, 100, 1), function(x) paste0("v", x))

  # loop over the mc_sim names and perturb the allocation params
  # saving a table for every time point in the calibration period
  sapply(mc_sims, function(Sim_name) {
    # inner loop over time periods and parameter tables
    mapply(
      function(Time_steps, calibration_param_dir, param_table) {
        # create folder for saving param tables for MC sim name
        dir.create(file.path(calibration_param_dir, Sim_name), recursive = TRUE)

        # random perturbation of % expander
        # (increase of decrease value by random amount in normal distribution with mean
        # = 0 and sd = 0.05 effectively 5% bounding)
        param_table$Perc_expander <-
          param_table$Perc_expander +
          rnorm(length(param_table$Perc_expander), mean = 0, sd = 0.05)

        # if any values are greater than 1 or less than 0 then set to these values
        param_table$Perc_expander <- sapply(
          param_table$Perc_expander,
          function(x) {
            if (x > 1) {
              x <- 1
            } else if (x < 0) {
              0
            } else {
              x
            }
          }
        )

        # recalculate % patcher so that total does not exceed 1 (100%)
        param_table$Perc_patcher <- 1 - param_table$Perc_expander

        # inner loop over individual time points
        sapply(Time_steps, function(x) {
          file_name <- file.path(
            calibration_param_dir,
            Sim_name,
            paste0("allocation_param_table_", x, ".csv")
          )
          readr::write_csv(param_table, file = file_name)
        }) # close loop over time points
      },
      Time_steps = Time_points_by_period,
      calibration_param_dir = config[["calibration_param_dir"]],
      param_table = Allocation_params_by_period,
      SIMPLIFY = FALSE
    ) # close loop over time periods
  }) # close loop over simulation IDs.

  # Now add entries for these MC simulations into the calibration control table
  # add v1 to mc_sims
  mc_sims <- c("v1", mc_sims)

  # add rows for MC sim_names
  for (i in seq_along(mc_sims)) {
    calibration_control_table[i, "simulation_id.string"] <- mc_sims[i]
    calibration_control_table[i, "simulation_num."] <- i
  }

  # fill in remaining columns
  calibration_control_table$scenario_id.string <- "CALIBRATION"
  calibration_control_table$scenario_start.real <- scenario_start
  calibration_control_table$scenario_end.real <- scenario_end
  calibration_control_table$step_length.real <- step_length
  calibration_control_table$model_mode.string <- "Calibration"
  calibration_control_table$parallel_tpc.string <- "N"
  calibration_control_table$completed.string <- "N"
  calibration_control_table$spatial_interventions.string <- "N"
  calibration_control_table$deterministic_trans.string <- "N"

  # save table
  readr::write_csv(
    calibration_control_table,
    config[["calibration_ctrl_tbl_path"]]
  )

  # D - Perform simulation for calibration ####
  work_dir <- Sys.getenv("EVOLAND_CALIBRATION_DIR", unset = "calibration")
  run_evoland_dinamica_sim(
    calibration = TRUE,
    work_dir = work_dir
  )

  # because the simulations may fail without the system command returning an error
  # (if the error occurs in Dinamica) then check the control table to see
  # if/how many simulations have failed
  updated_control_tbl <- read.csv(
    fs::path(work_dir, default_ctrl_tbl_path())
  )

  if (errs <- sum(updated_control_tbl$completed.string == "ERROR")) {
    stop(
      sum(errs),
      " of ",
      nrow(updated_control_tbl),
      " simulations have failed to run till completion, go check the logs."
    )
  }

  # E - Evaluate calibration, selecting best parameter set ####

  # load the similarity values produced from the validation process inside Dinamica
  # for each simulation
  calibration_results <-
    fs::dir_ls(
      path = work_dir,
      glob = "**/similarity_value*.csv", # grandparent folder is simulation_id
      recurse = TRUE
    ) |>
    rlang::set_names(\(x) stringr::str_split_i(x, "/", -3)) |> # get simulation_id from folder name
    purrr::map(
      readr::read_csv, # these files are actually just a float without header
      col_names = "similarity_value",
      col_types = "d"
    ) |>
    dplyr::bind_rows(.id = "simulation_id") |>
    dplyr::filter(simulation_id != "summary")

  # summary statistics
  calibration_summary <- data.frame(
    Mean = mean(calibration_results[["similarity_value"]]),
    `Standard Deviation` = sd(calibration_results[["similarity_value"]]),
    Minimum = min(calibration_results[["similarity_value"]]),
    Maximum = max(calibration_results[["similarity_value"]]),
    n = length(calibration_results[["similarity_value"]])
  )

  # save summary statistics
  readr::write_csv(
    calibration_summary,
    file.path(work_dir, "validation_summary.csv")
  )

  # select best performing simulation_ID
  best_sim_ID <- calibration_results[
    which.max(calibration_results[["similarity_value"]]),
  ][["simulation_id"]]

  # Use this sim ID to create parameter tables for all simulation time points
  # in the Simulation folder

  # get exemplar table
  param_table <- read.csv(
    list.files(
      file.path(config[["calibration_param_dir"]], best_sim_ID),
      full.names = TRUE,
      pattern = "2020"
    )
  )
  colnames(param_table) <- c(
    "From*",
    "To*",
    " Mean_Patch_Size",
    "Patch_Size_Variance",
    "Patch_Isometry",
    "Perc_expander",
    "Perc_patcher"
  )

  # get simulation start and end times from control table
  simulation_control <- readr::read_csv(config[["ctrl_tbl_path"]])
  simulation_start <- min(simulation_control$scenario_start.real)
  simulation_end <- max(simulation_control$scenario_end.real)
  scenario_IDs <- unique(simulation_control$scenario_id.string)

  # loop over scenario IDs and simulation time points creating allocation param tables
  sapply(
    scenario_IDs,
    function(scenario_id) {
      sapply(
        seq(simulation_start, simulation_end, step_length),
        function(simulation_year) {
          save_dir <- file.path(config[["simulation_param_dir"]], scenario_id)
          ensure_dir(save_dir)
          file_name <- file.path(
            save_dir,
            paste0("allocation_param_table_", simulation_year, ".csv")
          )
          readr::write_csv(param_table, file = file_name)
        }
      )
    }
  )
}
