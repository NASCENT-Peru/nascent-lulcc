#' dinamica_trans_potent_calc
#'
#' Calculates the transition potential for land use/land cover (LULC) changes using
#' fitted statistical models. Supports both calibration and simulation modes, and can
#' optionally apply scenario-specific spatial interventions. Generates dynamic
#' predictors (e.g., population, neighbourhood), predicts transition probabilities for
#' each possible LULC transition, rescales probabilities, applies interventions if
#' required, and saves the resulting probability maps as raster files.
#'
#' @param config A list containing configuration parameters and file paths. Default:
#' result of `get_config()`.
#' @param simulation_num Integer. Simulation ID number to select simulation parameters.
#' @param time_step Integer. The current time step for which transition potentials are
#' calculated.
#'
#' @details
#' 1. Loads simulation parameters and configuration.
#' 1. Loads current LULC raster and layerizes it by class.
#' 1. Loads suitability and accessibility predictors, adapting to calibration or
#'    simulation mode.
#' 1. In simulation mode, generates dynamic predictors such as municipal population and
#'    neighbourhood statistics.
#' 1. Stacks all predictors and converts them to a dataframe for model prediction.
#' 1. Loads transition models and predicts transition probabilities for each transition
#'    and region.
#' 1. Rescales probabilities to ensure they sum to 1 for each cell.
#' 1. Optionally applies scenario-specific spatial interventions to the probabilities.
#' 1. Saves probability maps for each transition as raster files.
#'
#' @author Ben Black, Carlson Büth
#'
#' @seealso [lulcc.spatprobmanipulation()]
#'
#' @export

dinamica_trans_potent_calc <- function(
  config = get_config(),
  simulation_num = integer(),
  time_step = integer()
) {
  params <- get_simulation_params(simulation_id = simulation_num)

  if (grepl("y", params[["parallel_tpc.string"]], ignore.case = TRUE)) {
    stop("Removed support for parallelisation at this stage")
  }

  # Convert model mode into a string of the dates calibration period being used
  # this makes it easier to load files because they use this nomenclature

  calibration_periods <-
    config[["model_specs_path"]] |>
    readr::read_csv(col_types = "cccccllcc") |>
    purrr::pluck("data_period_name") |>
    unique()

  calibration_dates <- lapply(calibration_periods, function(period) {
    as.numeric(stringr::str_split(period, "_")[[1]])
  })

  period_tag <- if (!params[["is_simulation"]]) {
    Period_log <- sapply(calibration_dates, function(x) {
      time_step > x[1] & time_step <= x[2]
    })
    if (all(Period_log == FALSE)) {
      # find the closet year
      closest_year <- unlist(calibration_dates)[
        which.min(abs(unlist(calibration_dates) - time_step))
      ]
      # match year to period name
      Period_log <- calibration_periods[
        grepl(paste0(closest_year), calibration_periods)
      ]
    } else {
      calibration_periods[Period_log]
    }
  } else {
    # if simulation, take last calibration_period (presuming that vector is ordered)
    calibration_periods[length(calibration_periods)]
  }

  # create folder for saving prediction probability maps
  prob_map_folder <-
    fs::path(
      "results",
      params[["scenario_id.string"]],
      params[["simulation_id.string"]],
      "pred_prob_maps",
      time_step
    ) |>
    ensure_dir()

  # load model look up
  model_lookup <-
    readxl::read_excel(
      config[["model_lookup_path"]],
      sheet = period_tag
    ) |>
    dplyr::rename_with(tolower) |>
    dplyr::mutate(file_path = fs::path(config[["data_basepath"]], file_path))

  if (
    (no_predicted_maps <- length(fs::dir_ls(prob_map_folder))) ==
      length(unique(model_lookup[["trans_id"]]))
  ) {
    message(
      "Found ",
      no_predicted_maps,
      " predicted probability maps at ",
      prob_map_folder,
      "\n\tSkipping transition potential calculation."
    )
    return(prob_map_folder)
  }

  message(
    "Starting transition potential calculation for ",
    params[["model_mode.string"]],
    ": ",
    params[["simulation_id.string"]],
    ", with scenario: ",
    params[["scenario_id.string"]],
    ", at time step: ",
    time_step
  )
  message(" - loading maps from: ", params[["sim_results_path"]])

  # B- Retrieve current LULC map and layerize ####
  current_LULC_path <- fs::path(
    params[["sim_results_path"]],
    paste0(time_step, ".tif")
  )

  current_LULC_raster <- terra::rast(current_LULC_path)

  # load aggregation scheme (RAT = raster attribute table)
  lulc_rat <-
    config[["LULC_aggregation_path"]] |>
    readxl::read_excel() |>
    dplyr::select(tidyselect::all_of(c(
      "Class_abbreviation",
      "Aggregated_ID"
    ))) |>
    dplyr::distinct() |>
    dplyr::arrange(Aggregated_ID)

  # layerize data - one column for each LULC class. depends
  trans_dataset_list <- list()
  trans_dataset_list[["lulc_data_dt"]] <-
    current_LULC_raster |>
    terra::segregate(lulc_rat$Aggregated_ID) |>
    terra::as.data.frame(cells = TRUE, na.rm = TRUE) |>
    rlang::set_names(c("id", lulc_rat$Class_abbreviation)) |>
    data.table::as.data.table(key = "id")

  # C- Load Suitability and Accessibility predictors ####
  message("Loading Suitability and Accessibility predictors")

  prepared_data_dt_path <- fs::path(
    config[["prepped_pred_stacks"]],
    paste0(period_tag, ".csv.gz")
  )
  if (fs::file_exists(prepared_data_dt_path)) {
    trans_dataset_list[["prepared_data_dt"]] <-
      data.table::fread(prepared_data_dt_path, key = "id")
  } else {
    t1 <- proc.time()
    prepared_data_metatbl <-
      config[["pred_table_path"]] |>
      readxl::read_excel(sheet = period_tag) |>
      dplyr::mutate(
        path = fs::path(
          config[["data_basepath"]],
          path
        )
      )
    prepared_data_dt <-
      purrr::map2(
        prepared_data_metatbl$path,
        prepared_data_metatbl$pred_name,
        function(path, name) {
          terra::rast(path) |>
            terra::as.data.frame(cells = TRUE, na.rm = TRUE) |>
            rlang::set_names(c("id", name)) |>
            data.table::as.data.table(key = "id")
        }
      ) |>
      purrr::reduce(\(x, y) x[y, on = "id", nomatch = NULL])
    data.table::fwrite(prepared_data_dt, prepared_data_dt_path)
    data.table::setDT(prepared_data_dt, key = "id")
    trans_dataset_list[["prepared_data_dt"]] <- prepared_data_dt
    rm(prepared_data_dt, prepared_data_metatbl)
    t2 <- proc.time() - t1
    message(
      "Took ",
      t2[["elapsed"]],
      "s to read, join and write ",
      fs::file_size(prepared_data_dt_path),
      " for period ",
      period_tag,
      " at \n",
      prepared_data_dt_path
    )
  }

  message(" - Loaded Suitability and Accessibility predictors")

  # E- Dynamic predictors ####

  if (params[["is_simulation"]]) {
    message("Generating dynamic predictors:")
    trans_dataset_list[["pop_raster_dt"]] <- model_municip_pop(
      current_LULC_raster = current_LULC_raster,
      config = config,
      params = params,
      time_step = time_step
    )
    trans_dataset_list[["nhood_dt"]] <- model_neigh_preds(
      current_LULC_raster = current_LULC_raster,
      config = config,
      period_tag = period_tag,
      lulc_rat = lulc_rat
    )
  }

  # F- Regionalization ####
  if (config[["regionalization"]]) {
    message("Adding regionalization")
    trans_dataset_list[["bioregion_dt"]] <-
      fs::path(config[["reg_dir"]], "bioreg_raster.tif") |>
      terra::rast() |>
      terra::as.data.frame(cells = TRUE, na.rm = TRUE) |>
      rlang::set_names(c("id", "Bioregion")) |>
      data.table::as.data.table(key = "id")
  }

  # reduce all previously made tables by inner joining into a single table
  message("Combining all predictors")
  trans_dataset_complete <- purrr::reduce(
    trans_dataset_list,
    function(x, y) {
      x[y, on = "id", nomatch = NULL]
    }
  )
  trans_dataset_complete[, idx_complete_cases := seq_len(.N)]

  message(" - Reduced all transition datasets to a single data.table")

  # release memory
  rm(trans_dataset_list)

  # G- Run transition potential prediction for each transition ####
  # remove transitions if they are being implemented deterministically
  if (
    params[["is_simulation"]] &&
      grepl("Y", params[["deterministic_trans.string"]], ignore.case = TRUE)
  ) {
    # remove transitions with initial class == glacier
    message(" - Removing transitions with initial class == glacier")
    model_lookup <- model_lookup[model_lookup$initial_lulc != "Glacier", ]
  }

  # initialize prediction result data.table
  pred_subs_cols <- c("id", unique(model_lookup[["initial_lulc"]]))
  prediction_probs <- trans_dataset_complete[, ..pred_subs_cols]

  final_lulc_prob_cols <- paste0("prob_", unique(model_lookup[["final_lulc"]]))
  for (col in final_lulc_prob_cols) {
    prediction_probs[[col]] <- 0
  }

  # G2 Actual transition potential computation ####
  # loop over models
  message("Running transition potential prediction")
  t1 <- proc.time()
  for (mod_params in split(model_lookup, seq_len(nrow(model_lookup)))) {
    fitted_model <- readRDS(mod_params[["file_path"]])
    pred_data <- trans_dataset_complete[
      trans_dataset_complete[[mod_params[["initial_lulc"]]]] == 1 &
        trans_dataset_complete[["Bioregion"]] == mod_params[["region"]],
    ]

    prob_predicts <-
      predict(fitted_model, pred_data, type = "prob")[, 2] |>
      setNames(NULL)

    data.table::set(
      x = prediction_probs,
      i = pred_data[["idx_complete_cases"]],
      j = paste0("prob_", mod_params[["final_lulc"]]),
      value = prob_predicts
    )
  }

  t2 <- proc.time() - t1
  message(
    "Took ",
    t2[["elapsed"]],
    "s to complete the transition potential calculation for ",
    nrow(model_lookup),
    " models"
  )

  # G3- Re-scale predictions ####
  # loop over rows and re-scale probability values so that they sum to 1
  message("Re-scaling transition probabilities")
  normalize_over_cols(
    dt = prediction_probs,
    cols = final_lulc_prob_cols
  )

  # Get XY coordinates of cells
  xy_coordinates <-
    config[["ref_grid_path"]] |>
    terra::rast() |>
    terra::as.data.frame(xy = TRUE, cell = TRUE, na.rm = FALSE) |>
    dplyr::transmute(
      id = as.integer(cell),
      x = as.integer(x),
      y = as.integer(y)
    ) |>
    data.table::as.data.table(key = "id")

  # bind with background values
  raster_prob_values <- prediction_probs[xy_coordinates]

  # H- Spatial manipulations of transition probabilities ####
  if (
    params[["is_simulation"]] &&
      params[["spatial_interventions.string"]] == "Y"
  ) {
    # load table of scenario interventions
    Interventions <- readr::read_csv2(
      config[["spat_ints_path"]],
      col_types = "cccccdcc"
    )

    # Use function to perform manipulation of spatial transition probabilities
    # according to scenario-specific interventions
    raster_prob_values <- lulcc.spatprobmanipulation(
      Interventions = Interventions,
      scenario_id = params[["scenario_id.string"]],
      Raster_prob_values = raster_prob_values,
      Simulation_time_step = paste(time_step)
    )
  }

  # I- Final rescaling ####
  message("Performing final re-scaling")
  normalize_over_cols(
    dt = raster_prob_values,
    cols = final_lulc_prob_cols
  )

  # J- Save transition rasters ####

  message("Saving transition rasters")

  # subset model_lookup table to unique trans ID
  unique_trans <-
    model_lookup |>
    dplyr::distinct(trans_id, final_lulc, initial_lulc) |>
    (\(x) split(x, seq_len(nrow(x))))()

  # Loop over unique trans using details to subset data and save Rasters
  for (trans_params in unique_trans) {
    message(" - Preparing layer ", trans_params[["trans_id"]])
    initial_lulc <- trans_params[["initial_lulc"]]
    final_lulc <- trans_params[["final_lulc"]]

    cols <- c("x", "y", paste0("prob_", final_lulc))

    trans_raster_values <- data.table::copy(raster_prob_values[, ..cols])

    data.table::set(
      x = trans_raster_values,
      # get indices of non-class cells
      i = which(raster_prob_values[[initial_lulc]] == 0),
      j = paste0("prob_", final_lulc),
      # replace values of non-class cells with 0
      value = 0
    )

    data.table::setnames(
      x = trans_raster_values,
      old = paste0("prob_", final_lulc),
      new = paste0("prob_", initial_lulc, "_to_", final_lulc)
    )

    # rasterize and save using Initial and Final class names
    prob_raster <- terra::rast(
      trans_raster_values,
      crs = terra::crs(current_LULC_raster)
    )

    # vector file path for saving probability maps
    prob_map_path <- fs::path(
      prob_map_folder,
      paste0(
        trans_params[["trans_id"]],
        "_probability_",
        initial_lulc,
        "_to_",
        final_lulc,
        ".tif"
      )
    )

    # FIXME this has been erratically crashing for transition 16, grassland to urban
    # defaults to LZW compression
    terra::writeRaster(
      x = prob_raster,
      filename = prob_map_path,
      overwrite = TRUE,
      NAflag = -999 # because dinamica cannot handle nan
    )
  } # close loop over transitions

  # Return the probability map folder path as a string to
  # Dinamica to indicate completion
  # Note strings must be vectorized for 'outputString to work
  message(
    "Probability maps saved to: ",
    prob_map_folder
  )

  prob_map_folder
}


model_municip_pop <- function(current_LULC_raster, config, params, time_step) {
  # E.1- Dynamic predictors:  Municipal Population; for calibration the raster stacks
  # already contain the dynamic predictor layers so there is nothing to be done
  message(" - Municipal population")
  # create population data layer
  # subset current LULC to just urban cells
  Urban_rast <- current_LULC_raster == 10

  # load canton shapefile
  Canton_shp <- terra::vect(
    fs::path(
      config[["ch_geoms_path"]],
      "swissboundaries3d_1_5_tlm_kantonsgebiet.shp"
    )
  )

  # Zonal stats to get urban area per kanton
  Canton_urban_areas <- terra::extract(
    Urban_rast,
    Canton_shp,
    fun = sum,
    na.rm = TRUE
  )

  # append Kanton ID
  Canton_urban_areas$Canton_num <- Canton_shp$KANTONSNUM

  # combine areas for cantons with multiple polygons
  Canton_urban_areas <-
    Canton_urban_areas |>
    dplyr::group_by(Canton_num) |>
    dplyr::summarise(layer = sum(lulc_name))

  # load the municipality shape file
  Muni_shp <- terra::vect(
    fs::path(
      config[["ch_geoms_path"]],
      "swissboundaries3d_1_5_tlm_hoheitsgebiet.shp"
    )
  )

  # filter out non-swiss municipalities
  Muni_shp <- Muni_shp[
    Muni_shp$ICC == "CH" & Muni_shp$OBJEKTART == "Gemeindegebiet",
  ]

  # Zonal stats to get number of Urban cells per Municipality polygon
  # sum is used as a function because urban cells = 1 all others = 0
  Muni_urban_areas <- terra::extract(
    Urban_rast,
    Muni_shp,
    fun = sum,
    na.rm = TRUE
  )

  # append Kanton and Municipality IDs
  Muni_urban_areas$Canton_num <- Muni_shp$KANTONSNUM
  Muni_urban_areas$Muni_num <- Muni_shp$BFS_NUMMER
  Muni_urban_areas$Perc_urban <- 0

  # loop over kanton numbers and calculate municipality urban areas as a % of canton urban area
  for (i in Canton_urban_areas$Canton_num) {
    # vector kanton urban area
    Can_urban_area <- as.numeric(Canton_urban_areas[
      Canton_urban_areas$Canton_num == i,
      "layer"
    ])

    # subset municipalities to this canton number
    munis_indices <- which(Muni_urban_areas$Canton_num == i)

    # calculate urban areas as a % of the Canton's total
    Muni_urban_areas$Perc_urban[munis_indices] <- (Muni_urban_areas[
      munis_indices,
      "lulc_name"
    ] /
      Can_urban_area) *
      100
  }

  # estimate % of predicted cantonal population per municipality
  # Load list of cantonal population models
  pop_models <- readRDS(
    file.path(config[["preds_tools_dir"]], "dynamic_pop_models.rds")
  )

  # add predicted % pop results column
  Muni_urban_areas$Perc_pop <- 0

  # estimate % of pop per municipality
  # loop over unique polygon IDs rather than BFS numbers to take into account that
  # multiple polygons have the same BFS number
  for (i in Muni_urban_areas$ID) {
    # seperate canton specific model
    canton_model <- pop_models[[
      Muni_urban_areas[Muni_urban_areas$ID == i, "Canton_num"]
    ]]

    # perform prediction
    Muni_urban_areas$Perc_pop[[i]] <- predict(
      canton_model,
      newdata = Muni_urban_areas[Muni_urban_areas$ID == i, ]
    )
  }

  # load correct sheet of future population predictions according to scenario
  Pop_prediction_table <- openxlsx::read.xlsx(
    fs::path(config[["preds_tools_dir"]], "population_projections.xlsx"),
    sheet = params[["pop_scenario.string"]]
  )

  # loop over unique kanton numbers, rescale the predicted population percentages
  # and calculate the estimated population per municipality as a % of the cantonal total

  # add results column
  Muni_urban_areas$Pop_est <- 0

  for (i in unique(Muni_urban_areas$Canton_num)) {
    # subset to predicted cantonal population percentages
    Canton_dat <- Muni_urban_areas[
      Muni_urban_areas$Canton_num == i,
      "Perc_urban"
    ]

    # loop over the municipalites re-scaling the values
    Canton_preds_rescaled <- sapply(
      Canton_dat,
      function(y) {
        value <- y * 1 / sum(Canton_dat)
        # dividing by Zero introduces NA's so these must be converted back to zero
        value[is.na(value)] <- 0
        value
      }
    ) # close inner loop

    # get the projected canton population value for this time point
    Canton_pop <- Pop_prediction_table[
      Pop_prediction_table$Canton_num == i,
      paste(time_step)
    ]

    # loop over the rescaled values calculating the estimated population
    Muni_indices <- which(Muni_urban_areas$Canton_num == i)
    Muni_urban_areas$Pop_est[Muni_indices] <- (Canton_pop *
      Canton_preds_rescaled)
  } # close loop over cantons

  # add estimated population to  table of polygons and then rasterize
  Muni_shp$Pop_est <- Muni_urban_areas$Pop_est
  Ref_grid <- terra::rast(config[["ref_grid_path"]])
  pop_raster <- terra::rasterize(
    x = Muni_shp,
    y = Ref_grid,
    field = "Pop_est"
  )

  # why was this todo? THIS MUST BE THE LAYER NAME IN THE CALIBRATION STACKS/MODELS
  names(pop_raster) <- "Muni_pop"

  terra::as.data.frame(pop_raster, cells = TRUE, na.rm = TRUE) |>
    rlang::set_names(c("id", "Muni_pop")) |>
    data.table::as.data.table(key = "id")
}

#' E.2- Dynamic predictors: Neighbourhood predictors
model_neigh_preds <- function(
  current_LULC_raster,
  config,
  period_tag,
  lulc_rat
) {
  message(" - Neighbourhood predictors")

  # load matrices used to create focal layers
  Focal_matrices <-
    fs::path(
      config[["preds_tools_dir"]],
      "neighbourhood_matrices",
      "all_matrices.rds"
    ) |>
    readRDS() |>
    unlist(recursive = FALSE)

  # adjust matrix names
  names(Focal_matrices) <- sapply(names(Focal_matrices), function(x) {
    stringr::str_split(x, "[.]")[[1]][2]
  })

  # Load details of focal layers required for the model set being utilised
  Required_focals_details <- readRDS(list.files(
    fs::path(
      config[["preds_tools_dir"]],
      "neighbourhood_details_for_dynamic_updating"
    ),
    pattern = period_tag,
    full.names = TRUE
  ))

  # Loop over details of focal layers required creating a list of rasters from the
  # current LULC map
  message(" - Neighbourhood predictors: focals")
  Nhood_rasters <- list()
  for (i in seq_len(nrow(Required_focals_details))) {
    # vector active class names
    Active_class_name <- Required_focals_details[i, ]$active_lulc

    # get pixel values of active LULC class
    Active_class_value <- unlist(
      lulc_rat[
        lulc_rat$Class_abbreviation == Active_class_name,
        "Aggregated_ID"
      ]
    )

    # subset LULC raster by all Active_class_value
    Active_class_raster_subset <- current_LULC_raster == Active_class_value

    # create focal layer using matrix
    Focal_layer <- terra::focal(
      x = Active_class_raster_subset,
      w = Focal_matrices[[Required_focals_details[i, ]$matrix_id]],
      na.rm = FALSE,
      expand = TRUE,
      fillvalue = 0
    )

    # create file path for saving this layer
    Focal_name <- paste(
      Active_class_name,
      "nhood",
      Required_focals_details[i, ]$matrix_id,
      sep = "_"
    )
    Nhood_rasters[[Focal_name]] <- Focal_layer
  }

  message(" - Neighbourhood predictors: reducing")
  out <-
    purrr::map2(
      Nhood_rasters,
      names(Nhood_rasters),
      function(rast, name) {
        rast |>
          terra::as.data.frame(cells = TRUE, na.rm = TRUE) |>
          rlang::set_names(c("id", name)) |>
          data.table::as.data.table(key = "id")
      }
    ) |>
    purrr::reduce(\(x, y) x[y, on = "id", nomatch = NULL])

  out
}


#' @describeIn dinamica_trans_potent_calc Normalize a set of columns such that the sum
#' of probabilities does not exceed 1.
#' @importFrom data.table `:=`
#' @param dt a data.table
#' @param cols character vector of columns
#' This function is called for the side effect on the dt argument.
normalize_over_cols <- function(dt, cols) {
  # FIXME in non-interactive mode, R sometimes chooses to copy this data table
  dt[,
    prob_totals := rowSums(.SD), # need intermediary for filtering
    .SDcols = cols
  ][
    # FIXME this was called non_zero_indices but is filtering for >1
    prob_totals > 1,
    names(.SD) := purrr::map(.SD, \(x) x / prob_totals),
    .SDcols = cols
  ][,
    prob_totals := NULL
  ]
}
