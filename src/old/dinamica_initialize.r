#' Get Simulation Parameters
#'
#' This functionality is roughly equivalent to what used to be called
#' `Dinamica_initialize.r`. It is intended to be called from within a Dinamica
#' simulation to read parameters.
#' Date: 25-02-2022
#' Author: Ben Black

#' @export
default_ctrl_tbl_path <- function() {
  Sys.getenv(
    "EVOLAND_CTRL_TBL_PATH",
    unset = "simulation_control.csv"
  )
}

#' @export
get_control_table <- function(ctrl_tbl_path = default_ctrl_tbl_path()) {
  tbl <- readr::read_csv(
    ctrl_tbl_path,
    col_types = readr::cols(
      simulation_num. = readr::col_integer(),
      scenario_id.string = readr::col_character(),
      simulation_id.string = readr::col_character(),
      model_mode.string = readr::col_character(),
      scenario_start.real = readr::col_double(),
      scenario_end.real = readr::col_double(),
      step_length.real = readr::col_double(),
      parallel_tpc.string = readr::col_character(),
      spatial_interventions.string = readr::col_character(),
      deterministic_trans.string = readr::col_character(),
      completed.string = readr::col_character()
    )
  )

  if (
    !is.null(tbl[["ei_intervention_id.string"]]) &&
      !all(is.na(tbl[["ei_intervention_id.string"]]))
  ) {
    # hard error for safety's sake
    stop(
      "The ei_intervention_id.string does nothing. ",
      "Empty or remove the column to proceed."
    )
  }

  tbl
}

# get a table of those
#' @export
get_remaining_simulations <- function(ctrl_tbl_path = default_ctrl_tbl_path()) {
  get_control_table(ctrl_tbl_path) |>
    dplyr::filter(completed.string == "N") |>
    dplyr::relocate(simulation_num.)
}

# get parameters for a given simulation run (scenario) given a control table path and a
# single integer simulation ID_loc; has side effect of creating lulc_maps dir
#' @export
get_simulation_params <- function(
    ctrl_tbl_path = default_ctrl_tbl_path(),
    simulation_id = integer()) {
  stopifnot(rlang::is_scalar_integerish(simulation_id))

  params <-
    get_control_table(ctrl_tbl_path) |>
    dplyr::filter(simulation_num. == simulation_id) |>
    as.list()

  if (!rlang::is_scalar_integerish(params[["simulation_num."]])) {
    stop(
      "No simulation matching the requested simulation_id ",
      "(check simulation_num in control table)"
    )
  }

  stopifnot(grepl(
    "simulation|calibration",
    params[["model_mode.string"]],
    ignore.case = TRUE
  ))

  params[["is_simulation"]] <- grepl(
    "simulation",
    params[["model_mode.string"]],
    ignore.case = TRUE
  )

  if (fs::is_absolute_path(ctrl_tbl_path)) {
    resultsdir <- fs::path(fs::path_dir(ctrl_tbl_path), "results")
  } else {
    resultsdir <- "results"
  }

  params[["sim_results_path"]] <-
    fs::path(
      resultsdir,
      # simulation_id.string is _almost_ redundant with scenario_id.string, but too much
      # effort to prise them appart
      params[["scenario_id.string"]],
      params[["simulation_id.string"]],
      "lulc_maps"
    ) |>
    ensure_dir()

  params[["initial_lulc_path"]] <-
    fs::path(
      params[["sim_results_path"]],
      paste0(params[["scenario_start.real"]], ".tif")
    )

  config <- get_config()

  # the <v1> is a placeholder that the Dinamica CreateString functor replaces with the
  # integer timestep in format YYYY
  params[["params_folder_dinamica"]] <-
    ifelse(
      params[["is_simulation"]],
      file.path(
        config[["simulation_param_dir"]],
        params[["scenario_id.string"]],
        "allocation_param_table_<v1>.csv"
      ),
      file.path(
        config[["calibration_param_dir"]],
        params[["simulation_id.string"]],
        "allocation_param_table_<v1>.csv"
      )
    )

  params
}

# To be used for a lookuptable in Dinamica
#' @export
get_simulation_timesteps <- function(params = get_simulation_params()) {
  steps <- seq.int(
    from = params[["scenario_start.real"]],
    to = params[["scenario_end.real"]],
    by = params[["step_length.real"]]
  )

  list(
    key = steps[-length(steps)],
    value = steps[-1]
  )
}

#' Creates the initial LULC raster and copies it into the results directory
#' @export
create_init_lulc_raster <- function(params = get_simulation_params(), config = get_config()) {
  scenario_start <- params[["scenario_start.real"]]

  closest_observation <-
    fs::path(config[["historic_lulc_basepath"]]) |>
    fs::dir_ls(glob = "*.grd") |>
    tibble::as_tibble_col(column_name = "path") |>
    dplyr::mutate(
      year = stringr::str_extract(path, "([0-9]{4})") |> as.integer(),
      how_close = abs(scenario_start - year)
    ) |>
    dplyr::slice_min(order_by = how_close)

  initial_lulc_raster <-
    terra::rast(closest_observation[["path"]])

  # if simulation, then we need to overwrite arealstatistik glacier values with those
  # from the glacier model
  if (params[["is_simulation"]]) {
    # convert raster to dataframe
    lulc_tbl <-
      initial_lulc_raster |>
      terra::as.data.frame(na.rm = FALSE) |>
      tibble::as_tibble() |>
      tibble::rowid_to_column() |>
      rlang::set_names(c("id", "value"))

    # For the transition rates for glaciers to be accurate we need to make sure that the
    # initial LULC map has the correct glacier cells according to glacial modelling
    # using the scenario specific glacier index
    glacier_index <-
      fs::path(config[["glacial_change_path"]], "scenario_indices") |>
      fs::dir_ls(regex = params[["climate_scenario.string"]], ignore.case = TRUE) |>
      readRDS() |>
      tibble::as_tibble() |>
      dplyr::select(tidyselect::all_of(c("ID_loc", value = scenario_start)))

    # seperate vector of cell IDs for glacier and non-glacier cells
    non_glacier_ids <- glacier_index |>
      dplyr::filter(value == 0) |>
      purrr::pluck("ID_loc")
    glacier_ids <- glacier_index |>
      dplyr::filter(value == 1) |>
      purrr::pluck("ID_loc")

    # replace the 1's and 0's with the correct LULC
    lulc_tbl[lulc_tbl[["id"]] %in% non_glacier_ids, "value"] <- "static"
    lulc_tbl[lulc_tbl[["id"]] %in% glacier_ids, "value"] <- "glacier"

    # 2nd step ensure that other glacial cells that do not match the glacier index
    # are also changed to static so that the transition rates calculate the
    # correct number of cell changes
    lulc_tbl[
      which(lulc_tbl[["value"]] == "Glacier" &
        !(lulc_tbl[["id"]] %in% glacier_ids)),
      "value"
    ] <- "static"

    # convert back to raster
    terra::values(initial_lulc_raster) <- lulc_tbl[["value"]]
  } # close if statement for glacial modification

  # write to the results folder, from where dinamica will also read it
  terra::writeRaster(
    initial_lulc_raster,
    filename = params[["initial_lulc_path"]],
    overwrite = TRUE,
    datatype = "INT1U"
  )
}
