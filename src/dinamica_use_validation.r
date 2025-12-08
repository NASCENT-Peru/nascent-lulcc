#' dinamica_use_validation
#'
#' Determine if validation should be used, and what paths to use for validation. If run
#' in simulation mode (i.e. control table `model_mode.string` matches "simulation") then
#' no validation will take place.
#'
#' @returns A list of `validation_condition, Validation_result_path, Validation_map_path,
#' Final_LULC_path, Sim_final_LULC_path`
#'
#' @param simulation_id integerish
#' @param sim_results_path character
#'
#' @export

dinamica_use_validation <- function(
    config = get_config(),
    simulation_id = integer(),
    sim_results_path) {
  params <- get_simulation_params(simulation_id = simulation_id)

  # If model mode is simulation then return '0' as validation is not required
  # else if calibration ,then validation is required so return '1'
  # and generate file paths for saving validation results

  # vector folder path for validation results
  Val_res_folder <- file.path(
    "results",
    params[["scenario_id.string"]],
    params[["simulation_id.string"]],
    "validation"
  )

  if (params[["is_simulation"]]) {
    validation_condition <- 0 # This is interpreted by Dinamica ifThen as FALSE
    Validation_map_path <- "NA"
    Validation_result_path <- "NA"
  } else {
    validation_condition <- 1

    # vector file paths for results
    ensure_dir(Val_res_folder)

    # adjust to file path
    Validation_map_path <- file.path(Val_res_folder, "validation_map.tif")
    Validation_result_path <- file.path(Val_res_folder, "similarity_value.csv")
  }


  ### =========================================================================
  ### B- Identify file path for relevant year Observed LULC
  ### =========================================================================

  # gather file path for observed LULC year that is closest to that of the final simulation year
  Final_LULC_path <-
    fs::path(config[["historic_lulc_basepath"]]) |>
    fs::dir_ls(glob = "*.grd") |>
    tibble::as_tibble_col(column_name = "path") |>
    dplyr::mutate(
      year = stringr::str_extract(path, "([0-9]{4})") |> as.integer(),
      how_close = abs(params[["scenario_end.real"]] - year)
    ) |>
    dplyr::slice_min(order_by = how_close) |>
    purrr::pluck("path")

  ### =========================================================================
  ### C- Identify file path for final simulation year Simulated LULC
  ### =========================================================================

  # alter file path for simulated LULC map for final simulation year
  Sim_final_LULC_path <-
    file.path(
      sim_results_path,
      paste0(
        params[["scenario_end.real"]],
        ".tif"
      )
    )

  return(list(
    validation_condition = validation_condition,
    Validation_result_path = Validation_result_path,
    Validation_map_path = Validation_map_path,
    Final_LULC_path = Final_LULC_path,
    Sim_final_LULC_path = Sim_final_LULC_path
  ))
}
