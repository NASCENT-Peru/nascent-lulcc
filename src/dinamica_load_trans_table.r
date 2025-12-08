#' dinamica_load_trans_table
#'
#' Loads a scenario and year-specific transition matrix for a simulation, and updates it
#' to reflect any deterministic transitions (such as glacier transitions).
#'
#' @param simulation_id Integer. The ID of the simulation to load parameters for.
#' @param year Integer. The year for which to load the transition matrix.
#' @param config List. Configuration settings, defaults to the result of [get_config()].
#'
#' @return A data frame containing the transition table ("From*" (int), "To*" (int),
#' "Rate" (float))
#'
#' @author Ben Black
#' @export

dinamica_load_trans_table <- function(
    simulation_id,
    year,
    config = get_config()) {
  params <- get_simulation_params(simulation_id = simulation_id)

  # use scenario ID to grab folder path of scenario specific transition tables
  scenario_trans_table_dir <- list.files(
    config[["trans_rate_table_dir"]],
    pattern = params[["scenario_id.string"]],
    full.names = TRUE
  )

  # Use folder path to create a generic scenario specific transition matrix file path
  scenario_trans_table_file <- file.path(
    scenario_trans_table_dir,
    paste0(
      params[["scenario_id.string"]], "_trans_table_", year, ".csv"
    )
  )

  # load the table
  trans_table <- readr::read_csv(scenario_trans_table_file, col_types = "iid")

  # remove glacier transition (implemented deterministically)
  if (
    params[["is_simulation"]] &&
      grepl("Y", params[["deterministic_trans.string"]], ignore.case = TRUE)
  ) {
    # TODO remove hard-coded assumption that glacier == 19
    # remove transitions with initial class == glacier
    trans_table <- trans_table[trans_table$`From*` != 19, ]
  }

  return(trans_table)
}
