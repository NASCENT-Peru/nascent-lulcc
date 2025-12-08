#' Create a control table
#'
#' From master lulcc: how to prepare a sim control table
#' User enter start and end dates for the scenarios either enter a single number
#' value or a vector of values the same length as the number of scenarios earliest
#' possible model start time is 1985 and end time is 2060, simulations begin from
#' 2020 and we have initially agreed to use 5 year time steps
#'
#' @param scenario_names char
#' @param scenario_start 1985 or later
#' @param scenario_end 2060 per default
#' @param step_length in years
#' @param reps Number of runs for each simulation
#'
#' @export

prepare_simctrl_tbl <- function(
    scenario_names = c("BAU", "EI-NAT", "EI-CUL", "EI-SOC", "GR-EX"),
    scenario_start = 2020L,
    scenario_end = 2060L,
    step_length = 5L,
    reps = 1L) {
  control_table <- data.frame(matrix(ncol = 11, nrow = 0))
  colnames(control_table) <- c(
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
    # FIXME this is lacking some columns, compare with tools/simulation_control.csv
    # might be because they get added later?
    # "econ_scenario.string",
    # "climate_scenario.string",
    # "pop_scenario.string",
    # "ei_intervention_id.string",
  )

  # expand vector of scenario names according to number of repetitions and add to table
  scenario_ids <- c(sapply(scenario_names, function(x) rep(x, reps), simplify = TRUE))
  control_table[1:length(scenario_ids), "scenario_id.string"] <- scenario_ids

  # fill other columns
  control_table$simulation_id.string <- rep(
    paste0("v", seq(1, reps, 1)), length(scenario_names)
  )
  control_table$scenario_start.real <- if (length(unique(scenario_start)) == 1) {
    scenario_start
  } else {
    c(rep(scenario_start, length(scenario_names)))
  }
  control_table$scenario_end.real <- if (length(unique(scenario_end)) == 1) {
    scenario_end
  } else {
    c(rep(scenario_end, length(scenario_names)))
  }
  control_table$step_length.real <- step_length
  control_table$model_mode.string <- "Simulation"
  control_table$simulation_num. <- seq(1, nrow(control_table), 1)
  control_table$parallel_tpc.string <- "N"
  control_table$spatial_interventions.string <- "Y"
  control_table$deterministic_trans.string <- "Y"
  control_table$completed.string <- "N"

  return(control_table)
}
