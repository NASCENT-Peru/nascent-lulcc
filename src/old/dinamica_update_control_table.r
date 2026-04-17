#' Update the control table after completion of simulation
#'
#' This function updates the "completed" status in a control table CSV file after a simulation run.
#'
#' @param ctrl_tbl_path Character. Path to the control table CSV file.
#' @param success Character. Indicates if the simulation was successful ("TRUE") or not.
#' @param simulation_num Integer. The simulation number to update in the control table.
#'
#' @details
#' The function reads the control table, updates the "completed.string" column for the
#' specified simulation number to "Y" if the simulation was successful, or "ERROR"
#' otherwise, and writes the updated table back to disk.
#'
#' @return None. The function is called for its side effect of updating the CSV file.
#'
#' @author Ben Black
#' @export

dinamica_update_control_table <- function(
    ctrl_tbl_path = default_ctrl_tbl_path(),
    success = character(),
    simulation_num = integer()) {
  # load control and subset to simulation number
  control_table <- get_control_table(ctrl_tbl_path = ctrl_tbl_path)

  # update value in completed column for current simulation
  if (grepl("TRUE", success, ignore.case = TRUE)) {
    control_table[control_table$simulation_num. == simulation_num, "completed.string"] <- "Y"
  } else {
    control_table[control_table$simulation_num. == simulation_num, "completed.string"] <- "ERROR"
  }

  # save table
  readr::write_csv(control_table, ctrl_tbl_path)
}
