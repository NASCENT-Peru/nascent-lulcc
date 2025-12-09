#############################################################################
## Dinamica_get_env_vars: Get environment variables required for simulation
## Date: 18-01-2024
## Author: Ben Black
#############################################################################
dinamica_get_env_vars <- function() {
  # testing values for env vars
  LULCC_M_WORK_DIR <- "e:/lulcc_ch"
  LULCC_M_CLASS_AGG <- "tools/lulc_class_aggregation.xlsx"
  LULCC_M_SPEC <- "tools/model_specs.xlsx"
  LULCC_M_PARAM_GRID <- "tools/param-grid.xlsx"
  LULCC_M_PRED_TABLE <- "tools/predictor_table.xlsx"
  LULCC_M_REF_GRID <- "data/ref_grid.gri"
  LULCC_M_CAL_PARAM_DIR <- "data/allocation_parameters/calibration"
  LULCC_M_SIM_PARAM_DIR <- "data/allocation_parameters/simulation"
  LULCC_M_RATE_TABLE_DIR <- "data/transition_rates/prepared_trans_rates"
  LULCC_M_SIM_CONTROL_TABLE <- "tools/simulation_control.csv"
  LULCC_M_SPAT_INTS_TABLE <- "tools/spatial_interventions.csv"
  LULCC_M_EI_INTS_TABLE <- "tools/ei_interventions.xlsx"

  # Created list of environment variables with names used in R scripts
  env_var_key <- list(
    "LULCC_M_WORK_DIR" = "wpath", # Working directory
    "LULCC_M_CLASS_AGG" = "LULC_aggregation_path", # Path to LULC class aggregation table
    "LULCC_M_SPEC" = "model_specs_path", # Path to model specifications table
    "LULCC_M_PARAM_GRID" = "param_grid_path", # Path to model hyper parameter grids
    "LULCC_M_PRED_TABLE" = "pred_table_path", # Path to predictor table
    "LULCC_M_REF_GRID" = "ref_grid_path", # Path to reference raster
    "LULCC_M_CAL_PARAM_DIR" = "calibration_param_dir", # Path to calibration parameter directory
    "LULCC_M_SIM_PARAM_DIR" = "simulation_param_dir", # Path to simulation parameter directory
    "LULCC_M_RATE_TABLE_DIR" = "trans_rate_table_dir", # Path to transition rate table directory
    "LULCC_M_SIM_CONTROL_TABLE" = "ctrl_tbl_path", # Path to control table
    "LULCC_M_SPAT_INTS_TABLE" = "spat_ints_path", # Path to spatial interventions table
    "LULCC_M_EI_INTS_TABLE" = "EI_ints_path"
  ) # Path to EI interventions table
  # list values of env_vars
  model_vars <- mget(names(env_var_key))

  # rename with model specific names
  names(model_vars) <- unlist(env_var_key)

  # Send re-named variables to global environment
  list2env(model_vars, .GlobalEnv)

  # Remove redundant variables from environment
  rm(list = names(env_var_key))
  rm(model_vars, env_var_key)
}
