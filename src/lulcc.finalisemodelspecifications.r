#' Finalize Model Specifications
#'
#' This function prepares configuration files for predicting land use land cover change.
#' It interactively gathers model parameters from the user and creates specification
#' files for prediction based on previously completed models.
#'
#' @author Ben Black
#'
#' @param model_type Character string specifying the model type
#' @param model_scale Character string specifying the model scale
#' @param feature_selection_employed Logical indicating whether feature selection was used
#' @param balance_adjustment Logical indicating whether balance adjustment was applied
#' @param config List containing configuration settings obtained from [get_config()] function
#'
#' @details
#' The function performs the following steps:
#' 1. Loads model specifications from the Excel file specified in config
#' 2. Filters model specifications based on user-provided criteria
#' 3. Verifies all required data periods have completed models available
#' 4. Creates and saves a prediction-specific model specification file
#' 5. Prompts the user for parameter values to create a prediction parameter grid
#' 6. Saves the prediction parameter grid for use in subsequent prediction steps
#'
#' @return No return value, called for side effects (creating configuration files)
#'
#' @export

lulcc.finalisemodelspecifications <- function(
    model_type = "rf",
    model_scale = "regionalized",
    feature_selection_employed = TRUE,
    balance_adjustment = TRUE,
    config = get_config()) {
  # Load model specifications
  model_specs <- readr::read_csv(config[["model_specs_path"]])

  # Subset model specifications by user inputs
  predict_model_specs <- model_specs[
    model_specs$modelling_completed == "Y" &
      model_specs$model_type == model_type &
      model_specs$model_scale == model_scale &
      model_specs$feature_selection_employed == feature_selection_employed &
      model_specs$balance_adjustment == balance_adjustment,
  ]

  # change completion values to 'N'
  predict_model_specs$modelling_completed <- "N"

  data_periods <- config[["data_periods"]]
  # Print a warning that not all data periods are included in the predict model specs table
  if (!all(data_periods %in% unique(predict_model_specs$data_period_name))) {
    stop(
      "Warning, the data period/s: ",
      paste(
        data_periods[which(!(data_periods %in% unique(predict_model_specs$data_period_name)))],
        collapse = ", "
      ),
      " do not have models of the desired specification completed for them.",
      " (i.e. Errors may have occured in model fitting or evaluation,",
      " please check before continuing)"
    )
  }

  # save prediction model specs
  readr::write_csv(predict_model_specs, file = config[["predict_model_specs_path"]])

  # Load parameter grid
  param_grid <- readxl::read_excel(config[["param_grid_path"]], sheet = model_type)

  # create a duplicate parameter grid for prediction
  predict_param_grid <- data.frame(matrix(nrow = nrow(param_grid), ncol = ncol(param_grid)))
  colnames(predict_param_grid) <- colnames(param_grid)

  # fill grid
  for (i in colnames(param_grid)) {
    predict_param_grid[[i]] <- readline(prompt = paste0("Enter value for parameter ", i, ": "))
  }

  # save prediction parameter grid
  openxlsx::write.xlsx(
    predict_param_grid,
    file = config[["predict_param_grid_path"]],
    sheetName = model_type
  )
}
