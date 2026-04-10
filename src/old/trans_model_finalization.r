#' Trans_model_finalization
#'
#' Re-fit LULCC transition models using optimal specifcation after calling
#' [lulcc.finalisemodelspecifications()]
#'
#' @param config A list of configuration parameters
#' @author Ben Black
#' @export

trans_model_finalization <- function(config = get_config()) {
  # A- Preparation ####
  # Provide base folder paths for saving models for simulation
  model_base_folder <- config[["prediction_models_dir"]]
  ensure_dir(model_base_folder)

  # load table of model specifications
  models_specs <- readr::read_csv(config[["predict_model_specs_path"]])

  # Filter for models already completed
  models_specs <- models_specs[models_specs$modelling_completed == "N", ]

  # split into named list
  model_list <- lapply(
    split(models_specs, seq_len(nrow(models_specs))),
    as.list
  )
  names(model_list) <- models_specs$detail_model_tag

  # Instantiate wrapper function over process of modelling prep, fitting,
  # evaluation, saving and completeness checking
  lulcc.multispectransmodelling <- function(model_specs) {
    # A- Prepare model specifications ####

    # vector model specifcations
    Data_period <- model_specs[["data_period_name"]]
    Model_type <- model_specs[["model_type"]]
    model_scale <- model_specs[["model_scale"]]
    Feature_selection_employed <- model_specs[["feature_selection_employed"]]
    Correct_balance <- model_specs[["balance_adjustment"]]

    message(
      "Conducting modelling under specification ",
      model_specs[["detail_model_tag"]]
    )

    # finalise folder paths
    model_folder <- file.path(model_base_folder, Data_period)
    ensure_dir(model_folder)

    # Get file paths of transition datasets for period
    Data_paths_for_period <- if (Feature_selection_employed) {
      list.files(
        file.path(config[["trans_post_pred_filter_dir"]], Data_period),
        pattern = model_scale,
        full.names = TRUE
      )
    } else {
      list.files(
        file.path(config[["trans_post_pred_filter_dir"]], Data_period),
        pattern = model_scale,
        full.names = TRUE
      )
    }

    # B- Performing modelling ####

    if ((n_workers <- future:::nbrOfWorkers()) > 6L) {
      warning(
        "There are currently ",
        n_workers,
        " future workers enabled. ",
        "The following processing step is heavy on disk IO. ",
        "If the system interrupts your workers, try running this with fewer workers."
      )
    }
    # Loop over datasets, side effect of walk: write out model
    furrr::future_walk(
      .x = Data_paths_for_period,
      .options = furrr::furrr_options(seed = TRUE, scheduling = FALSE),
      .f = function(Dataset_path) {
        # load dataset
        Trans_dataset <- readRDS(Dataset_path)
        Trans_name <- stringr::str_remove_all(
          basename(Dataset_path),
          paste(c(".rds", paste0("_", model_scale)), collapse = "|")
        )

        # B.1 - Attach model parameters ####

        # Attach  a list of model parameters('model_settings')
        # for each type of model specified in the parameter grid
        model_settings <- lulcc.setparams(
          transition_result = Trans_dataset$trans_result,
          covariate_names = names(Trans_dataset$cov_data),
          model_name = Model_type,
          # Parameter tuning grid (all possible combinations will be evaluated)
          param_grid = config[["predict_param_grid_path"]],
          weights = 1
        )

        Trans_dataset[["model_settings"]] <- model_settings
        rm(model_settings)

        # B.2- Fit model ####

        Downsampling_bounds <- list(lower = 0.05, upper = 60)

        # Fit model
        mod <- try(
          lulcc.fitmodel(
            trans_result = Trans_dataset$trans_result, # transitions data
            cov_data = Trans_dataset$cov_data, # covariate data
            replicatetype = "none", # cross-validation strategy
            reps = 1, # Number of replicates
            mod_args = Trans_dataset$model_settings,
            path = NA,
            # utilise downsampling based on imbalance ratio
            Downsampling = (Correct_balance &&
              Trans_dataset$imbalance_ratio <= Downsampling_bounds$lower ||
              Trans_dataset$imbalance_ratio >= Downsampling_bounds$upper)
          ),
          silent = TRUE
        ) # Supply model arguments

        # extract only the part of the model that is needed (see below)
        model_extract <- mod@fits[["replicate_01"]][[1]]

        # save in final model location
        model_save_path <- paste0(
          model_folder,
          "/",
          Trans_name,
          ".",
          Data_period,
          ".",
          Model_type,
          ".rds"
        )
        saveRDS(model_extract, model_save_path)

        # return save path for model look up table
        return(model_save_path)
      }
    )

    # B.3- Mark specification as complete ####

    # load model spec table and replace the values in the 'completed' column
    model_spec_table <- readr::read_csv(config[["predict_model_specs_path"]])

    # find the correct row
    model_spec_table$modelling_completed[
      model_spec_table$detail_model_tag == model_specs[["detail_model_tag"]]
    ] <- "Y"

    readr::write_csv(
      model_spec_table,
      file = config[["predict_model_specs_path"]]
    )

    message("Prediction model fitting finished \n")
  } # close wrapper function

  # loop wrapper function over list of models
  purrr::walk(
    .x = model_list,
    .f = lulcc.multispectransmodelling
  )

  # F- create a model look up table for each period ####
  model_periods <- unique(models_specs$data_period_name)
  viable_trans_lists <-
    readRDS(config[["viable_transitions_lists"]]) |>
    purrr::keep_at(model_periods)
  model_lookups <-
    purrr::pmap(
      list(
        model_period = model_periods,
        model_base_folder = model_base_folder,
        trans_table = viable_trans_lists
      ),
      make_model_lookup_table
    ) |>
    purrr::set_names(model_periods)

  writexl::write_xlsx(
    model_lookups, # named list of DFs = named sheets
    path = config[["model_lookup_path"]]
  )
}

#' create a df with model lookup info for each period
make_model_lookup_table <- function(
  model_period,
  model_base_folder,
  trans_table,
  config = get_config()
) {
  trans_table <- dplyr::transmute(
    trans_table,
    Trans_name = tolower(Trans_name), # because of filename case (in-)sensitivity mess
    Trans_ID
  )

  fs::path(model_base_folder, model_period) |>
    fs::dir_ls() |>
    fs::path_rel(config[["data_basepath"]]) |>
    tibble::as_tibble_col("File_path") |>
    dplyr::mutate(
      Model_name = fs::path_file(File_path),
      Region = stringr::str_split_i(Model_name, stringr::fixed("."), i = 1),
      Initial_LULC = stringr::str_split_i(Model_name, stringr::fixed("."), i = 2),
      Final_LULC = stringr::str_split_i(Model_name, stringr::fixed("."), i = 3),
      Trans_name = paste0(Initial_LULC, "_", Final_LULC),
      Model_period = stringr::str_split_i(Model_name, stringr::fixed("."), i = 4),
      Model_type = stringr::str_split_i(Model_name, stringr::fixed("."), i = 5),
    ) |>
    # filter out persistence models not required by dinamica
    dplyr::filter(Initial_LULC != Final_LULC) |>
    dplyr::select(
      Trans_name,
      Region,
      Initial_LULC,
      Final_LULC,
      Model_type,
      Model_period,
      Model_name,
      File_path
    ) |>
    dplyr::left_join(trans_table, by = "Trans_name")
}
